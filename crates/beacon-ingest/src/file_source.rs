//! A tailed JSON-lines file as a source (`type: file`, spec §5.1).
//!
//! ```yaml
//! id: file:applog
//! type: file
//! path: /var/log/app/events.jsonl
//! start: latest                     # first start: earliest = whole file, latest = only new lines
//! checkpoint: /var/lib/beacon/applog.pos   # optional: resume here after a restart
//! ```
//!
//! One JSON object per line. Like `tail -F`:
//! - only complete lines (ending in `\n`) are read; a half-written last line waits;
//! - a file that doesn't exist yet is waited for, and read from its start when it appears;
//! - truncation (size below the read position) or rotation (a new inode) restarts at 0.
//!
//! Each event's id is `<source>:<inode>:<byte offset of the line>`, so a replay
//! after a crash republishes the same ids and the matcher drops them. The
//! position (and the checkpoint, if set) moves only after every line before it
//! is durably in `events.raw`. Lines that aren't JSON objects, or longer than
//! [`MAX_LINE`], go to `events.quarantine`.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::kafka_source::{SourceSpec, publish_or_quarantine};
use crate::publisher::EventPublisher;

/// Longest accepted line; a longer one is quarantined (its first 64 KiB) and skipped.
pub const MAX_LINE: usize = 1024 * 1024;
/// Bytes read per pass, so a large backlog is published in bounded steps.
const CHUNK: usize = 4 * 1024 * 1024;
const POLL: Duration = Duration::from_millis(250);

pub fn validate(s: &SourceSpec) -> Result<()> {
    match s.path.as_deref() {
        Some(p) if !p.trim().is_empty() => {}
        _ => bail!("source `{}`: type file needs `path`", s.id),
    }
    if !matches!(s.start.as_str(), "earliest" | "latest") {
        bail!("source `{}`: `start` must be earliest or latest", s.id);
    }
    if !s.topics.is_empty() || s.bootstrap.is_some() || s.group.is_some() || s.security.is_some() || s.url.is_some() || s.interval.is_some() || s.items.is_some() || s.id_path.is_some() || s.token_env.is_some() {
        bail!("source `{}`: type file takes only path, start and checkpoint", s.id);
    }
    if s.checkpoint.as_deref().is_some_and(|c| c.trim().is_empty()) {
        bail!("source `{}`: `checkpoint` must be a file path", s.id);
    }
    Ok(())
}

/// Where reading resumes: the file's identity and the byte offset after the
/// last published line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub inode: u64,
    pub offset: u64,
}

#[cfg(unix)]
fn inode(meta: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(meta)
}

#[cfg(not(unix))]
fn inode(_meta: &std::fs::Metadata) -> u64 {
    0
}

fn read_checkpoint(path: &Path) -> Option<Position> {
    let text = std::fs::read_to_string(path).ok()?;
    let (i, o) = text.trim().split_once(' ')?;
    Some(Position { inode: i.parse().ok()?, offset: o.parse().ok()? })
}

/// Atomic: write a temp file, then rename over the checkpoint.
fn write_checkpoint(path: &Path, pos: Position) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, format!("{} {}\n", pos.inode, pos.offset))?;
    std::fs::rename(tmp, path)
}

/// Complete lines in `buf` (read from `base`): each line's start offset and
/// bytes, plus how far they reach. A trailing partial line is left for later,
/// unless `buf` is a full chunk with no newline at all (an overlong line).
pub fn split_lines(buf: &[u8], base: u64, full_chunk: bool) -> (Vec<(u64, &[u8])>, u64) {
    let mut out = Vec::new();
    let mut start = 0usize;
    while let Some(i) = buf[start..].iter().position(|&b| b == b'\n') {
        out.push((base + start as u64, &buf[start..start + i]));
        start += i + 1;
    }
    if start == 0 && full_chunk {
        // No newline in a whole chunk: hand it over as one overlong "line".
        out.push((base, buf));
        return (out, base + buf.len() as u64);
    }
    (out, base + start as u64)
}

/// Parse one line: a JSON object, or the reason it is quarantined. Blank lines are `None`.
pub fn parse_line(line: &[u8]) -> Option<Result<serde_json::Value, String>> {
    let trimmed = line.strip_suffix(b"\r").unwrap_or(line);
    if trimmed.iter().all(u8::is_ascii_whitespace) {
        return None;
    }
    if trimmed.len() > MAX_LINE {
        return Some(Err(format!("line longer than {MAX_LINE} bytes")));
    }
    Some(match serde_json::from_slice::<serde_json::Value>(trimmed) {
        Ok(v) if v.is_object() => Ok(v),
        Ok(_) => Err("line is JSON but not an object".into()),
        Err(e) => Err(format!("not JSON: {e}")),
    })
}

pub async fn run_file_source(spec: SourceSpec, publisher: EventPublisher, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    let path = PathBuf::from(spec.path.clone().unwrap_or_default());
    let checkpoint = spec.checkpoint.clone().map(PathBuf::from);
    let mut pos: Option<Position> = checkpoint.as_deref().and_then(read_checkpoint);
    // `start` applies only to the file as it is at our first look.
    let mut first_look = pos.is_none();
    info!(source = %spec.id, path = %path.display(), ?pos, "file source started");
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        match std::fs::metadata(&path) {
            Ok(meta) if meta.is_file() => {
                let ino = inode(&meta);
                let len = meta.len();
                let at = match pos {
                    Some(p) if p.inode == ino && p.offset <= len => p.offset,
                    // Rotated (new inode) or truncated: start over.
                    Some(_) => 0,
                    None if first_look && spec.start == "latest" => len,
                    None => 0,
                };
                first_look = false;
                if pos != Some(Position { inode: ino, offset: at }) {
                    pos = Some(Position { inode: ino, offset: at });
                }
                if len > at {
                    match read_from(&path, at, len).await {
                        Ok(buf) => {
                            let full = buf.len() == CHUNK;
                            let (lines, next) = split_lines(&buf, at, full);
                            for (offset, line) in lines {
                                let Some(parsed) = parse_line(line) else { continue };
                                let id = format!("{}:{ino}:{offset}", spec.id);
                                let payload = &line[..line.len().min(64 * 1024)];
                                if !publish_or_quarantine(&publisher, &spec.id, id, parsed, payload, &shutdown).await {
                                    return Ok(());
                                }
                            }
                            let p = Position { inode: ino, offset: next };
                            if Some(p) != pos {
                                pos = Some(p);
                                if let Some(c) = &checkpoint
                                    && let Err(e) = write_checkpoint(c, p)
                                {
                                    warn!(source = %spec.id, error = %e, "could not write the checkpoint");
                                }
                            }
                            if next > at && len > next {
                                continue; // more backlog: read again without waiting
                            }
                        }
                        Err(e) => warn!(source = %spec.id, error = %e, "reading the file failed"),
                    }
                }
            }
            Ok(_) => warn!(source = %spec.id, path = %path.display(), "path is not a regular file"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Not there (yet, or between a rotation's rename and create): when a
                // file appears it is new, so read it from its start.
                if pos.is_some() {
                    pos = None;
                }
                first_look = false;
            }
            Err(e) => warn!(source = %spec.id, error = %e, "cannot stat the file"),
        }
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = tokio::time::sleep(POLL) => {}
        }
    }
}

async fn read_from(path: &Path, at: u64, len: u64) -> std::io::Result<Vec<u8>> {
    let mut f = tokio::fs::File::open(path).await?;
    f.seek(SeekFrom::Start(at)).await?;
    let want = ((len - at) as usize).min(CHUNK);
    let mut buf = vec![0u8; want];
    let mut got = 0;
    while got < want {
        let n = f.read(&mut buf[got..]).await?;
        if n == 0 {
            break;
        }
        got += n;
    }
    buf.truncate(got);
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(yaml: &str) -> SourceSpec {
        serde_saphyr::from_str(yaml).unwrap()
    }

    #[test]
    fn splits_complete_lines_and_keeps_the_partial_one() {
        let (lines, next) = split_lines(b"{\"a\":1}\n{\"b\":2}\n{\"c\"", 100, false);
        assert_eq!(lines, vec![(100, &b"{\"a\":1}"[..]), (108, &b"{\"b\":2}"[..])]);
        assert_eq!(next, 116, "the partial third line waits");
        let (lines, next) = split_lines(b"no newline yet", 0, false);
        assert!(lines.is_empty());
        assert_eq!(next, 0);
        // A whole chunk without a newline is handed over so the reader can't stall.
        let (lines, next) = split_lines(b"xxxx", 0, true);
        assert_eq!((lines.len(), next), (1, 4));
    }

    #[test]
    fn parses_objects_and_quarantines_the_rest() {
        assert!(parse_line(b"").is_none());
        assert!(parse_line(b"   \r").is_none());
        assert_eq!(parse_line(b"{\"level\":\"WARN\"}\r").unwrap().unwrap()["level"], "WARN");
        assert!(parse_line(b"[1,2]").unwrap().unwrap_err().contains("not an object"));
        assert!(parse_line(b"oops").unwrap().unwrap_err().contains("not JSON"));
    }

    #[test]
    fn checkpoint_roundtrips() {
        let dir = std::env::temp_dir().join(format!("beacon-fs-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let c = dir.join("pos");
        assert!(read_checkpoint(&c).is_none());
        write_checkpoint(&c, Position { inode: 7, offset: 42 }).unwrap();
        assert_eq!(read_checkpoint(&c), Some(Position { inode: 7, offset: 42 }));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn validation() {
        validate(&spec("id: file:a\ntype: file\npath: /tmp/a.jsonl\n")).unwrap();
        assert!(validate(&spec("id: file:a\ntype: file\n")).is_err(), "path required");
        assert!(validate(&spec("id: file:a\ntype: file\npath: /x\ntopics: [t]\n")).is_err(), "no kafka keys");
        assert!(validate(&spec("id: file:a\ntype: file\npath: /x\nstart: middle\n")).is_err());
    }
}
