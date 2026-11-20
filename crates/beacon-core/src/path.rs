//! Field paths into an event.
//!
//! Two syntaxes:
//! - dot syntax for common cases: `raw.error.code`, `fields.severity`, `raw.items.0.id`
//! - JSON Pointer for arbitrary keys: `raw/labels/team.name`, `raw/a~1b` (RFC 6901 escapes)
//!
//! The first segment is the root: `raw`, `fields`, `schema`, `source` or `id`.
//! Mappings resolve paths relative to `raw` (see [`Path::parse_with_default_root`]).

use std::borrow::Cow;
use std::fmt;

use serde_json::Value;

use crate::event::Event;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Root {
    Raw,
    Fields,
    Schema,
    Source,
    Id,
}

impl Root {
    fn parse(s: &str) -> Option<Root> {
        Some(match s {
            "raw" => Root::Raw,
            "fields" => Root::Fields,
            "schema" => Root::Schema,
            "source" => Root::Source,
            "id" => Root::Id,
            _ => return None,
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Root::Raw => "raw",
            Root::Fields => "fields",
            Root::Schema => "schema",
            Root::Source => "source",
            Root::Id => "id",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Path {
    pub root: Root,
    pub segments: Vec<String>,
    text: String,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum PathError {
    #[error("empty path")]
    Empty,
    #[error("unknown path root `{0}` (expected raw, fields, schema, source or id)")]
    UnknownRoot(String),
    #[error("path `{0}` has an empty segment")]
    EmptySegment(String),
    #[error("`{0}` has no sub-fields")]
    ScalarRoot(String),
}

impl Path {
    /// Parse a fully-qualified path (`raw.a.b`, `fields.type`, `raw/a/b`).
    pub fn parse(s: &str) -> Result<Path, PathError> {
        Self::parse_inner(s, None)
    }

    /// Parse a path where a missing or unknown root means `default` (used by
    /// schema mappings, whose paths are relative to the raw payload).
    pub fn parse_with_default_root(s: &str, default: Root) -> Result<Path, PathError> {
        Self::parse_inner(s, Some(default))
    }

    fn parse_inner(s: &str, default: Option<Root>) -> Result<Path, PathError> {
        let s = s.trim();
        if s.is_empty() {
            return Err(PathError::Empty);
        }
        // Split off the root token: up to the first '.' or '/'.
        let split_at = s.find(['.', '/']);
        let (head, rest) = match split_at {
            Some(i) => (&s[..i], Some((&s[i..i + 1], &s[i + 1..]))),
            None => (s, None),
        };
        let (root, segments) = match Root::parse(head) {
            Some(root) => {
                let segs = match rest {
                    None => vec![],
                    Some((sep, tail)) => split_segments(sep, tail, s)?,
                };
                (root, segs)
            }
            None => match default {
                Some(root) => {
                    let sep = if s.starts_with('/') { "/" } else { "." };
                    let body = s.strip_prefix('/').unwrap_or(s);
                    (root, split_segments(sep, body, s)?)
                }
                None => return Err(PathError::UnknownRoot(head.to_string())),
            },
        };
        if matches!(root, Root::Schema | Root::Source | Root::Id) && !segments.is_empty() {
            return Err(PathError::ScalarRoot(root.as_str().to_string()));
        }
        let text = render(root, &segments);
        Ok(Path {
            root,
            segments,
            text,
        })
    }

    /// Resolve the path against an event. `None` means *missing*, which is
    /// different from a present JSON `null`.
    pub fn resolve<'a>(&self, event: &'a Event) -> Option<Cow<'a, Value>> {
        match self.root {
            Root::Raw => walk(&event.raw, &self.segments).map(Cow::Borrowed),
            Root::Fields => {
                let (first, rest) = self.segments.split_first()?;
                let v = event.fields.get(first)?;
                walk(v, rest).map(Cow::Borrowed)
            }
            Root::Schema => Some(Cow::Owned(Value::String(event.schema.clone()))),
            Root::Source => Some(Cow::Owned(Value::String(event.source.clone()))),
            Root::Id => Some(Cow::Owned(Value::String(event.id.clone()))),
        }
    }

    /// Resolve against a bare JSON value (the raw payload), ignoring the root.
    pub fn resolve_in<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        walk(value, &self.segments)
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The last segment, used as a default human label.
    pub fn label(&self) -> &str {
        self.segments
            .last()
            .map(String::as_str)
            .unwrap_or(self.root.as_str())
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

fn split_segments(sep: &str, tail: &str, whole: &str) -> Result<Vec<String>, PathError> {
    if tail.is_empty() {
        return Err(PathError::EmptySegment(whole.to_string()));
    }
    if sep == "/" {
        // JSON Pointer: empty segments are legal keys, `~1` = '/', `~0` = '~'.
        Ok(tail
            .split('/')
            .map(|s| s.replace("~1", "/").replace("~0", "~"))
            .collect())
    } else {
        let segs: Vec<String> = tail.split('.').map(str::to_string).collect();
        if segs.iter().any(String::is_empty) {
            return Err(PathError::EmptySegment(whole.to_string()));
        }
        Ok(segs)
    }
}

fn render(root: Root, segments: &[String]) -> String {
    let mut out = root.as_str().to_string();
    let needs_pointer = segments.iter().any(|s| s.is_empty() || s.contains(['.', '/', '~']));
    for s in segments {
        if needs_pointer {
            out.push('/');
            out.push_str(&s.replace('~', "~0").replace('/', "~1"));
        } else {
            out.push('.');
            out.push_str(s);
        }
    }
    out
}

fn walk<'a>(mut v: &'a Value, segments: &[String]) -> Option<&'a Value> {
    for seg in segments {
        v = match v {
            Value::Object(map) => map.get(seg)?,
            Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(raw: Value) -> Event {
        Event::new("e1".into(), "http:test".into(), raw)
    }

    #[test]
    fn dot_and_pointer_paths() {
        let e = ev(json!({"error": {"code": "TIMEOUT"}, "a/b": 1, "items": [{"id": 7}], "x.y": 2}));
        assert_eq!(
            Path::parse("raw.error.code").unwrap().resolve(&e).unwrap().as_ref(),
            &json!("TIMEOUT")
        );
        assert_eq!(Path::parse("raw/a~1b").unwrap().resolve(&e).unwrap().as_ref(), &json!(1));
        assert_eq!(Path::parse("raw.items.0.id").unwrap().resolve(&e).unwrap().as_ref(), &json!(7));
        assert_eq!(Path::parse("raw/x.y").unwrap().resolve(&e).unwrap().as_ref(), &json!(2));
        assert!(Path::parse("raw.missing").unwrap().resolve(&e).is_none());
    }

    #[test]
    fn null_is_present_not_missing() {
        let e = ev(json!({"n": null}));
        assert_eq!(Path::parse("raw.n").unwrap().resolve(&e).unwrap().as_ref(), &Value::Null);
    }

    #[test]
    fn default_root_for_mappings() {
        let p = Path::parse_with_default_root("labels.severity", Root::Raw).unwrap();
        assert_eq!(p.as_str(), "raw.labels.severity");
        let p = Path::parse_with_default_root("raw.x", Root::Raw).unwrap();
        assert_eq!(p.as_str(), "raw.x");
    }

    #[test]
    fn errors() {
        assert_eq!(Path::parse(""), Err(PathError::Empty));
        assert!(matches!(Path::parse("foo.bar"), Err(PathError::UnknownRoot(_))));
        assert!(matches!(Path::parse("raw..a"), Err(PathError::EmptySegment(_))));
        assert!(matches!(Path::parse("schema.x"), Err(PathError::ScalarRoot(_))));
    }

    #[test]
    fn scalar_roots() {
        let mut e = ev(json!({}));
        e.schema = "workflow.failed".into();
        assert_eq!(Path::parse("schema").unwrap().resolve(&e).unwrap().as_ref(), &json!("workflow.failed"));
        assert_eq!(Path::parse("source").unwrap().resolve(&e).unwrap().as_ref(), &json!("http:test"));
    }
}
