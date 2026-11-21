//! Canonical severities: `info < warning < error < critical`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

impl Severity {
    /// Normalize the many spellings found in the wild (`WARN`, `ERR`, `crit`, `P1`...).
    pub fn normalize(s: &str) -> Option<Severity> {
        Some(match s.trim().to_ascii_lowercase().as_str() {
            "debug" | "trace" | "info" | "information" | "informational" | "notice" | "ok" | "p4"
            | "p5" | "low" => Severity::Info,
            "warn" | "warning" | "minor" | "p3" | "medium" => Severity::Warning,
            "error" | "err" | "major" | "high" | "p2" | "severe" => Severity::Error,
            "critical" | "crit" | "fatal" | "emergency" | "emerg" | "alert" | "p1" | "blocker" => {
                Severity::Critical
            }
            _ => return None,
        })
    }

    /// Numeric rank so rules can say `fields.severity_rank gte 2` (warning or above).
    pub fn rank(self) -> u8 {
        match self {
            Severity::Info => 1,
            Severity::Warning => 2,
            Severity::Error => 3,
            Severity::Critical => 4,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Error => "error",
            Severity::Critical => "critical",
        }
    }

    /// Default UI colour token for the severity.
    pub fn default_color(self) -> &'static str {
        match self {
            Severity::Info => "blue",
            Severity::Warning => "amber",
            Severity::Error => "orange",
            Severity::Critical => "red",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_common_spellings() {
        assert_eq!(Severity::normalize("WARN"), Some(Severity::Warning));
        assert_eq!(Severity::normalize("ERROR"), Some(Severity::Error));
        assert_eq!(Severity::normalize("critical"), Some(Severity::Critical));
        assert_eq!(Severity::normalize("bogus"), None);
        assert!(Severity::Warning < Severity::Critical);
    }
}
