use chrono::{DateTime, Duration, Utc};

#[derive(Debug, thiserror::Error)]
#[error("invalid duration `{0}`: expected a number with unit s, m, h, or d (e.g. 15m)")]
pub struct SinceParseError(String);

/// Parse `30s`, `15m`, `2h`, `7d`.
pub fn parse_duration(s: &str) -> Result<Duration, SinceParseError> {
    let s = s.trim();
    let err = || SinceParseError(s.to_string());
    let split = s.find(|c: char| !c.is_ascii_digit()).ok_or_else(err)?;
    let (num, unit) = s.split_at(split);
    let n: i64 = num.parse().map_err(|_| err())?;
    match unit {
        "s" => Ok(Duration::seconds(n)),
        "m" => Ok(Duration::minutes(n)),
        "h" => Ok(Duration::hours(n)),
        "d" => Ok(Duration::days(n)),
        _ => Err(err()),
    }
}

/// A position in the change journal, as accepted by `changes_since`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Since {
    Seq(i64),
    Checkpoint(String),
    Time(DateTime<Utc>),
}

impl Since {
    /// Accepts `seq:<n>` or a bare integer, an RFC 3339 timestamp, a relative
    /// duration like `15m`, or otherwise a checkpoint name.
    pub fn parse(s: &str, now: DateTime<Utc>) -> Self {
        let s = s.trim();
        if let Some(n) = s.strip_prefix("seq:").and_then(|n| n.parse().ok()) {
            return Self::Seq(n);
        }
        if let Ok(n) = s.parse::<i64>() {
            return Self::Seq(n);
        }
        if let Ok(t) = DateTime::parse_from_rfc3339(s) {
            return Self::Time(t.with_timezone(&Utc));
        }
        if let Ok(d) = parse_duration(s) {
            return Self::Time(now - d);
        }
        Self::Checkpoint(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("15m").unwrap(), Duration::minutes(15));
        assert_eq!(parse_duration("2d").unwrap(), Duration::days(2));
        assert!(parse_duration("15").is_err());
        assert!(parse_duration("m").is_err());
        assert!(parse_duration("5w").is_err());
    }

    #[test]
    fn since_forms() {
        let now = Utc::now();
        assert_eq!(Since::parse("seq:42", now), Since::Seq(42));
        assert_eq!(Since::parse("42", now), Since::Seq(42));
        assert_eq!(
            Since::parse("1h", now),
            Since::Time(now - Duration::hours(1))
        );
        assert!(matches!(
            Since::parse("2026-10-01T00:00:00Z", now),
            Since::Time(_)
        ));
        assert_eq!(
            Since::parse("before-deploy", now),
            Since::Checkpoint("before-deploy".into())
        );
    }
}
