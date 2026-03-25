use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LamportClock {
    node_id: String,
    time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LamportTimestamp {
    node_id: String,
    time: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum LamportTimestampError {
    #[error("missing ':' delimiter")]
    MissingDelimiter,
    #[error("invalid time value: {0}")]
    InvalidTime(String),
    #[error("empty node id")]
    EmptyNodeId,
}

// --- LamportClock ---

impl LamportClock {
    pub fn new(node_id: &str) -> Self {
        assert!(!node_id.is_empty(), "node_id must not be empty");
        assert!(!node_id.contains(':'), "node_id must not contain ':'");
        Self {
            node_id: node_id.to_string(),
            time: 0,
        }
    }

    pub fn tick(&mut self) -> u64 {
        self.time += 1;
        self.time
    }

    pub fn send(&mut self) -> LamportTimestamp {
        self.tick();
        LamportTimestamp {
            node_id: self.node_id.clone(),
            time: self.time,
        }
    }

    pub fn receive(&mut self, timestamp: &LamportTimestamp) -> u64 {
        self.time = self.time.max(timestamp.time) + 1;
        self.time
    }

    pub fn time(&self) -> u64 {
        self.time
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

// --- LamportTimestamp ---

impl LamportTimestamp {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn time(&self) -> u64 {
        self.time
    }
}

impl fmt::Display for LamportTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.node_id, self.time)
    }
}

impl FromStr for LamportTimestamp {
    type Err = LamportTimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let colon_pos = s.rfind(':').ok_or(LamportTimestampError::MissingDelimiter)?;
        let node_id = &s[..colon_pos];
        let time_str = &s[colon_pos + 1..];

        if node_id.is_empty() {
            return Err(LamportTimestampError::EmptyNodeId);
        }

        let time = time_str
            .parse::<u64>()
            .map_err(|_| LamportTimestampError::InvalidTime(time_str.to_string()))?;

        Ok(Self {
            node_id: node_id.to_string(),
            time,
        })
    }
}

impl PartialOrd for LamportTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LamportTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time
            .cmp(&other.time)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_clock_with_zero_time() {
        let clock = LamportClock::new("node-1");
        assert_eq!(clock.time(), 0);
    }

    #[test]
    fn new_preserves_node_id() {
        let clock = LamportClock::new("order-service");
        assert_eq!(clock.node_id(), "order-service");
    }

    #[test]
    #[should_panic]
    fn new_rejects_empty_node_id() {
        LamportClock::new("");
    }

    #[test]
    #[should_panic]
    fn new_rejects_node_id_with_colon() {
        LamportClock::new("bad:id");
    }

    #[test]
    fn timestamp_accessors_return_correct_values() {
        let mut clock = LamportClock::new("svc");
        let ts = clock.send();
        assert_eq!(ts.node_id(), "svc");
        assert_eq!(ts.time(), 1);
    }

    #[test]
    fn tick_returns_one_on_first_call() {
        let mut clock = LamportClock::new("a");
        assert_eq!(clock.tick(), 1);
    }

    #[test]
    fn tick_increments_monotonically() {
        let mut clock = LamportClock::new("a");
        assert_eq!(clock.tick(), 1);
        assert_eq!(clock.tick(), 2);
        assert_eq!(clock.tick(), 3);
    }

    #[test]
    fn send_increments_and_returns_timestamp() {
        let mut clock = LamportClock::new("node");
        let ts = clock.send();
        assert_eq!(ts.time(), 1);
        assert_eq!(clock.time(), 1);
    }

    #[test]
    fn send_timestamp_contains_correct_node_id() {
        let mut clock = LamportClock::new("order-service");
        let ts = clock.send();
        assert_eq!(ts.node_id(), "order-service");
    }

    #[test]
    fn receive_advances_past_remote_timestamp() {
        let mut clock_a = LamportClock::new("a");
        let mut clock_b = LamportClock::new("b");

        for _ in 0..5 {
            clock_a.tick();
        }
        let ts = clock_a.send(); // time=6

        let new_time = clock_b.receive(&ts);
        assert_eq!(new_time, 7); // max(0,6)+1
        assert_eq!(clock_b.time(), 7);
    }

    #[test]
    fn receive_with_lower_remote_still_increments() {
        let mut clock = LamportClock::new("a");
        for _ in 0..10 {
            clock.tick();
        }

        let remote_ts = LamportTimestamp {
            node_id: "b".to_string(),
            time: 3,
        };

        let new_time = clock.receive(&remote_ts);
        assert_eq!(new_time, 11); // max(10,3)+1
    }

    #[test]
    fn receive_with_equal_remote_increments() {
        let mut clock = LamportClock::new("a");
        for _ in 0..5 {
            clock.tick();
        }

        let remote_ts = LamportTimestamp {
            node_id: "b".to_string(),
            time: 5,
        };

        let new_time = clock.receive(&remote_ts);
        assert_eq!(new_time, 6); // max(5,5)+1
    }

    #[test]
    fn timestamp_display_format() {
        let ts = LamportTimestamp {
            node_id: "order-service".to_string(),
            time: 42,
        };
        assert_eq!(ts.to_string(), "order-service:42");
    }

    #[test]
    fn timestamp_parse_roundtrip() {
        let ts = LamportTimestamp {
            node_id: "order-service".to_string(),
            time: 42,
        };
        let s = ts.to_string();
        let parsed: LamportTimestamp = s.parse().unwrap();
        assert_eq!(parsed, ts);
    }

    #[test]
    fn timestamp_parse_missing_delimiter() {
        let result = "no-delimiter".parse::<LamportTimestamp>();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LamportTimestampError::MissingDelimiter
        ));
    }

    #[test]
    fn timestamp_parse_invalid_time() {
        let result = "node:abc".parse::<LamportTimestamp>();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LamportTimestampError::InvalidTime(_)
        ));
    }

    #[test]
    fn timestamp_parse_empty_node_id() {
        let result = ":42".parse::<LamportTimestamp>();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LamportTimestampError::EmptyNodeId
        ));
    }

    #[test]
    fn timestamp_ordering_by_time() {
        let ts1 = LamportTimestamp {
            node_id: "a".to_string(),
            time: 1,
        };
        let ts2 = LamportTimestamp {
            node_id: "a".to_string(),
            time: 2,
        };
        assert!(ts1 < ts2);
    }

    #[test]
    fn timestamp_ordering_tiebreak_by_node_id() {
        let ts_a = LamportTimestamp {
            node_id: "a".to_string(),
            time: 5,
        };
        let ts_b = LamportTimestamp {
            node_id: "b".to_string(),
            time: 5,
        };
        assert!(ts_a < ts_b);
    }

    #[test]
    fn serde_json_roundtrip_clock() {
        let clock = LamportClock::new("serde-node");
        let json = serde_json::to_string(&clock).unwrap();
        let deserialized: LamportClock = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.node_id(), "serde-node");
        assert_eq!(deserialized.time(), 0);
    }

    #[test]
    fn serde_json_roundtrip_timestamp() {
        let ts = LamportTimestamp {
            node_id: "ts-node".to_string(),
            time: 99,
        };
        let json = serde_json::to_string(&ts).unwrap();
        let deserialized: LamportTimestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, ts);
    }
}
