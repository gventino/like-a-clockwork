use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::causality::{compare, CausalityRelation};

#[derive(Debug, thiserror::Error)]
pub enum VectorTimestampError {
    #[error("empty vector timestamp string")]
    Empty,
    #[error("missing '=' delimiter in segment")]
    MissingDelimiter,
    #[error("invalid time value: {0}")]
    InvalidTime(String),
    #[error("empty node id")]
    EmptyNodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorTimestamp {
    clocks: HashMap<String, u64>,
}

impl VectorTimestamp {
    pub fn clocks(&self) -> &HashMap<String, u64> {
        &self.clocks
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }
}

impl From<HashMap<String, u64>> for VectorTimestamp {
    fn from(clocks: HashMap<String, u64>) -> Self {
        VectorTimestamp { clocks }
    }
}

impl fmt::Display for VectorTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut keys: Vec<&String> = self.clocks.keys().collect();
        keys.sort();
        let parts: Vec<String> = keys.iter().map(|k| format!("{}={}", k, self.clocks[*k])).collect();
        write!(f, "{}", parts.join(","))
    }
}

impl FromStr for VectorTimestamp {
    type Err = VectorTimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(VectorTimestampError::Empty);
        }

        let mut clocks = HashMap::new();
        for segment in s.split(',') {
            let Some((node_id, value_str)) = segment.split_once('=') else {
                return Err(VectorTimestampError::MissingDelimiter);
            };
            if node_id.is_empty() {
                return Err(VectorTimestampError::EmptyNodeId);
            }
            let value: u64 = value_str
                .parse()
                .map_err(|_| VectorTimestampError::InvalidTime(value_str.to_string()))?;
            clocks.insert(node_id.to_string(), value);
        }

        Ok(VectorTimestamp { clocks })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorClock {
    node_id: String,
    clocks: HashMap<String, u64>,
}

impl VectorClock {
    pub fn new(node_id: &str, peers: &[&str]) -> Self {
        assert!(!node_id.is_empty(), "node_id must not be empty");
        assert!(
            !node_id.contains('=') && !node_id.contains(','),
            "node_id must not contain '=' or ','"
        );

        for peer in peers {
            assert!(!peer.is_empty(), "peer id must not be empty");
            assert!(
                !peer.contains('=') && !peer.contains(','),
                "peer id must not contain '=' or ','"
            );
            assert!(*peer != node_id, "node_id must not appear in peers");
        }

        let mut clocks = HashMap::new();
        clocks.insert(node_id.to_string(), 0);
        for peer in peers {
            clocks.insert(peer.to_string(), 0);
        }

        VectorClock {
            node_id: node_id.to_string(),
            clocks,
        }
    }

    pub fn from_map(node_id: &str, clocks: HashMap<String, u64>) -> Self {
        VectorClock {
            node_id: node_id.to_string(),
            clocks,
        }
    }

    pub fn tick(&mut self) -> u64 {
        let counter = self.clocks.entry(self.node_id.clone()).or_insert(0);
        *counter += 1;
        *counter
    }

    pub fn send(&mut self) -> VectorTimestamp {
        self.tick();
        self.snapshot()
    }

    pub fn receive(&mut self, timestamp: &VectorTimestamp) {
        for (node, &value) in timestamp.clocks() {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(value);
        }
        let local = self.clocks.entry(self.node_id.clone()).or_insert(0);
        *local += 1;
    }

    pub fn relation(&self, other: &VectorClock) -> CausalityRelation {
        let self_ts = self.snapshot();
        let other_ts = other.snapshot();
        compare(&self_ts, &other_ts)
    }

    pub fn snapshot(&self) -> VectorTimestamp {
        VectorTimestamp {
            clocks: self.clocks.clone(),
        }
    }

    pub fn get(&self, node_id: &str) -> u64 {
        self.clocks.get(node_id).copied().unwrap_or(0)
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (node, &value) in &other.clocks {
            let entry = self.clocks.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_all_clocks_to_zero() {
        let vc = VectorClock::new("a", &["b", "c"]);
        assert_eq!(vc.get("a"), 0);
        assert_eq!(vc.get("b"), 0);
        assert_eq!(vc.get("c"), 0);
    }

    #[test]
    fn new_includes_self_in_clocks() {
        let vc = VectorClock::new("node1", &[]);
        assert_eq!(vc.get("node1"), 0);
        assert!(vc.clocks.contains_key("node1"));
    }

    #[test]
    #[should_panic]
    fn new_rejects_empty_node_id() {
        VectorClock::new("", &["b"]);
    }

    #[test]
    #[should_panic]
    fn new_rejects_duplicate_peer() {
        VectorClock::new("a", &["a"]);
    }

    #[test]
    #[should_panic]
    fn new_rejects_invalid_chars_in_node_id() {
        VectorClock::new("a=b", &[]);
    }

    #[test]
    fn from_map_preserves_values() {
        let mut map = HashMap::new();
        map.insert("x".to_string(), 5);
        map.insert("y".to_string(), 3);
        let vc = VectorClock::from_map("x", map);
        assert_eq!(vc.get("x"), 5);
        assert_eq!(vc.get("y"), 3);
    }

    #[test]
    fn tick_increments_only_local_node() {
        let mut vc = VectorClock::new("a", &["b"]);
        let val = vc.tick();
        assert_eq!(val, 1);
        assert_eq!(vc.get("a"), 1);
    }

    #[test]
    fn tick_does_not_affect_other_nodes() {
        let mut vc = VectorClock::new("a", &["b", "c"]);
        vc.tick();
        vc.tick();
        assert_eq!(vc.get("a"), 2);
        assert_eq!(vc.get("b"), 0);
        assert_eq!(vc.get("c"), 0);
    }

    #[test]
    fn send_increments_and_returns_snapshot() {
        let mut vc = VectorClock::new("a", &["b"]);
        let ts = vc.send();
        assert_eq!(ts.get("a"), 1);
        assert_eq!(ts.get("b"), 0);
        assert_eq!(vc.get("a"), 1);
    }

    #[test]
    fn send_snapshot_is_independent_copy() {
        let mut vc = VectorClock::new("a", &["b"]);
        let ts = vc.send();
        vc.tick();
        assert_eq!(ts.get("a"), 1);
        assert_eq!(vc.get("a"), 2);
    }

    #[test]
    fn receive_takes_max_then_increments_local() {
        let mut vc_a = VectorClock::new("a", &["b"]);
        let mut vc_b = VectorClock::new("b", &["a"]);
        let ts = vc_a.send();
        vc_b.receive(&ts);
        assert_eq!(vc_b.get("a"), 1);
        assert_eq!(vc_b.get("b"), 1);
    }

    #[test]
    fn receive_takes_max_for_all_nodes() {
        let mut vc_a = VectorClock::new("a", &["b", "c"]);
        vc_a.tick(); // a=1
        vc_a.tick(); // a=2

        let mut vc_b = VectorClock::new("b", &["a", "c"]);
        vc_b.tick(); // b=1

        let ts_a = vc_a.send(); // a=3
        vc_b.receive(&ts_a);
        // b should have max(a)=3, b=1+1=2, c=0
        assert_eq!(vc_b.get("a"), 3);
        assert_eq!(vc_b.get("b"), 2);
        assert_eq!(vc_b.get("c"), 0);
    }

    #[test]
    fn receive_with_unknown_node_adds_it() {
        let mut vc_a = VectorClock::new("a", &[]);
        let mut vc_b = VectorClock::new("b", &[]);
        let ts = vc_a.send(); // a=1
        vc_b.receive(&ts);
        assert_eq!(vc_b.get("a"), 1);
        assert_eq!(vc_b.get("b"), 1);
    }

    #[test]
    fn receive_with_lower_remote_still_increments_local() {
        let mut vc_a = VectorClock::new("a", &["b"]);
        let ts = vc_a.send(); // a=1

        let mut vc_b = VectorClock::new("b", &["a"]);
        vc_b.tick(); // b=1
        vc_b.tick(); // b=2
        vc_b.tick(); // b=3

        vc_b.receive(&ts);
        assert_eq!(vc_b.get("a"), 1);
        assert_eq!(vc_b.get("b"), 4); // 3 + 1
    }

    #[test]
    fn snapshot_returns_current_state() {
        let mut vc = VectorClock::new("a", &["b"]);
        vc.tick();
        let ts = vc.snapshot();
        assert_eq!(ts.get("a"), 1);
        assert_eq!(ts.get("b"), 0);
    }

    #[test]
    fn snapshot_does_not_mutate_clock() {
        let mut vc = VectorClock::new("a", &["b"]);
        vc.tick();
        let _ = vc.snapshot();
        assert_eq!(vc.get("a"), 1);
        let _ = vc.snapshot();
        assert_eq!(vc.get("a"), 1);
    }

    #[test]
    fn get_returns_value_for_known_node() {
        let mut vc = VectorClock::new("a", &["b"]);
        vc.tick();
        assert_eq!(vc.get("a"), 1);
    }

    #[test]
    fn get_returns_zero_for_unknown_node() {
        let vc = VectorClock::new("a", &[]);
        assert_eq!(vc.get("unknown"), 0);
    }

    #[test]
    fn merge_takes_max_without_incrementing() {
        let mut vc_a = VectorClock::new("a", &["b"]);
        vc_a.tick(); // a=1

        let mut vc_b = VectorClock::new("b", &["a"]);
        vc_b.tick(); // b=1
        vc_b.tick(); // b=2

        vc_a.merge(&vc_b);
        assert_eq!(vc_a.get("a"), 1); // unchanged
        assert_eq!(vc_a.get("b"), 2); // max(0, 2)
    }

    #[test]
    fn merge_is_commutative() {
        let mut vc_a = VectorClock::new("a", &["b"]);
        vc_a.tick();

        let mut vc_b = VectorClock::new("b", &["a"]);
        vc_b.tick();

        let vc_a2 = vc_a.clone();
        let vc_b2 = vc_b.clone();

        vc_a.merge(&vc_b);
        vc_b.merge(&vc_a2);

        // After merge, both should have the same view
        let mut vc_a2_clone = vc_a2.clone();
        vc_a2_clone.merge(&vc_b2);

        assert_eq!(vc_a.get("a"), vc_a2_clone.get("a"));
        assert_eq!(vc_a.get("b"), vc_a2_clone.get("b"));
    }

    #[test]
    fn merge_adds_unknown_nodes() {
        let mut vc_a = VectorClock::new("a", &[]);
        let mut vc_b = VectorClock::new("b", &["c"]);
        vc_b.tick();

        vc_a.merge(&vc_b);
        assert_eq!(vc_a.get("b"), 1);
        assert_eq!(vc_a.get("c"), 0);
    }

    #[test]
    fn relation_delegates_to_compare() {
        let mut vc_a = VectorClock::new("a", &["b"]);
        let vc_b = VectorClock::new("b", &["a"]);
        vc_a.tick();

        let rel = vc_a.relation(&vc_b);
        assert_eq!(rel, CausalityRelation::HappensAfter);
    }

    #[test]
    fn timestamp_display_format() {
        let mut clocks = HashMap::new();
        clocks.insert("svc-b".to_string(), 1);
        clocks.insert("svc-a".to_string(), 3);
        let ts = VectorTimestamp { clocks };
        assert_eq!(format!("{}", ts), "svc-a=3,svc-b=1");
    }

    #[test]
    fn timestamp_parse_roundtrip() {
        let mut clocks = HashMap::new();
        clocks.insert("a".to_string(), 2);
        clocks.insert("b".to_string(), 5);
        let ts = VectorTimestamp { clocks };
        let s = ts.to_string();
        let parsed: VectorTimestamp = s.parse().unwrap();
        assert_eq!(ts, parsed);
    }

    #[test]
    fn timestamp_parse_empty_string() {
        let result = "".parse::<VectorTimestamp>();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VectorTimestampError::Empty));
    }

    #[test]
    fn timestamp_parse_missing_delimiter() {
        let result = "abc".parse::<VectorTimestamp>();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VectorTimestampError::MissingDelimiter
        ));
    }

    #[test]
    fn timestamp_parse_invalid_time() {
        let result = "a=xyz".parse::<VectorTimestamp>();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VectorTimestampError::InvalidTime(_)
        ));
    }

    #[test]
    fn timestamp_parse_empty_node_id() {
        let result = "=5".parse::<VectorTimestamp>();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VectorTimestampError::EmptyNodeId
        ));
    }

    #[test]
    fn serde_json_roundtrip_vector_clock() {
        let mut vc = VectorClock::new("a", &["b"]);
        vc.tick();
        let json = serde_json::to_string(&vc).unwrap();
        let deserialized: VectorClock = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.get("a"), 1);
        assert_eq!(deserialized.get("b"), 0);
    }

    #[test]
    fn serde_json_roundtrip_vector_timestamp() {
        let mut clocks = HashMap::new();
        clocks.insert("x".to_string(), 10);
        clocks.insert("y".to_string(), 20);
        let ts = VectorTimestamp { clocks };
        let json = serde_json::to_string(&ts).unwrap();
        let deserialized: VectorTimestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, deserialized);
    }

    #[test]
    fn three_process_message_chain() {
        let mut a = VectorClock::new("a", &["b", "c"]);
        let mut b = VectorClock::new("b", &["a", "c"]);
        let mut c = VectorClock::new("c", &["a", "b"]);

        let ts_a = a.send(); // A sends to B
        b.receive(&ts_a);

        let ts_b = b.send(); // B sends to C
        c.receive(&ts_b);

        // C has causal knowledge of A
        assert!(c.get("a") >= 1);
        assert_eq!(a.relation(&c), CausalityRelation::HappensBefore);
    }

    #[test]
    fn fork_join_causal_graph() {
        let mut a = VectorClock::new("a", &["b", "c", "d"]);
        let mut b = VectorClock::new("b", &["a", "c", "d"]);
        let mut c = VectorClock::new("c", &["a", "b", "d"]);
        let mut d = VectorClock::new("d", &["a", "b", "c"]);

        // A sends to B and C
        let ts_a = a.send();
        b.receive(&ts_a);
        c.receive(&ts_a);

        // B and C send to D
        let ts_b = b.send();
        let ts_c = c.send();
        d.receive(&ts_b);
        d.receive(&ts_c);

        // D sees both B and C
        assert!(d.get("b") >= 1);
        assert!(d.get("c") >= 1);
        assert!(d.get("a") >= 1);
    }

    #[test]
    fn concurrent_writes_detected() {
        let mut a = VectorClock::new("a", &["b"]);
        let mut b = VectorClock::new("b", &["a"]);

        a.tick();
        b.tick();

        assert_eq!(a.relation(&b), CausalityRelation::Concurrent);
    }
}
