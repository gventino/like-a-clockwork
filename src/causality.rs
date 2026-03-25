use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;

use crate::vector::VectorTimestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CausalityRelation {
    /// a → b — event A caused event B
    HappensBefore,
    /// b → a — event B caused event A
    HappensAfter,
    /// a ∥ b — events are concurrent
    Concurrent,
    /// a = b — same causal state
    Equal,
}

impl CausalityRelation {
    pub fn inverse(&self) -> Self {
        match self {
            Self::HappensBefore => Self::HappensAfter,
            Self::HappensAfter => Self::HappensBefore,
            Self::Concurrent => Self::Concurrent,
            Self::Equal => Self::Equal,
        }
    }

    pub fn is_causal(&self) -> bool {
        matches!(self, Self::HappensBefore | Self::HappensAfter)
    }

    pub fn is_concurrent(&self) -> bool {
        matches!(self, Self::Concurrent)
    }
}

impl fmt::Display for CausalityRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HappensBefore => write!(f, "→"),
            Self::HappensAfter => write!(f, "←"),
            Self::Concurrent => write!(f, "∥"),
            Self::Equal => write!(f, "="),
        }
    }
}

pub fn compare(a: &VectorTimestamp, b: &VectorTimestamp) -> CausalityRelation {
    let keys: HashSet<&String> = a.clocks().keys().chain(b.clocks().keys()).collect();

    let mut a_leq_b = true;
    let mut b_leq_a = true;

    for key in &keys {
        let va = a.get(key);
        let vb = b.get(key);
        if va > vb {
            a_leq_b = false;
        }
        if vb > va {
            b_leq_a = false;
        }
    }

    match (a_leq_b, b_leq_a) {
        (true, true) => CausalityRelation::Equal,
        (true, false) => CausalityRelation::HappensBefore,
        (false, true) => CausalityRelation::HappensAfter,
        (false, false) => CausalityRelation::Concurrent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inverse_happens_before_returns_happens_after() {
        assert_eq!(CausalityRelation::HappensBefore.inverse(), CausalityRelation::HappensAfter);
    }

    #[test]
    fn inverse_happens_after_returns_happens_before() {
        assert_eq!(CausalityRelation::HappensAfter.inverse(), CausalityRelation::HappensBefore);
    }

    #[test]
    fn inverse_concurrent_returns_concurrent() {
        assert_eq!(CausalityRelation::Concurrent.inverse(), CausalityRelation::Concurrent);
    }

    #[test]
    fn inverse_equal_returns_equal() {
        assert_eq!(CausalityRelation::Equal.inverse(), CausalityRelation::Equal);
    }

    #[test]
    fn double_inverse_is_identity() {
        for variant in [
            CausalityRelation::HappensBefore,
            CausalityRelation::HappensAfter,
            CausalityRelation::Concurrent,
            CausalityRelation::Equal,
        ] {
            assert_eq!(variant.inverse().inverse(), variant);
        }
    }

    #[test]
    fn is_causal_true_for_happens_before() {
        assert!(CausalityRelation::HappensBefore.is_causal());
    }

    #[test]
    fn is_causal_true_for_happens_after() {
        assert!(CausalityRelation::HappensAfter.is_causal());
    }

    #[test]
    fn is_causal_false_for_concurrent() {
        assert!(!CausalityRelation::Concurrent.is_causal());
    }

    #[test]
    fn is_causal_false_for_equal() {
        assert!(!CausalityRelation::Equal.is_causal());
    }

    #[test]
    fn is_concurrent_true_for_concurrent() {
        assert!(CausalityRelation::Concurrent.is_concurrent());
    }

    #[test]
    fn is_concurrent_false_for_others() {
        assert!(!CausalityRelation::HappensBefore.is_concurrent());
        assert!(!CausalityRelation::HappensAfter.is_concurrent());
        assert!(!CausalityRelation::Equal.is_concurrent());
    }

    #[test]
    fn display_happens_before() {
        assert_eq!(format!("{}", CausalityRelation::HappensBefore), "→");
    }

    #[test]
    fn display_happens_after() {
        assert_eq!(format!("{}", CausalityRelation::HappensAfter), "←");
    }

    #[test]
    fn display_concurrent() {
        assert_eq!(format!("{}", CausalityRelation::Concurrent), "∥");
    }

    #[test]
    fn display_equal() {
        assert_eq!(format!("{}", CausalityRelation::Equal), "=");
    }

    #[test]
    fn serde_json_roundtrip_all_variants() {
        for variant in [
            CausalityRelation::HappensBefore,
            CausalityRelation::HappensAfter,
            CausalityRelation::Concurrent,
            CausalityRelation::Equal,
        ] {
            let json = serde_json::to_string(&variant).unwrap();
            let deserialized: CausalityRelation = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, deserialized);
        }
    }

    #[test]
    fn compare_equal_vectors() {
        use std::collections::HashMap;
        let mut m = HashMap::new();
        m.insert("a".to_string(), 1u64);
        m.insert("b".to_string(), 2u64);
        let ts_a = VectorTimestamp::from(m.clone());
        let ts_b = VectorTimestamp::from(m);
        assert_eq!(compare(&ts_a, &ts_b), CausalityRelation::Equal);
    }

    #[test]
    fn compare_strictly_less() {
        use std::collections::HashMap;
        let mut ma = HashMap::new();
        ma.insert("a".to_string(), 1u64);
        ma.insert("b".to_string(), 1u64);
        let mut mb = HashMap::new();
        mb.insert("a".to_string(), 2u64);
        mb.insert("b".to_string(), 2u64);
        let ts_a = VectorTimestamp::from(ma);
        let ts_b = VectorTimestamp::from(mb);
        assert_eq!(compare(&ts_a, &ts_b), CausalityRelation::HappensBefore);
    }

    #[test]
    fn compare_less_or_equal_with_one_strict() {
        use std::collections::HashMap;
        let mut ma = HashMap::new();
        ma.insert("a".to_string(), 1u64);
        ma.insert("b".to_string(), 2u64);
        let mut mb = HashMap::new();
        mb.insert("a".to_string(), 2u64);
        mb.insert("b".to_string(), 2u64);
        let ts_a = VectorTimestamp::from(ma);
        let ts_b = VectorTimestamp::from(mb);
        assert_eq!(compare(&ts_a, &ts_b), CausalityRelation::HappensBefore);
    }

    #[test]
    fn compare_strictly_greater() {
        use std::collections::HashMap;
        let mut ma = HashMap::new();
        ma.insert("a".to_string(), 3u64);
        ma.insert("b".to_string(), 3u64);
        let mut mb = HashMap::new();
        mb.insert("a".to_string(), 1u64);
        mb.insert("b".to_string(), 1u64);
        let ts_a = VectorTimestamp::from(ma);
        let ts_b = VectorTimestamp::from(mb);
        assert_eq!(compare(&ts_a, &ts_b), CausalityRelation::HappensAfter);
    }

    #[test]
    fn compare_concurrent() {
        use std::collections::HashMap;
        let mut ma = HashMap::new();
        ma.insert("a".to_string(), 2u64);
        ma.insert("b".to_string(), 1u64);
        let mut mb = HashMap::new();
        mb.insert("a".to_string(), 1u64);
        mb.insert("b".to_string(), 2u64);
        let ts_a = VectorTimestamp::from(ma);
        let ts_b = VectorTimestamp::from(mb);
        assert_eq!(compare(&ts_a, &ts_b), CausalityRelation::Concurrent);
    }

    #[test]
    fn compare_with_different_sized_vectors() {
        use std::collections::HashMap;
        let mut ma = HashMap::new();
        ma.insert("a".to_string(), 1u64);
        let mut mb = HashMap::new();
        mb.insert("a".to_string(), 1u64);
        mb.insert("b".to_string(), 1u64);
        let ts_a = VectorTimestamp::from(ma);
        let ts_b = VectorTimestamp::from(mb);
        // a has a=1, b=0(missing); b has a=1, b=1 → a <= b, so HappensBefore
        assert_eq!(compare(&ts_a, &ts_b), CausalityRelation::HappensBefore);
    }

    #[test]
    fn compare_inverse_symmetry() {
        use std::collections::HashMap;
        let mut ma = HashMap::new();
        ma.insert("a".to_string(), 1u64);
        ma.insert("b".to_string(), 0u64);
        let mut mb = HashMap::new();
        mb.insert("a".to_string(), 0u64);
        mb.insert("b".to_string(), 1u64);
        let ts_a = VectorTimestamp::from(ma);
        let ts_b = VectorTimestamp::from(mb);
        assert_eq!(compare(&ts_a, &ts_b), compare(&ts_b, &ts_a).inverse());
    }
}
