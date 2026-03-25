use std::collections::HashMap;

use super::TransportError;
use crate::vector::VectorTimestamp;

pub fn inject(
    json: &serde_json::Value,
    timestamp: &VectorTimestamp,
    event_type: &str,
    event_id: &str,
) -> Result<serde_json::Value, TransportError> {
    let obj = json
        .as_object()
        .ok_or_else(|| TransportError::SerializationError("input must be a JSON object".into()))?;

    let mut result = obj.clone();
    result.insert(
        "_causality".to_string(),
        serde_json::json!({
            "vector": serde_json::to_value(timestamp.clocks())
                .map_err(|e| TransportError::SerializationError(e.to_string()))?,
            "event_type": event_type,
            "event_id": event_id,
        }),
    );

    Ok(serde_json::Value::Object(result))
}

pub fn extract(
    json: &serde_json::Value,
) -> Result<(serde_json::Value, VectorTimestamp, String, String), TransportError> {
    let obj = json
        .as_object()
        .ok_or_else(|| TransportError::DeserializationError("input must be a JSON object".into()))?;

    let causality = obj
        .get("_causality")
        .ok_or_else(|| TransportError::MissingKey("_causality".to_string()))?;

    let vector_value = causality
        .get("vector")
        .ok_or_else(|| TransportError::MissingKey("_causality.vector".to_string()))?;
    let clocks: HashMap<String, u64> = serde_json::from_value(vector_value.clone())
        .map_err(|e| TransportError::DeserializationError(e.to_string()))?;

    let event_type = causality
        .get("event_type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransportError::MissingKey("_causality.event_type".to_string()))?
        .to_string();

    let event_id = causality
        .get("event_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| TransportError::MissingKey("_causality.event_id".to_string()))?
        .to_string();

    let mut payload = obj.clone();
    payload.remove("_causality");

    Ok((
        serde_json::Value::Object(payload),
        VectorTimestamp::from(clocks),
        event_type,
        event_id,
    ))
}

pub fn has_causality(json: &serde_json::Value) -> bool {
    json.as_object()
        .is_some_and(|obj| obj.contains_key("_causality"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_vector() -> VectorTimestamp {
        let mut clocks = HashMap::new();
        clocks.insert("svc-a".to_string(), 3);
        clocks.insert("svc-b".to_string(), 1);
        VectorTimestamp::from(clocks)
    }

    fn sample_json() -> serde_json::Value {
        serde_json::json!({
            "order_id": "abc-123",
            "amount": 42
        })
    }

    #[test]
    fn json_inject_adds_causality_field() {
        let result = inject(&sample_json(), &sample_vector(), "order.created", "ev-1").unwrap();
        let causality = result.get("_causality").expect("missing _causality");
        assert_eq!(causality.get("event_type").unwrap(), "order.created");
        assert_eq!(causality.get("event_id").unwrap(), "ev-1");
        let vector = causality.get("vector").unwrap().as_object().unwrap();
        assert_eq!(vector.get("svc-a").unwrap(), 3);
        assert_eq!(vector.get("svc-b").unwrap(), 1);
        // Original fields preserved
        assert_eq!(result.get("order_id").unwrap(), "abc-123");
    }

    #[test]
    fn json_inject_overwrites_existing_causality() {
        let mut json = sample_json();
        json.as_object_mut()
            .unwrap()
            .insert("_causality".to_string(), serde_json::json!("old"));
        let result = inject(&json, &sample_vector(), "order.created", "ev-1").unwrap();
        let causality = result.get("_causality").unwrap();
        assert_eq!(causality.get("event_type").unwrap(), "order.created");
    }

    #[test]
    fn json_extract_returns_payload_and_timestamp() {
        let injected = inject(&sample_json(), &sample_vector(), "order.created", "ev-1").unwrap();
        let (payload, ts, event_type, event_id) = extract(&injected).unwrap();
        assert_eq!(event_type, "order.created");
        assert_eq!(event_id, "ev-1");
        assert_eq!(ts, sample_vector());
        assert!(payload.get("_causality").is_none());
        assert_eq!(payload.get("order_id").unwrap(), "abc-123");
    }

    #[test]
    fn json_extract_missing_causality_returns_err() {
        let result = extract(&sample_json());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TransportError::MissingKey(k) if k == "_causality"
        ));
    }

    #[test]
    fn json_has_causality_true_when_present() {
        let injected = inject(&sample_json(), &sample_vector(), "order.created", "ev-1").unwrap();
        assert!(has_causality(&injected));
    }

    #[test]
    fn json_has_causality_false_when_absent() {
        assert!(!has_causality(&sample_json()));
    }

    #[test]
    fn json_inject_then_extract_roundtrip() {
        let original = sample_json();
        let ts = sample_vector();
        let injected = inject(&original, &ts, "order.created", "ev-1").unwrap();
        let (payload, extracted_ts, event_type, event_id) = extract(&injected).unwrap();
        assert_eq!(payload, original);
        assert_eq!(extracted_ts, ts);
        assert_eq!(event_type, "order.created");
        assert_eq!(event_id, "ev-1");
    }
}
