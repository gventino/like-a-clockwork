use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::vector::VectorTimestamp;

#[derive(Debug, thiserror::Error)]
pub enum TracedEventError {
    #[error("missing required header: {0}")]
    MissingHeader(String),

    #[error("invalid vector clock: {0}")]
    InvalidVectorClock(String),

    #[error("invalid event type: cannot be empty")]
    InvalidEventType,

    #[error("json parse error: {0}")]
    JsonError(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracedEvent {
    event_type: String,
    payload: Vec<u8>,
    causality: VectorTimestamp,
    event_id: String,
    timestamp_utc: Option<String>,
}

impl TracedEvent {
    pub fn new(
        event_type: &str,
        payload: &[u8],
        causality: VectorTimestamp,
    ) -> Result<Self, TracedEventError> {
        if event_type.is_empty() {
            return Err(TracedEventError::InvalidEventType);
        }
        Ok(Self {
            event_type: event_type.to_string(),
            payload: payload.to_vec(),
            causality,
            event_id: uuid::Uuid::now_v7().to_string(),
            timestamp_utc: None,
        })
    }

    pub fn with_id(
        event_id: &str,
        event_type: &str,
        payload: &[u8],
        causality: VectorTimestamp,
    ) -> Result<Self, TracedEventError> {
        if event_type.is_empty() {
            return Err(TracedEventError::InvalidEventType);
        }
        Ok(Self {
            event_type: event_type.to_string(),
            payload: payload.to_vec(),
            causality,
            event_id: event_id.to_string(),
            timestamp_utc: None,
        })
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn causality(&self) -> &VectorTimestamp {
        &self.causality
    }

    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    pub fn timestamp_utc(&self) -> Option<&str> {
        self.timestamp_utc.as_deref()
    }

    pub fn to_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert(
            "X-Causality-Vector".to_string(),
            self.causality.to_string(),
        );
        headers.insert(
            "X-Causality-EventId".to_string(),
            self.event_id.clone(),
        );
        headers.insert(
            "X-Causality-EventType".to_string(),
            self.event_type.clone(),
        );
        headers
    }

    pub fn from_headers(
        headers: &HashMap<String, String>,
        payload: &[u8],
    ) -> Result<Self, TracedEventError> {
        let vector_str = headers
            .get("X-Causality-Vector")
            .ok_or_else(|| TracedEventError::MissingHeader("X-Causality-Vector".to_string()))?;
        let event_id = headers
            .get("X-Causality-EventId")
            .ok_or_else(|| TracedEventError::MissingHeader("X-Causality-EventId".to_string()))?;
        let event_type = headers
            .get("X-Causality-EventType")
            .ok_or_else(|| TracedEventError::MissingHeader("X-Causality-EventType".to_string()))?;

        let causality = vector_str
            .parse::<VectorTimestamp>()
            .map_err(|e| TracedEventError::InvalidVectorClock(e.to_string()))?;

        Ok(Self {
            event_type: event_type.clone(),
            payload: payload.to_vec(),
            causality,
            event_id: event_id.clone(),
            timestamp_utc: None,
        })
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::json!({
            "_causality": {
                "vector": serde_json::to_value(self.causality.clocks()).unwrap(),
                "event_id": self.event_id,
                "event_type": self.event_type,
            },
            "payload": serde_json::to_value(&self.payload).unwrap(),
        })
    }

    pub fn from_json_value(value: &serde_json::Value) -> Result<Self, TracedEventError> {
        let causality_obj = value
            .get("_causality")
            .ok_or_else(|| TracedEventError::MissingHeader("_causality".to_string()))?;

        let vector_map: HashMap<String, u64> = serde_json::from_value(
            causality_obj
                .get("vector")
                .ok_or_else(|| TracedEventError::MissingHeader("vector".to_string()))?
                .clone(),
        )?;

        let event_id: String = serde_json::from_value(
            causality_obj
                .get("event_id")
                .ok_or_else(|| TracedEventError::MissingHeader("event_id".to_string()))?
                .clone(),
        )?;

        let event_type: String = serde_json::from_value(
            causality_obj
                .get("event_type")
                .ok_or_else(|| TracedEventError::MissingHeader("event_type".to_string()))?
                .clone(),
        )?;

        let payload: Vec<u8> = serde_json::from_value(
            value
                .get("payload")
                .ok_or_else(|| TracedEventError::MissingHeader("payload".to_string()))?
                .clone(),
        )?;

        Ok(Self {
            event_type,
            payload,
            causality: VectorTimestamp::from(vector_map),
            event_id,
            timestamp_utc: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_causality() -> VectorTimestamp {
        let mut clocks = HashMap::new();
        clocks.insert("svc-a".to_string(), 3);
        clocks.insert("svc-b".to_string(), 1);
        VectorTimestamp::from(clocks)
    }

    #[test]
    fn new_preserves_event_type() {
        let event = TracedEvent::new("order.created", b"data", sample_causality()).unwrap();
        assert_eq!(event.event_type(), "order.created");
    }

    #[test]
    fn new_preserves_payload() {
        let payload = b"hello world";
        let event = TracedEvent::new("order.created", payload, sample_causality()).unwrap();
        assert_eq!(event.payload(), payload);
    }

    #[test]
    fn new_captures_vector_timestamp() {
        let causality = sample_causality();
        let event = TracedEvent::new("order.created", b"data", causality.clone()).unwrap();
        assert_eq!(event.causality(), &causality);
    }

    #[test]
    fn new_generates_unique_event_ids() {
        let e1 = TracedEvent::new("order.created", b"a", sample_causality()).unwrap();
        let e2 = TracedEvent::new("order.created", b"b", sample_causality()).unwrap();
        assert_ne!(e1.event_id(), e2.event_id());
    }

    #[test]
    fn new_rejects_empty_event_type() {
        let result = TracedEvent::new("", b"data", sample_causality());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TracedEventError::InvalidEventType));
    }

    #[test]
    fn new_accepts_empty_payload() {
        let event = TracedEvent::new("order.created", b"", sample_causality()).unwrap();
        assert!(event.payload().is_empty());
    }

    #[test]
    fn with_id_uses_provided_id() {
        let event =
            TracedEvent::with_id("my-id-123", "order.created", b"data", sample_causality())
                .unwrap();
        assert_eq!(event.event_id(), "my-id-123");
    }

    #[test]
    fn accessors_return_correct_values() {
        let causality = sample_causality();
        let event =
            TracedEvent::with_id("ev-1", "user.signup", b"payload", causality.clone()).unwrap();
        assert_eq!(event.event_type(), "user.signup");
        assert_eq!(event.payload(), b"payload");
        assert_eq!(event.causality(), &causality);
        assert_eq!(event.event_id(), "ev-1");
        assert_eq!(event.timestamp_utc(), None);
    }

    #[test]
    fn to_headers_contains_vector_clock() {
        let event =
            TracedEvent::with_id("ev-1", "order.created", b"data", sample_causality()).unwrap();
        let headers = event.to_headers();
        let vector = headers.get("X-Causality-Vector").unwrap();
        assert_eq!(vector, "svc-a=3,svc-b=1");
    }

    #[test]
    fn to_headers_contains_event_id() {
        let event =
            TracedEvent::with_id("ev-1", "order.created", b"data", sample_causality()).unwrap();
        let headers = event.to_headers();
        assert_eq!(headers.get("X-Causality-EventId").unwrap(), "ev-1");
    }

    #[test]
    fn to_headers_contains_event_type() {
        let event =
            TracedEvent::with_id("ev-1", "order.created", b"data", sample_causality()).unwrap();
        let headers = event.to_headers();
        assert_eq!(headers.get("X-Causality-EventType").unwrap(), "order.created");
    }

    #[test]
    fn to_headers_does_not_include_payload() {
        let event =
            TracedEvent::with_id("ev-1", "order.created", b"secret", sample_causality()).unwrap();
        let headers = event.to_headers();
        assert_eq!(headers.len(), 3);
        for value in headers.values() {
            assert!(!value.contains("secret"));
        }
    }

    #[test]
    fn from_headers_roundtrip() {
        let payload = b"roundtrip-data";
        let original =
            TracedEvent::with_id("ev-rt", "order.created", payload, sample_causality()).unwrap();
        let headers = original.to_headers();
        let restored = TracedEvent::from_headers(&headers, payload).unwrap();

        assert_eq!(restored.event_type(), original.event_type());
        assert_eq!(restored.event_id(), original.event_id());
        assert_eq!(restored.payload(), original.payload());
        assert_eq!(restored.causality(), original.causality());
    }

    #[test]
    fn from_headers_missing_vector_returns_error() {
        let mut headers = HashMap::new();
        headers.insert("X-Causality-EventId".to_string(), "ev-1".to_string());
        headers.insert("X-Causality-EventType".to_string(), "order.created".to_string());

        let result = TracedEvent::from_headers(&headers, b"data");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TracedEventError::MissingHeader(h) if h == "X-Causality-Vector"
        ));
    }

    #[test]
    fn from_headers_missing_event_id_returns_error() {
        let mut headers = HashMap::new();
        headers.insert("X-Causality-Vector".to_string(), "svc-a=3".to_string());
        headers.insert("X-Causality-EventType".to_string(), "order.created".to_string());

        let result = TracedEvent::from_headers(&headers, b"data");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TracedEventError::MissingHeader(h) if h == "X-Causality-EventId"
        ));
    }

    #[test]
    fn from_headers_invalid_vector_returns_error() {
        let mut headers = HashMap::new();
        headers.insert("X-Causality-Vector".to_string(), "not-valid".to_string());
        headers.insert("X-Causality-EventId".to_string(), "ev-1".to_string());
        headers.insert("X-Causality-EventType".to_string(), "order.created".to_string());

        let result = TracedEvent::from_headers(&headers, b"data");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TracedEventError::InvalidVectorClock(_)
        ));
    }

    #[test]
    fn to_json_contains_causality_field() {
        let event =
            TracedEvent::with_id("ev-1", "order.created", b"hi", sample_causality()).unwrap();
        let json = event.to_json_value();

        let causality = json.get("_causality").expect("missing _causality");
        assert_eq!(causality.get("event_id").unwrap(), "ev-1");
        assert_eq!(causality.get("event_type").unwrap(), "order.created");

        let vector = causality.get("vector").unwrap().as_object().unwrap();
        assert_eq!(vector.get("svc-a").unwrap(), 3);
        assert_eq!(vector.get("svc-b").unwrap(), 1);
    }

    #[test]
    fn from_json_roundtrip() {
        let original =
            TracedEvent::with_id("ev-json", "order.created", b"json-data", sample_causality())
                .unwrap();
        let json = original.to_json_value();
        let restored = TracedEvent::from_json_value(&json).unwrap();

        assert_eq!(restored.event_type(), original.event_type());
        assert_eq!(restored.event_id(), original.event_id());
        assert_eq!(restored.payload(), original.payload());
        assert_eq!(restored.causality(), original.causality());
    }

    #[test]
    fn serde_json_roundtrip() {
        let original =
            TracedEvent::with_id("ev-serde", "user.signup", b"serde-payload", sample_causality())
                .unwrap();
        let json_str = serde_json::to_string(&original).unwrap();
        let restored: TracedEvent = serde_json::from_str(&json_str).unwrap();

        assert_eq!(restored.event_type(), original.event_type());
        assert_eq!(restored.event_id(), original.event_id());
        assert_eq!(restored.payload(), original.payload());
        assert_eq!(restored.causality(), original.causality());
        assert_eq!(restored.timestamp_utc(), original.timestamp_utc());
    }
}
