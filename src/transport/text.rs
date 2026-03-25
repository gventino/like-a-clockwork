use super::{HeaderMap, TransportError};
use crate::event::TracedEvent;
use crate::lamport::LamportTimestamp;
use crate::vector::VectorTimestamp;

const LAMPORT_KEY: &str = "X-Causality-Lamport";
const VECTOR_KEY: &str = "X-Causality-Vector";
const EVENT_ID_KEY: &str = "X-Causality-EventId";
const EVENT_TYPE_KEY: &str = "X-Causality-EventType";

pub fn inject_lamport(
    headers: &mut impl HeaderMap,
    timestamp: &LamportTimestamp,
) -> Result<(), TransportError> {
    headers.set(LAMPORT_KEY.to_string(), timestamp.to_string());
    Ok(())
}

pub fn extract_lamport(
    headers: &impl HeaderMap,
) -> Result<Option<LamportTimestamp>, TransportError> {
    match headers.get(LAMPORT_KEY) {
        None => Ok(None),
        Some(value) => value
            .parse::<LamportTimestamp>()
            .map(Some)
            .map_err(|e| TransportError::MalformedValue {
                key: LAMPORT_KEY.to_string(),
                reason: e.to_string(),
            }),
    }
}

pub fn inject_vector(
    headers: &mut impl HeaderMap,
    timestamp: &VectorTimestamp,
) -> Result<(), TransportError> {
    headers.set(VECTOR_KEY.to_string(), timestamp.to_string());
    Ok(())
}

pub fn extract_vector(
    headers: &impl HeaderMap,
) -> Result<Option<VectorTimestamp>, TransportError> {
    match headers.get(VECTOR_KEY) {
        None => Ok(None),
        Some(value) => value
            .parse::<VectorTimestamp>()
            .map(Some)
            .map_err(|e| TransportError::MalformedValue {
                key: VECTOR_KEY.to_string(),
                reason: e.to_string(),
            }),
    }
}

pub fn inject_event(
    headers: &mut impl HeaderMap,
    event: &TracedEvent,
) -> Result<(), TransportError> {
    let event_headers = event.to_headers();
    for (key, value) in event_headers {
        headers.set(key, value);
    }
    Ok(())
}

pub fn extract_event(
    headers: &impl HeaderMap,
    payload: &[u8],
) -> Result<TracedEvent, TransportError> {
    let vector_str = headers
        .get(VECTOR_KEY)
        .ok_or_else(|| TransportError::MissingKey(VECTOR_KEY.to_string()))?;
    let event_id = headers
        .get(EVENT_ID_KEY)
        .ok_or_else(|| TransportError::MissingKey(EVENT_ID_KEY.to_string()))?;
    let event_type = headers
        .get(EVENT_TYPE_KEY)
        .ok_or_else(|| TransportError::MissingKey(EVENT_TYPE_KEY.to_string()))?;

    let causality = vector_str
        .parse::<VectorTimestamp>()
        .map_err(|e| TransportError::MalformedValue {
            key: VECTOR_KEY.to_string(),
            reason: e.to_string(),
        })?;

    TracedEvent::with_id(event_id, event_type, payload, causality).map_err(|e| {
        TransportError::DeserializationError(e.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn sample_lamport() -> LamportTimestamp {
        "order-svc:42".parse().unwrap()
    }

    fn sample_vector() -> VectorTimestamp {
        let mut clocks = HashMap::new();
        clocks.insert("svc-a".to_string(), 3);
        clocks.insert("svc-b".to_string(), 1);
        VectorTimestamp::from(clocks)
    }

    fn sample_event() -> TracedEvent {
        TracedEvent::with_id("ev-1", "order.created", b"data", sample_vector()).unwrap()
    }

    #[test]
    fn text_inject_lamport_sets_correct_header() {
        let mut headers = HashMap::<String, String>::new();
        inject_lamport(&mut headers, &sample_lamport()).unwrap();
        assert_eq!(
            HeaderMap::get(&headers, "X-Causality-Lamport"),
            Some("order-svc:42")
        );
    }

    #[test]
    fn text_extract_lamport_roundtrip() {
        let mut headers = HashMap::<String, String>::new();
        let ts = sample_lamport();
        inject_lamport(&mut headers, &ts).unwrap();
        let extracted = extract_lamport(&headers).unwrap().unwrap();
        assert_eq!(extracted, ts);
    }

    #[test]
    fn text_extract_lamport_missing_returns_none() {
        let headers = HashMap::<String, String>::new();
        assert!(extract_lamport(&headers).unwrap().is_none());
    }

    #[test]
    fn text_extract_lamport_malformed_returns_err() {
        let mut headers = HashMap::<String, String>::new();
        headers.set("X-Causality-Lamport".to_string(), "no-colon".to_string());
        let result = extract_lamport(&headers);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TransportError::MalformedValue { .. }
        ));
    }

    #[test]
    fn text_inject_vector_sets_correct_header() {
        let mut headers = HashMap::<String, String>::new();
        inject_vector(&mut headers, &sample_vector()).unwrap();
        let value = HeaderMap::get(&headers, "X-Causality-Vector").unwrap();
        assert_eq!(value, "svc-a=3,svc-b=1");
    }

    #[test]
    fn text_extract_vector_roundtrip() {
        let mut headers = HashMap::<String, String>::new();
        let ts = sample_vector();
        inject_vector(&mut headers, &ts).unwrap();
        let extracted = extract_vector(&headers).unwrap().unwrap();
        assert_eq!(extracted, ts);
    }

    #[test]
    fn text_extract_vector_missing_returns_none() {
        let headers = HashMap::<String, String>::new();
        assert!(extract_vector(&headers).unwrap().is_none());
    }

    #[test]
    fn text_extract_vector_malformed_returns_err() {
        let mut headers = HashMap::<String, String>::new();
        headers.set("X-Causality-Vector".to_string(), "bad-format".to_string());
        let result = extract_vector(&headers);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TransportError::MalformedValue { .. }
        ));
    }

    #[test]
    fn text_inject_event_sets_all_headers() {
        let mut headers = HashMap::<String, String>::new();
        let event = sample_event();
        inject_event(&mut headers, &event).unwrap();
        assert!(HeaderMap::get(&headers, "X-Causality-Vector").is_some());
        assert_eq!(HeaderMap::get(&headers, "X-Causality-EventId"), Some("ev-1"));
        assert_eq!(
            HeaderMap::get(&headers, "X-Causality-EventType"),
            Some("order.created")
        );
    }

    #[test]
    fn text_extract_event_roundtrip() {
        let mut headers = HashMap::<String, String>::new();
        let event = sample_event();
        let payload = event.payload().to_vec();
        inject_event(&mut headers, &event).unwrap();
        let extracted = extract_event(&headers, &payload).unwrap();
        assert_eq!(extracted.event_id(), event.event_id());
        assert_eq!(extracted.event_type(), event.event_type());
        assert_eq!(extracted.payload(), event.payload());
        assert_eq!(extracted.causality(), event.causality());
    }

    #[test]
    fn text_extract_event_missing_required_header_returns_err() {
        let mut headers = HashMap::<String, String>::new();
        headers.set("X-Causality-Vector".to_string(), "svc-a=3".to_string());
        // Missing EventId and EventType
        let result = extract_event(&headers, b"data");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TransportError::MissingKey(_)
        ));
    }
}
