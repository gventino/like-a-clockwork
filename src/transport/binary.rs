use std::collections::HashMap;

use super::{BinaryHeaderMap, TransportError};
use crate::event::TracedEvent;
use crate::lamport::LamportTimestamp;
use crate::vector::VectorTimestamp;

const LAMPORT_KEY: &str = "causality-lc";
const VECTOR_KEY: &str = "causality-vc";
const EVENT_ID_KEY: &str = "causality-eid";
const EVENT_TYPE_KEY: &str = "causality-etype";

pub fn inject_lamport(
    headers: &mut impl BinaryHeaderMap,
    timestamp: &LamportTimestamp,
) -> Result<(), TransportError> {
    headers.set(
        LAMPORT_KEY.to_string(),
        timestamp.to_string().into_bytes(),
    );
    Ok(())
}

pub fn extract_lamport(
    headers: &impl BinaryHeaderMap,
) -> Result<Option<LamportTimestamp>, TransportError> {
    match headers.get(LAMPORT_KEY) {
        None => Ok(None),
        Some(bytes) => {
            let s = std::str::from_utf8(bytes).map_err(|e| TransportError::MalformedValue {
                key: LAMPORT_KEY.to_string(),
                reason: e.to_string(),
            })?;
            s.parse::<LamportTimestamp>()
                .map(Some)
                .map_err(|e| TransportError::MalformedValue {
                    key: LAMPORT_KEY.to_string(),
                    reason: e.to_string(),
                })
        }
    }
}

pub fn inject_vector(
    headers: &mut impl BinaryHeaderMap,
    timestamp: &VectorTimestamp,
) -> Result<(), TransportError> {
    let bytes = rmp_serde::to_vec(timestamp.clocks())
        .map_err(|e| TransportError::SerializationError(e.to_string()))?;
    headers.set(VECTOR_KEY.to_string(), bytes);
    Ok(())
}

pub fn extract_vector(
    headers: &impl BinaryHeaderMap,
) -> Result<Option<VectorTimestamp>, TransportError> {
    match headers.get(VECTOR_KEY) {
        None => Ok(None),
        Some(bytes) => {
            let clocks: HashMap<String, u64> = rmp_serde::from_slice(bytes)
                .map_err(|e| TransportError::DeserializationError(e.to_string()))?;
            Ok(Some(VectorTimestamp::from(clocks)))
        }
    }
}

pub fn inject_event(
    headers: &mut impl BinaryHeaderMap,
    event: &TracedEvent,
) -> Result<(), TransportError> {
    inject_vector(headers, event.causality())?;
    headers.set(EVENT_ID_KEY.to_string(), event.event_id().as_bytes().to_vec());
    headers.set(
        EVENT_TYPE_KEY.to_string(),
        event.event_type().as_bytes().to_vec(),
    );
    Ok(())
}

pub fn extract_event(
    headers: &impl BinaryHeaderMap,
    payload: &[u8],
) -> Result<TracedEvent, TransportError> {
    let causality = extract_vector(headers)?
        .ok_or_else(|| TransportError::MissingKey(VECTOR_KEY.to_string()))?;

    let event_id_bytes = headers
        .get(EVENT_ID_KEY)
        .ok_or_else(|| TransportError::MissingKey(EVENT_ID_KEY.to_string()))?;
    let event_id = std::str::from_utf8(event_id_bytes).map_err(|e| {
        TransportError::MalformedValue {
            key: EVENT_ID_KEY.to_string(),
            reason: e.to_string(),
        }
    })?;

    let event_type_bytes = headers
        .get(EVENT_TYPE_KEY)
        .ok_or_else(|| TransportError::MissingKey(EVENT_TYPE_KEY.to_string()))?;
    let event_type = std::str::from_utf8(event_type_bytes).map_err(|e| {
        TransportError::MalformedValue {
            key: EVENT_TYPE_KEY.to_string(),
            reason: e.to_string(),
        }
    })?;

    TracedEvent::with_id(event_id, event_type, payload, causality).map_err(|e| {
        TransportError::DeserializationError(e.to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn binary_inject_lamport_sets_utf8_bytes() {
        let mut headers = HashMap::<String, Vec<u8>>::new();
        inject_lamport(&mut headers, &sample_lamport()).unwrap();
        let bytes = BinaryHeaderMap::get(&headers, "causality-lc").unwrap();
        assert_eq!(bytes, b"order-svc:42");
    }

    #[test]
    fn binary_extract_lamport_roundtrip() {
        let mut headers = HashMap::<String, Vec<u8>>::new();
        let ts = sample_lamport();
        inject_lamport(&mut headers, &ts).unwrap();
        let extracted = extract_lamport(&headers).unwrap().unwrap();
        assert_eq!(extracted, ts);
    }

    #[test]
    fn binary_extract_lamport_missing_returns_none() {
        let headers = HashMap::<String, Vec<u8>>::new();
        assert!(extract_lamport(&headers).unwrap().is_none());
    }

    #[test]
    fn binary_inject_vector_produces_msgpack() {
        let mut headers = HashMap::<String, Vec<u8>>::new();
        inject_vector(&mut headers, &sample_vector()).unwrap();
        let bytes = BinaryHeaderMap::get(&headers, "causality-vc").unwrap();
        // Verify it's valid msgpack by deserializing
        let clocks: HashMap<String, u64> = rmp_serde::from_slice(bytes).unwrap();
        assert_eq!(clocks.get("svc-a"), Some(&3));
        assert_eq!(clocks.get("svc-b"), Some(&1));
    }

    #[test]
    fn binary_extract_vector_roundtrip() {
        let mut headers = HashMap::<String, Vec<u8>>::new();
        let ts = sample_vector();
        inject_vector(&mut headers, &ts).unwrap();
        let extracted = extract_vector(&headers).unwrap().unwrap();
        assert_eq!(extracted, ts);
    }

    #[test]
    fn binary_extract_vector_missing_returns_none() {
        let headers = HashMap::<String, Vec<u8>>::new();
        assert!(extract_vector(&headers).unwrap().is_none());
    }

    #[test]
    fn binary_extract_vector_corrupt_bytes_returns_err() {
        let mut headers = HashMap::<String, Vec<u8>>::new();
        headers.insert("causality-vc".to_string(), vec![0xFF, 0xFE, 0xFD]);
        let result = extract_vector(&headers);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TransportError::DeserializationError(_)
        ));
    }

    #[test]
    fn binary_msgpack_is_compact() {
        let ts = sample_vector();
        let msgpack_bytes =
            rmp_serde::to_vec(ts.clocks()).unwrap();
        let json_bytes = serde_json::to_vec(ts.clocks()).unwrap();
        assert!(
            msgpack_bytes.len() < json_bytes.len(),
            "msgpack ({} bytes) should be smaller than JSON ({} bytes)",
            msgpack_bytes.len(),
            json_bytes.len()
        );
    }

    #[test]
    fn binary_inject_event_sets_all_headers() {
        let mut headers = HashMap::<String, Vec<u8>>::new();
        let event = sample_event();
        inject_event(&mut headers, &event).unwrap();
        assert!(BinaryHeaderMap::get(&headers, "causality-vc").is_some());
        assert_eq!(
            BinaryHeaderMap::get(&headers, "causality-eid"),
            Some(b"ev-1".as_slice())
        );
        assert_eq!(
            BinaryHeaderMap::get(&headers, "causality-etype"),
            Some(b"order.created".as_slice())
        );
    }

    #[test]
    fn binary_extract_event_roundtrip() {
        let mut headers = HashMap::<String, Vec<u8>>::new();
        let event = sample_event();
        let payload = event.payload().to_vec();
        inject_event(&mut headers, &event).unwrap();
        let extracted = extract_event(&headers, &payload).unwrap();
        assert_eq!(extracted.event_id(), event.event_id());
        assert_eq!(extracted.event_type(), event.event_type());
        assert_eq!(extracted.payload(), event.payload());
        assert_eq!(extracted.causality(), event.causality());
    }
}
