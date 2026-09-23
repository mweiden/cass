/// Zone map summarizing the minimum and maximum keys seen.
use prost::Message;
#[derive(Default)]
pub struct ZoneMap {
    /// Smallest key observed.
    pub min: Option<String>,
    /// Largest key observed.
    pub max: Option<String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ZoneMapProto {
    #[prost(string, optional, tag = "1")]
    pub min: Option<String>,
    #[prost(string, optional, tag = "2")]
    pub max: Option<String>,
}

impl ZoneMap {
    /// Update the zone map with a new `key`.
    pub fn update(&mut self, key: &str) {
        match self.min.as_deref() {
            Some(min) if key < min => self.min = Some(key.to_string()),
            None => self.min = Some(key.to_string()),
            _ => {}
        }
        match self.max.as_deref() {
            Some(max) if key > max => self.max = Some(key.to_string()),
            None => self.max = Some(key.to_string()),
            _ => {}
        }
    }

    /// Return `true` if `key` lies within the stored bounds. When bounds
    /// are missing the function defaults to `true` so as not to filter
    /// out potential matches.
    pub fn contains(&self, key: &str) -> bool {
        match (self.min.as_deref(), self.max.as_deref()) {
            (Some(min), Some(max)) => key >= min && key <= max,
            _ => true,
        }
    }

    /// Return `true` if any key starting with `prefix` could lie within the
    /// stored bounds. When bounds are missing the function defaults to
    /// `true` so as not to filter out potential matches.
    pub fn may_contain_prefix(&self, prefix: &str) -> bool {
        match (self.min.as_deref(), self.max.as_deref()) {
            (Some(min), Some(max)) => {
                // Keys with `prefix` form the interval [prefix, succ(prefix)).
                // They cannot exist if every key sorts below the prefix, or
                // if the smallest key already sorts past the whole interval.
                !(max < prefix || (min > prefix && !min.starts_with(prefix)))
            }
            _ => true,
        }
    }

    /// Convert the zone map into a protobuf message.
    pub fn to_proto(&self) -> ZoneMapProto {
        ZoneMapProto {
            min: self.min.clone(),
            max: self.max.clone(),
        }
    }

    /// Construct a zone map from a protobuf message.
    pub fn from_proto(proto: ZoneMapProto) -> Self {
        Self {
            min: proto.min,
            max: proto.max,
        }
    }

    /// Serialize the zone map using protobuf encoding.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.to_proto().encode(&mut buf).unwrap();
        buf
    }

    /// Reconstruct a zone map from protobuf bytes produced by [`to_bytes`].
    pub fn from_bytes(data: &[u8]) -> Self {
        let proto = ZoneMapProto::decode(data).unwrap();
        Self::from_proto(proto)
    }
}
