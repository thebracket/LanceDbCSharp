use lancedb::DistanceType;
use strum::FromRepr;

#[derive(Copy, Clone, Debug, FromRepr)]
#[repr(u32)]
pub enum MetricType {
    None = 0, // For FFI with C# to indicate that there isn't one
    L2 = 1,
    Cosine = 2,
    Dot = 3,
    Hamming = 4,
}

impl Into<DistanceType> for MetricType {
    fn into(self) -> DistanceType {
        match self {
            MetricType::L2 => DistanceType::L2,
            MetricType::Cosine => DistanceType::Cosine,
            MetricType::Dot => DistanceType::Dot,
            MetricType::Hamming => DistanceType::Hamming,
            _ => DistanceType::L2, // None isn't actually representable.
        }
    }
}

impl From<DistanceType> for MetricType {
    fn from(value: DistanceType) -> Self {
        match value {
            DistanceType::L2 => Self::L2,
            DistanceType::Cosine => Self::Cosine,
            DistanceType::Dot => Self::Dot,
            DistanceType::Hamming => Self::Hamming,
            _ => Self::L2, // None isn't actually representable.
        }
    }
}
