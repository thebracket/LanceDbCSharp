use lancedb::DistanceType;

pub enum MetricType {
    L2 = 1,
    Cosine = 2,
    Dot = 3,
}

impl From<u32> for MetricType {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::L2,
            2 => Self::Cosine,
            3 => Self::Dot,
            _ => panic!("Invalid metric type: {}", value),
        }
    }
}

impl Into<DistanceType> for MetricType {
    fn into(self) -> DistanceType {
        match self {
            MetricType::L2 => DistanceType::L2,
            MetricType::Cosine => DistanceType::Cosine,
            MetricType::Dot => DistanceType::Dot,
        }
    }
}
