// server/src/cli/serializers.rs
use serde::{self, Deserialize, Deserializer, Serializer, Serialize, de::Error};

pub mod string_or_u16 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrU16 {
            String(String),
            U16(u16),
        }

        let optional_value: Option<StringOrU16> = Option::deserialize(deserializer)?;

        match optional_value {
            Some(StringOrU16::String(s)) => Ok(Some(s)),
            Some(StringOrU16::U16(u)) => Ok(Some(u.to_string())),
            None => Ok(None),
        }
    }

    pub fn serialize<S>(s: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match s {
            Some(value) => serializer.serialize_some(value),
            None => serializer.serialize_none(),
        }
    }
}

pub mod string_or_u16_non_option {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrU16 {
            String(String),
            U16(u16),
        }

        let value: StringOrU16 = Deserialize::deserialize(deserializer)?;

        match value {
            StringOrU16::String(s) => Ok(s),
            StringOrU16::U16(u) => Ok(u.to_string()),
        }
    }

    pub fn serialize<S>(s: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(s.as_str())
    }
}