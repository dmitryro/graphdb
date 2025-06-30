// lib/src/engine/properties.rs

use std::collections::BTreeMap;
use std::fmt;
use ordered_float::NotNan;

/// PropertyValue enum to represent possible types of property values
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Float(NotNan<f64>),
    Boolean(bool),
    // Extend with DateTime, UUID, etc. as needed
}

impl fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyValue::String(s) => write!(f, "{}", s),
            PropertyValue::Integer(i) => write!(f, "{}", i),
            PropertyValue::Float(fl) => write!(f, "{}", fl),
            PropertyValue::Boolean(b) => write!(f, "{}", b),
        }
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::String(s)
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::String(s.to_string())
    }
}

impl From<i64> for PropertyValue {
    fn from(i: i64) -> Self {
        PropertyValue::Integer(i)
    }
}

impl From<f64> for PropertyValue {
    fn from(f: f64) -> Self {
        PropertyValue::Float(NotNan::new(f).expect("NaN not allowed in PropertyValue::Float"))
    }
}

impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self {
        PropertyValue::Boolean(b)
    }
}

/// A map from property name (string) to property value
pub type Properties = BTreeMap<String, PropertyValue>;

/// Helper functions to work with properties

pub fn get_string<'a>(properties: &'a Properties, key: &'a str) -> Option<&'a str> {
    match properties.get(key) {
        Some(PropertyValue::String(s)) => Some(s),
        _ => None,
    }
}

pub fn get_integer(properties: &Properties, key: &str) -> Option<i64> {
    match properties.get(key) {
        Some(PropertyValue::Integer(i)) => Some(*i),
        _ => None,
    }
}

pub fn get_float(properties: &Properties, key: &str) -> Option<f64> {
    match properties.get(key) {
        Some(PropertyValue::Float(f)) => Some(f.into_inner()),
        _ => None,
    }
}

pub fn get_bool(properties: &Properties, key: &str) -> Option<bool> {
    match properties.get(key) {
        Some(PropertyValue::Boolean(b)) => Some(*b),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::NotNan;

    #[test]
    fn test_property_value_from() {
        let s: PropertyValue = "hello".into();
        assert_eq!(s, PropertyValue::String("hello".to_string()));

        let i: PropertyValue = 42i64.into();
        assert_eq!(i, PropertyValue::Integer(42));

        let f: PropertyValue = PropertyValue::Float(NotNan::new(3.14).unwrap());
        assert_eq!(f, PropertyValue::Float(NotNan::new(3.14).unwrap()));

        let b: PropertyValue = true.into();
        assert_eq!(b, PropertyValue::Boolean(true));
    }

    #[test]
    fn test_getters() {
        let mut props = Properties::new();
        props.insert("name".into(), "Alice".into());
        props.insert("age".into(), 30i64.into());
        props.insert("weight".into(), PropertyValue::Float(NotNan::new(65.5).unwrap()));
        props.insert("active".into(), true.into());

        assert_eq!(get_string(&props, "name"), Some("Alice"));
        assert_eq!(get_integer(&props, "age"), Some(30));
        assert_eq!(get_float(&props, "weight"), Some(65.5));
        assert_eq!(get_bool(&props, "active"), Some(true));

        assert_eq!(get_string(&props, "missing"), None);
        assert_eq!(get_integer(&props, "name"), None);
    }
}

