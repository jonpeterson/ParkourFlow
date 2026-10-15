//! Typed configuration fields ("properties") that node types declare.
//!
//! A [`NodeDescriptor`](crate::NodeDescriptor) lists its [`Property`]
//! definitions. The UI renders an input for each one from its
//! [`PropertyType`], and the workflow stores each node's
//! [`PropertyValue`]s as configuration.

use serde::{Deserialize, Serialize};

use crate::error::WorkflowError;

/// Whether a file property picks an existing file or a place to write one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileMode {
    Open,
    Save,
}

/// What kind of value a property holds. Decides the UI widget and how values
/// are validated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PropertyType {
    Bool,
    Integer {
        min: Option<i64>,
        max: Option<i64>,
    },
    Float {
        min: Option<f64>,
        max: Option<f64>,
        step: Option<f64>,
    },
    String {
        /// Show a multi-line editor, e.g. for a filter expression.
        #[serde(default)]
        multiline: bool,
    },
    /// One of a fixed list of strings.
    Enum {
        options: Vec<String>,
    },
    /// A file path, chosen with the OS file picker.
    File {
        mode: FileMode,
        /// Allowed extensions without the dot, e.g. `csv`. Empty allows any.
        #[serde(default)]
        extensions: Vec<String>,
    },
    /// A directory path, chosen with the OS folder picker.
    Directory,
    /// A device id. The available devices change at runtime, so the options
    /// are listed on demand for the device `class` (e.g. `network_interface`).
    Device {
        class: String,
    },
}

/// When a changed value takes effect on a running node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Apply {
    /// The node picks up the new value while running.
    Live,
    /// The node must restart to use the new value.
    #[default]
    Restart,
}

/// A configuration value. File, directory, device, and enum values are
/// strings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PropertyValue {
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl From<bool> for PropertyValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<i64> for PropertyValue {
    fn from(v: i64) -> Self {
        Self::Integer(v)
    }
}

impl From<f64> for PropertyValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl From<&str> for PropertyValue {
    fn from(v: &str) -> Self {
        Self::String(v.into())
    }
}

impl From<String> for PropertyValue {
    fn from(v: String) -> Self {
        Self::String(v)
    }
}

/// A configuration field declared by a node type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Property {
    /// Key used in the node's values, e.g. `path`.
    pub name: String,
    /// Label shown in the UI, e.g. `File`.
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub ty: PropertyType,
    pub default: Option<PropertyValue>,
    /// The node cannot start until this property has a value.
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub apply: Apply,
}

impl Property {
    pub fn new(name: impl Into<String>, label: impl Into<String>, ty: PropertyType) -> Self {
        Self {
            name: name.into(),
            label: label.into(),
            description: None,
            ty,
            default: None,
            required: false,
            apply: Apply::Restart,
        }
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn default(mut self, value: impl Into<PropertyValue>) -> Self {
        self.default = Some(value.into());
        self
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn live(mut self) -> Self {
        self.apply = Apply::Live;
        self
    }

    /// Checks `value` against this property's type and returns it in
    /// canonical form (an integer given for a float property becomes a
    /// float).
    pub fn validate(&self, value: PropertyValue) -> Result<PropertyValue, WorkflowError> {
        let invalid = |reason: String| WorkflowError::InvalidPropertyValue {
            property: self.name.clone(),
            reason,
        };
        let expected = |what: &str| invalid(format!("expected {what}"));

        match (&self.ty, value) {
            (PropertyType::Bool, v @ PropertyValue::Bool(_)) => Ok(v),
            (PropertyType::Bool, _) => Err(expected("true or false")),

            (PropertyType::Integer { min, max }, PropertyValue::Integer(n)) => {
                if min.is_some_and(|m| n < m) || max.is_some_and(|m| n > m) {
                    return Err(invalid(range_message(*min, *max)));
                }
                Ok(PropertyValue::Integer(n))
            }
            (PropertyType::Integer { .. }, _) => Err(expected("a whole number")),

            (PropertyType::Float { min, max, .. }, v) => {
                let n = match v {
                    PropertyValue::Float(n) => n,
                    PropertyValue::Integer(n) => n as f64,
                    _ => return Err(expected("a number")),
                };
                if !n.is_finite() {
                    return Err(expected("a finite number"));
                }
                if min.is_some_and(|m| n < m) || max.is_some_and(|m| n > m) {
                    return Err(invalid(range_message(*min, *max)));
                }
                Ok(PropertyValue::Float(n))
            }

            (PropertyType::Enum { options }, PropertyValue::String(s)) => {
                if !options.contains(&s) {
                    return Err(invalid(format!("must be one of: {}", options.join(", "))));
                }
                Ok(PropertyValue::String(s))
            }
            (PropertyType::Enum { .. }, _) => Err(expected("one of the listed options")),

            (PropertyType::File { extensions, .. }, PropertyValue::String(s)) => {
                if !s.is_empty() && !extensions.is_empty() && !has_extension(&s, extensions) {
                    return Err(invalid(format!(
                        "file must end in .{}",
                        extensions.join(", .")
                    )));
                }
                Ok(PropertyValue::String(s))
            }

            (
                PropertyType::String { .. } | PropertyType::Directory | PropertyType::Device { .. },
                v @ PropertyValue::String(_),
            ) => Ok(v),
            (_, _) => Err(expected("text")),
        }
    }
}

fn range_message<T: std::fmt::Display>(min: Option<T>, max: Option<T>) -> String {
    match (min, max) {
        (Some(min), Some(max)) => format!("must be between {min} and {max}"),
        (Some(min), None) => format!("must be at least {min}"),
        (None, Some(max)) => format!("must be at most {max}"),
        (None, None) => unreachable!("range check without bounds"),
    }
}

fn has_extension(path: &str, extensions: &[String]) -> bool {
    std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| extensions.iter().any(|x| x.eq_ignore_ascii_case(e)))
}

/// A choice for a [`PropertyType::Device`] property.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceOption {
    /// The value stored in the property.
    pub id: String,
    /// Human-readable name.
    pub label: String,
}

/// Lists the devices currently available for a device class.
///
/// No device backends exist yet, so every class returns an empty list. Node
/// modules (libpcap, FFmpeg, audio) will provide real enumeration.
pub fn device_options(_class: &str) -> Vec<DeviceOption> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(min: Option<i64>, max: Option<i64>) -> Property {
        Property::new("n", "N", PropertyType::Integer { min, max })
    }

    #[test]
    fn integer_range() {
        let p = int(Some(1), Some(10));
        assert_eq!(p.validate(5.into()), Ok(PropertyValue::Integer(5)));
        assert!(p.validate(0.into()).is_err());
        assert!(p.validate(11.into()).is_err());
        assert!(p.validate(5.5.into()).is_err());
        assert!(p.validate("5".into()).is_err());
    }

    #[test]
    fn float_accepts_integers() {
        let p = Property::new(
            "gain",
            "Gain",
            PropertyType::Float {
                min: Some(0.0),
                max: Some(2.0),
                step: None,
            },
        );
        assert_eq!(p.validate(1.into()), Ok(PropertyValue::Float(1.0)));
        assert_eq!(p.validate(0.5.into()), Ok(PropertyValue::Float(0.5)));
        assert!(p.validate(3.into()).is_err());
        assert!(p.validate(f64::NAN.into()).is_err());
    }

    #[test]
    fn enum_options() {
        let p = Property::new(
            "codec",
            "Codec",
            PropertyType::Enum {
                options: vec!["h264".into(), "vp9".into()],
            },
        );
        assert!(p.validate("vp9".into()).is_ok());
        assert!(p.validate("av1".into()).is_err());
    }

    #[test]
    fn file_extensions() {
        let p = Property::new(
            "path",
            "File",
            PropertyType::File {
                mode: FileMode::Open,
                extensions: vec!["csv".into()],
            },
        );
        assert!(p.validate("/data/in.csv".into()).is_ok());
        assert!(p.validate("/data/IN.CSV".into()).is_ok());
        assert!(p.validate("".into()).is_ok());
        assert!(p.validate("/data/in.json".into()).is_err());
        assert!(p.validate(true.into()).is_err());
    }

    #[test]
    fn serializes_for_the_ui() {
        let p = Property::new(
            "path",
            "File",
            PropertyType::File {
                mode: FileMode::Save,
                extensions: vec!["csv".into()],
            },
        )
        .required();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["type"]["kind"], "file");
        assert_eq!(json["type"]["mode"], "save");
        assert_eq!(json["required"], true);
        assert_eq!(json["apply"], "restart");
        assert_eq!(json["default"], serde_json::Value::Null);

        let v: PropertyValue = serde_json::from_str("3").unwrap();
        assert_eq!(v, PropertyValue::Integer(3));
        let v: PropertyValue = serde_json::from_str("3.5").unwrap();
        assert_eq!(v, PropertyValue::Float(3.5));
    }
}
