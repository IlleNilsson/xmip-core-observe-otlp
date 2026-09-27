//! The OTLP resource an export is from: the node, named as OpenTelemetry's
//! semantic conventions name a service.

/// `service.name`: every Xmip node is one service.
const SERVICE_NAME: &str = "service.name";
/// `service.instance.id`: which node.
const SERVICE_INSTANCE: &str = "service.instance.id";
/// The service every node is.
const SERVICE: &str = "xmip";

/// What an export says it is from, as the resource's attributes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resource {
    attributes: Vec<(String, String)>,
}

impl Resource {
    /// The node called `name`: `service.name` `xmip`, and
    /// `service.instance.id` the node's name.
    #[must_use]
    pub fn node(name: &str) -> Self {
        Self {
            attributes: vec![
                (SERVICE_NAME.to_string(), SERVICE.to_string()),
                (SERVICE_INSTANCE.to_string(), name.to_string()),
            ],
        }
    }

    /// With one more attribute: a cluster, a deployment environment, what
    /// an operator's collector routes by.
    #[must_use]
    pub fn with(mut self, key: &str, value: &str) -> Self {
        self.attributes.push((key.to_string(), value.to_string()));
        self
    }

    /// Every attribute, in the order given.
    pub fn attributes(&self) -> impl Iterator<Item = (&str, &str)> {
        self.attributes
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}
