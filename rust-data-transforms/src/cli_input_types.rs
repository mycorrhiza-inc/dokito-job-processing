use crate::types::processed::ProcessedGenericDocket;
use openscraper_types::raw::RawGenericDocket;
use serde::Deserialize;

#[derive(Clone, Deserialize, Debug)]
#[serde(untagged)]
pub enum CliRawDockets {
    Singular(RawGenericDocket),
    Multiple(Vec<RawGenericDocket>),
}

impl From<CliRawDockets> for Vec<RawGenericDocket> {
    fn from(value: CliRawDockets) -> Self {
        match value {
            CliRawDockets::Multiple(list) => list,
            CliRawDockets::Singular(single) => vec![single],
        }
    }
}
impl From<Vec<RawGenericDocket>> for CliRawDockets {
    fn from(mut value: Vec<RawGenericDocket>) -> Self {
        if value.len() == 1
            && let Some(single) = value.pop()
        {
            return Self::Singular(single);
        }
        Self::Multiple(value)
    }
}

#[derive(Clone, Deserialize, Debug)]
#[serde(untagged)]
pub enum CliProcessedDockets {
    Singular(ProcessedGenericDocket),
    Multiple(Vec<ProcessedGenericDocket>),
}

impl From<CliProcessedDockets> for Vec<ProcessedGenericDocket> {
    fn from(value: CliProcessedDockets) -> Self {
        match value {
            CliProcessedDockets::Multiple(list) => list,
            CliProcessedDockets::Singular(single) => vec![single],
        }
    }
}

impl From<Vec<ProcessedGenericDocket>> for CliProcessedDockets {
    fn from(mut value: Vec<ProcessedGenericDocket>) -> Self {
        if value.len() == 1
            && let Some(single) = value.pop()
        {
            return Self::Singular(single);
        }
        Self::Multiple(value)
    }
}
