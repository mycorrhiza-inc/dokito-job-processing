use openscraper_types::raw::RawGenericDocket;
use openscraper_types::processed::ProcessedGenericDocket;
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
