//! Native gateway contract for messaging adapters.
mod acknowledgements;
mod approvals;
pub mod commands;
mod effects;
mod input_aliases;
mod intake;
pub mod media;
mod observations;
mod policy;
pub mod prepared_threads;
mod producer_results;
mod profile;
mod protection;
mod provision;
pub mod quotes;
mod receiver;
mod repair;
pub mod routes;
pub mod runtime;
mod sessions;
mod stock;
pub mod storage;
mod store;
mod system_audio;
mod timed;
mod transport;
mod types;
mod wake;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support;

#[cfg(test)]
mod conformance;
#[cfg(test)]
mod profile_tests;

pub use acknowledgements::{AcknowledgementLease, AcknowledgementOperation, LeasedAcknowledgement};
pub(crate) use approvals::{ToolApprovalContext, tool_key};
pub use commands::*;
pub use input_aliases::InputAlias;
pub use intake::*;
pub use observations::ConnectionObservation;
pub use policy::*;
pub use profile::*;
pub use quotes::{QuoteHydration, QuoteLookup, QuoteReference, ScrollbackQuote, ScrollbackReference};
pub use receiver::ReceiverItem;
pub use repair::*;
pub use media::{ArtifactFetch, AttachmentManifest, AudioService, MediaPurpose, MediaRead, MediaSource};
pub(super) use runtime::Host;
pub use effects::*;
pub use runtime::Ingress;
pub use stock::{StockAdapter, StockConfiguration};
pub use timed::*;
pub use transport::*;
pub use types::*;
pub use wake::*;
