//! Stock WhatsApp adapters on the native profile gateway.
mod cloud;
mod common;
mod intake;
mod linked;
mod outbound;

pub use cloud::CLOUD_ADAPTER;
pub use linked::LINKED_ADAPTER;

#[cfg(test)]
mod tests;
