//! The gateway executable and native clients bundle the stock messaging adapters.

pub use hermes_gateway_core::{
    GatewayConfig, METHODS, RunningGateway, client, messaging, replay, stock_plugins,
};

pub struct Gateway;

impl Gateway {
    pub async fn start(mut config: GatewayConfig) -> anyhow::Result<RunningGateway> {
        config
            .stock_adapters
            .push(hermes_discord_plugin::STOCK_ADAPTER);
        hermes_gateway_core::Gateway::start(config).await
    }
}

pub mod owner {
    pub use hermes_gateway_core::owner::{
        OwnerConnection, discover, dispatch_role, ensure_configured, ensure_local, profile_id,
    };

    pub async fn serve(args: impl IntoIterator<Item = String>) -> anyhow::Result<()> {
        hermes_gateway_core::owner::serve(args, vec![hermes_discord_plugin::STOCK_ADAPTER]).await
    }
}
