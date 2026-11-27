//! The gateway executable and native clients bundle the stock messaging adapters.

pub use hermes_gateway_core::{
    GatewayConfig, METHODS, RunningGateway, client, messaging, replay, stock_plugins,
};

pub struct Gateway;

const STOCK_MESSENGERS: &[messaging::StockAdapter] = &[
    hermes_discord_plugin::STOCK_ADAPTER,
    hermes_telegram_plugin::STOCK_ADAPTER,
    hermes_slack_plugin::STOCK_ADAPTER,
];

impl Gateway {
    pub async fn start(mut config: GatewayConfig) -> anyhow::Result<RunningGateway> {
        config.stock_adapters.extend_from_slice(STOCK_MESSENGERS);
        hermes_gateway_core::Gateway::start(config).await
    }
}

pub mod owner {
    pub use hermes_gateway_core::owner::{
        OwnerConnection, discover, dispatch_role, ensure_configured, ensure_local, profile_id,
    };

    pub async fn serve(args: impl IntoIterator<Item = String>) -> anyhow::Result<()> {
        hermes_gateway_core::owner::serve(args, super::STOCK_MESSENGERS.to_vec()).await
    }
}
