mod cloud_fixture;
mod linked_fixture;

use hermes_gateway_core::{
    RunningGateway,
    messaging::{
        EnrollmentCredential,
        runtime::now,
        test_support::{Model, dispatch},
    },
};
use serde_json::{Value, json};
use std::time::Duration;

fn model() -> impl std::future::Future<Output = Model> {
    async {
        Model::new()
            .await
            .with_stock_adapter(crate::CLOUD_ADAPTER)
            .with_stock_adapter(crate::LINKED_ADAPTER)
    }
}

async fn configure(gateway: &RunningGateway, platform: &str, name: &str, settings: Value) {
    dispatch(
        gateway,
        &format!("messaging.profile.{platform}.create"),
        &json!({"name":name,"settings":settings}),
        true,
    )
    .await
    .unwrap();
}

async fn enroll(gateway: &RunningGateway, name: &str, credential: String) -> anyhow::Result<()> {
    let lease = gateway
        .begin_adapter_enrollment(name, Duration::from_secs(15))
        .await?;
    gateway
        .complete_adapter_enrollment(name, &lease.id, EnrollmentCredential::new(credential)?)
        .await
}

async fn wait(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("WhatsApp fixture condition");
}

async fn state(gateway: &RunningGateway, predicate: impl Fn(&Value) -> bool) -> Value {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let snapshot = gateway.messaging_status().await.unwrap();
            if predicate(&snapshot) {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    match result {
        Ok(state) => state,
        Err(_) => panic!(
            "WhatsApp gateway status condition: {}",
            gateway.messaging_status().await.unwrap()
        ),
    }
}
