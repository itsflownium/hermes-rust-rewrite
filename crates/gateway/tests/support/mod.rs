use hermes_gateway::owner::{self, OwnerConnection};
use std::{path::Path, time::Duration};
use tokio::time::{error::Elapsed, sleep, timeout};

pub async fn wait_for_owner(root: &Path, startup_timeout: Duration) -> OwnerConnection {
    let mut last_timeout = None;
    let owner = timeout(startup_timeout, async {
        loop {
            match owner::discover(root).await {
                Ok(Some(owner)) => return owner,
                Ok(None) => {}
                Err(error) if error.is::<Elapsed>() => last_timeout = Some(error),
                Err(error) => panic!("gateway discovery failed: {error:#}"),
            }
            sleep(Duration::from_millis(40)).await;
        }
    })
    .await;

    match owner {
        Ok(owner) => owner,
        Err(_) => panic!(
            "gateway did not publish a profile endpoint (last probe timeout: {last_timeout:?})"
        ),
    }
}
