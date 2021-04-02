use mongodb::Client;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use tracing::debug;

pub async fn teardown(
    mongo_client: &Client,
    admin: &AdminClient<DefaultClientContext>,
    admin_options: &AdminOptions,
) {
    debug!("Deleting topics");
    admin
        .delete_topics(
            &["position-intents", "order-intents", "overmuse-trades"],
            &admin_options,
        )
        .await
        .unwrap();
    debug!("Deleting mongo database");
    mongo_client.database("testdb");
}
