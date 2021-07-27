use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::StreamConsumer,
    producer::FutureProducer,
    ClientConfig,
};
use tracing::{debug, subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub async fn setup() -> (
    AdminClient<DefaultClientContext>,
    AdminOptions,
    StreamConsumer,
    FutureProducer,
) {
    // Drop database, don't care if it fails
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .create()
        .unwrap();
    let admin_options = AdminOptions::new();
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    set_global_default(subscriber).unwrap();
    debug!("Creating topics");
    admin
        .create_topics(
            &[
                NewTopic::new("overmuse-trades", 1, TopicReplication::Fixed(1)),
                NewTopic::new("position-intents", 1, TopicReplication::Fixed(1)),
                NewTopic::new("trade-intents", 1, TopicReplication::Fixed(1)),
            ],
            &admin_options,
        )
        .await
        .unwrap();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .create()
        .unwrap();
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9094")
        .set("security.protocol", "PLAINTEXT")
        .set("group.id", "test-consumer")
        .create()
        .unwrap();

    debug!("Creating database");

    (admin, admin_options, consumer, producer)
}
