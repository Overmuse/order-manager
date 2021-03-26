use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::StreamConsumer,
    producer::FutureProducer,
    ClientConfig,
};
use tracing::{debug, subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

pub async fn setup(
    client_config: &ClientConfig,
) -> (
    AdminClient<DefaultClientContext>,
    AdminOptions,
    StreamConsumer,
    FutureProducer,
) {
    let admin: AdminClient<DefaultClientContext> = client_config.create().unwrap();
    let admin_options = AdminOptions::new();
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();
    set_global_default(subscriber).unwrap();
    debug!("Creating topics");
    admin
        .create_topics(
            &[
                NewTopic::new("position-intents", 1, TopicReplication::Fixed(1)),
                NewTopic::new("order-intents", 1, TopicReplication::Fixed(1)),
                NewTopic::new("overmuse-trades", 1, TopicReplication::Fixed(1)),
            ],
            &admin_options,
        )
        .await
        .unwrap();

    let producer: FutureProducer = client_config.create().unwrap();
    let consumer: StreamConsumer = client_config
        .clone()
        .set("group.id", "test-consumer")
        .create()
        .unwrap();

    (admin, admin_options, consumer, producer)
}
