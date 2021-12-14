use order_manager::{run, Settings};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig,
};
use tracing::{debug, subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use uuid::Uuid;

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
                NewTopic::new("allocations", 1, TopicReplication::Fixed(1)),
                NewTopic::new("claims", 1, TopicReplication::Fixed(1)),
                NewTopic::new("lots", 1, TopicReplication::Fixed(1)),
                NewTopic::new("overmuse-trades", 1, TopicReplication::Fixed(1)),
                NewTopic::new("position-intents", 1, TopicReplication::Fixed(1)),
                NewTopic::new("risk-check-request", 1, TopicReplication::Fixed(1)),
                NewTopic::new("risk-check-response", 1, TopicReplication::Fixed(1)),
                NewTopic::new("trade-intents", 1, TopicReplication::Fixed(1)),
                NewTopic::new("time", 1, TopicReplication::Fixed(1)),
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

    debug!("Subscribing to topics");
    consumer
        .subscribe(&[
            &"allocations",
            &"claims",
            &"lots",
            &"risk-check-request",
            &"trade-intents",
        ])
        .unwrap();
    consumer
        .subscription()
        .unwrap()
        .set_all_offsets(rdkafka::topic_partition_list::Offset::End)
        .unwrap();

    let database_address = "postgres://postgres:password@localhost:5432";
    let database_name = "order-manager";
    tokio::spawn(async move {
        std::env::set_var("APP__UNREPORTED_TRADE_EXPIRY_SECONDS", "1");
        std::env::set_var("DATABASE__NAME", database_name);
        std::env::set_var("DATABASE__URL", database_address);
        std::env::set_var("DATASTORE__BASE_URL", "http://localhost:9010");
        std::env::set_var("KAFKA__BOOTSTRAP_SERVER", "localhost:9094");
        std::env::set_var("KAFKA__GROUP_ID", Uuid::new_v4().to_string());
        std::env::set_var(
            "KAFKA__INPUT_TOPICS",
            "overmuse-trades,position-intents,risk-check-response,time",
        );
        std::env::set_var("KAFKA__BOOTSTRAP_SERVERS", "localhost:9094");
        std::env::set_var("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT");
        std::env::set_var("KAFKA__ACKS", "0");
        std::env::set_var("KAFKA__RETRIES", "0");
        std::env::set_var("KAFKA__RETRIES", "0");
        std::env::set_var("SENTRY__DSN", "");
        std::env::set_var("SENTRY__ENVIRONMENT", "test");
        std::env::set_var("WEBSERVER__PORT", "8127");
        let settings = Settings::new();
        tracing::debug!("{:?}", settings);
        let res = run(settings.unwrap()).await;
        tracing::error!("{:?}", res);
    });

    (admin, admin_options, consumer, producer)
}
