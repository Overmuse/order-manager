use alpaca::orders::OrderIntent;
use alpaca::AlpacaMessage;
use anyhow::{Context, Result};
use multimap::MultiMap;
use position_intents::PositionIntent;
use rdkafka::consumer::StreamConsumer;
//use sqlx::postgres::PgPool;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{error, info, trace};

mod allocation;
//mod db;
mod dependent_orders;
mod input;
mod intents;
mod lot;
mod order_updates;
use allocation::{split_lot, Allocation, Claim, Owner, Position};
use input::Input;
use lot::Lot;

pub struct OrderManager {
    kafka_consumer: StreamConsumer,
    scheduler_sender: UnboundedSender<PositionIntent>,
    scheduler_receiver: UnboundedReceiver<PositionIntent>,
    order_sender: UnboundedSender<OrderIntent>,
    partially_filled_orders: MultiMap<String, Lot>,
    dependent_orders: MultiMap<String, OrderIntent>,
    unfilled_claims: MultiMap<String, Claim>,
    allocations: Vec<Allocation>, //pool: PgPool,
}

impl OrderManager {
    pub fn new(
        kafka_consumer: StreamConsumer,
        scheduler_sender: UnboundedSender<PositionIntent>,
        scheduler_receiver: UnboundedReceiver<PositionIntent>,
        order_sender: UnboundedSender<OrderIntent>,
        //pool: PgPool,
    ) -> Self {
        Self {
            kafka_consumer,
            scheduler_sender,
            scheduler_receiver,
            order_sender,
            partially_filled_orders: MultiMap::new(),
            dependent_orders: MultiMap::new(),
            unfilled_claims: MultiMap::new(),
            allocations: Vec::new(), //pool,
        }
    }

    pub async fn run(mut self) {
        info!("Starting OrderManager");
        loop {
            let message = self.receive_message().await;
            trace!("Message received");
            if let Err(e) = self.handle_input(message).await {
                //panic!("{:?}", e)
                error!("{:?}", e)
            }
        }
    }

    async fn handle_input(&mut self, input: Result<Input>) -> Result<()> {
        match input {
            Ok(Input::PositionIntent(intent)) => self
                .handle_position_intent(intent)
                .context("Failed to handle PositionIntent")?,
            Ok(Input::AlpacaMessage(AlpacaMessage::TradeUpdates(oe))) => self
                .handle_order_update(oe)
                .await
                .context("Failed to handle OrderEvent")?,
            Ok(Input::AlpacaMessage(_)) => unreachable!(),
            Err(e) => Err(e)?,
        };
        Ok(())
    }
}
