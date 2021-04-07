use crate::{
    db::{
        order::{pending_orders_by_ticker, upsert_order},
        order_intent::{
            delete_order_intent_by_id, pending_order_intents_by_ticker, save_order_intents,
        },
        position::{get_position_by_ticker, upsert_position},
    },
    order_generator::make_orders,
    PositionIntent,
};
use alpaca::{
    common::Side,
    orders::OrderIntent,
    stream::{AlpacaMessage, Event, OrderEvent},
};
use anyhow::{Context, Error, Result};
use bson::doc;
use futures::StreamExt;
use mongodb::Database;
use serde::Deserialize;
use std::borrow::Cow;
use stream_processor::StreamProcessor;
use tracing::{debug, trace};

pub struct OrderManager {
    db: Database,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Input {
    PositionIntent(PositionIntent),
    AlpacaMessage(AlpacaMessage),
}

#[async_trait::async_trait]
impl StreamProcessor for OrderManager {
    type Input = Input;
    type Output = OrderIntent;
    type Error = Error;

    #[tracing::instrument(skip(self, msg))]
    async fn handle_message(&self, msg: Self::Input) -> Result<Option<Vec<Self::Output>>> {
        match msg {
            Input::PositionIntent(position_intent) => {
                trace!("Received PositionIntent: {:?}", position_intent);
                self.handle_position_intent(position_intent).await
            }
            Input::AlpacaMessage(AlpacaMessage::TradeUpdates(order_event)) => {
                trace!("Received OrderEvent: {:?}", order_event);
                self.register_order(*order_event)
                    .await
                    .context("Failed to register order")?;
                Ok(None)
            }
            Input::AlpacaMessage(_) => unreachable!(),
        }
    }

    fn assign_topic(&self, _output: &Self::Output) -> Cow<str> {
        Cow::Borrowed("order-intents")
    }

    fn assign_key(&self, output: &Self::Output) -> Cow<str> {
        Cow::Owned(output.symbol.clone())
    }
}

impl OrderManager {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    async fn handle_position_intent(
        &self,
        position_intent: PositionIntent,
    ) -> Result<Option<Vec<OrderIntent>>> {
        let pending_order_qty = pending_orders_by_ticker(&self.db, &position_intent.ticker)
            .await?
            .map(|order| {
                // TODO: Account for partially filled
                let order = order.unwrap();
                match order.side {
                    Side::Buy => (order.qty as i32),
                    Side::Sell => -(order.qty as i32),
                }
            })
            .fold(0, |acc, x| async move { acc + x })
            .await;
        let pending_order_intent_qty =
            pending_order_intents_by_ticker(&self.db, &position_intent.ticker)
                .await?
                .map(|order| {
                    let order = order.unwrap();
                    match order.side {
                        Side::Buy => (order.qty as i32),
                        Side::Sell => -(order.qty as i32),
                    }
                })
                .fold(0, |acc, x| async move { acc + x })
                .await;
        let current_qty = get_position_by_ticker(&self.db, &position_intent.ticker)
            .await?
            .map(|position| position.qty)
            .unwrap_or(0);
        debug!(
            "Pending order qty: {}\nPending order intent qty: {}\nCurrent qty: {}",
            pending_order_qty, pending_order_intent_qty, current_qty
        );
        let order_intents = make_orders(
            position_intent,
            current_qty,
            pending_order_intent_qty + pending_order_qty,
        );
        save_order_intents(&self.db, order_intents.clone()).await?;

        Ok(Some(order_intents))
    }

    async fn register_order(&self, msg: OrderEvent) -> Result<()> {
        upsert_order(&self.db, msg.order.clone()).await?;
        delete_order_intent_by_id(
            &self.db,
            msg.order
                .client_order_id
                .as_ref()
                .expect("Every order should have client_order_id"),
        )
        .await?;
        match msg.event {
            Event::Fill {
                price,
                position_qty,
                ..
            } => {
                let position = crate::Position {
                    ticker: msg.order.symbol,
                    qty: position_qty as i32,
                    avg_entry_price: price,
                };
                upsert_position(&self.db, position).await?;
            }
            Event::PartialFill {
                price,
                position_qty,
                ..
            } => {
                let position = crate::Position {
                    ticker: msg.order.symbol,
                    qty: position_qty as i32,
                    avg_entry_price: price,
                };
                upsert_position(&self.db, position).await?;
            }
            _ => (),
        }
        Ok(())
    }
}
