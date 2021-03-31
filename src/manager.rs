use crate::db::{
    delete_order_intent_by_id, get_position_by_ticker, pending_order_intents_by_ticker,
    pending_orders_by_ticker, save_order_intents, upsert_order, upsert_position,
};
use crate::policy::Policy;
use crate::PositionIntent;
use alpaca::common::{Order, Side};
use alpaca::orders::OrderIntent;
use alpaca::stream::{AlpacaMessage, Event, OrderEvent};
use anyhow::{Context, Error, Result};
use bson::doc;
use futures::StreamExt;
use mongodb::{Collection, Database};
use serde::Deserialize;
use std::borrow::Cow;
use stream_processor::StreamProcessor;
use tracing::{trace, Instrument};
use uuid::Uuid;

#[derive(Default)]
pub struct OrderManager {
    pub policy: Policy,
    db: Option<Database>,
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
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn bind(mut self, db: Database) -> Self {
        self.db = Some(db);
        self
    }

    fn db(&self) -> Result<&Database> {
        self.db
            .as_ref()
            .ok_or("Missing db")
            .map_err(Error::msg)
            .context("Failed to lookup db")
    }

    async fn handle_position_intent(
        &self,
        position_intent: PositionIntent,
    ) -> Result<Option<Vec<OrderIntent>>> {
        let db = self.db()?;
        let pending_order_qty = pending_orders_by_ticker(db, &position_intent.ticker)
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
        let pending_order_intent_qty = pending_order_intents_by_ticker(db, &position_intent.ticker)
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
        let current_qty = get_position_by_ticker(db, &position_intent.ticker)
            .await?
            .map(|position| position.qty)
            .unwrap_or(0);
        tracing::debug!(
            "Pending order qty: {}\nPending order intent qty: {}\nCurrent qty: {}",
            pending_order_qty,
            pending_order_intent_qty,
            current_qty
        );
        let order_intents = self
            .make_orders(
                position_intent,
                current_qty + pending_order_intent_qty + pending_order_qty,
            )
            .context("Failed to create orders")?;
        save_order_intents(db, order_intents.clone()).await?;

        Ok(Some(order_intents))
    }

    async fn register_order(&self, msg: OrderEvent) -> Result<()> {
        let db = self.db()?;
        upsert_order(db, msg.order.clone()).await?;
        delete_order_intent_by_id(
            db,
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
                upsert_position(db, position).await?;
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
                upsert_position(db, position).await?;
            }
            _ => (),
        }
        Ok(())
    }

    pub fn order_with_policy(&self, _position: PositionIntent) -> OrderIntent {
        todo!()
    }

    pub fn make_orders(
        &self,
        position: PositionIntent,
        current_qty: i32,
    ) -> Result<Vec<OrderIntent>> {
        match (current_qty, position.qty) {
            (c, n) if c == n => Ok(Vec::new()),
            (c, n) if c == 0 && n > 0 => {
                let oi = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(position.qty.abs() as usize)
                    .side(Side::Buy);
                Ok(vec![oi])
            }
            (c, n) if c == 0 && n < 0 => {
                let oi = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(position.qty.abs() as usize)
                    .side(Side::Sell);
                Ok(vec![oi])
            }
            (c, n) if c < 0 && n > 0 => {
                let first_order = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(c.abs() as usize)
                    .side(Side::Buy);
                let second_order = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(n as usize)
                    .side(Side::Buy);
                Ok(vec![first_order, second_order])
            }
            (c, n) if c > 0 && n < 0 => {
                let first_order = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(c as usize)
                    .side(Side::Sell);
                let second_order = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(n.abs() as usize)
                    .side(Side::Sell);
                Ok(vec![first_order, second_order])
            }
            (c, n) if n > c => {
                let oi = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty((n - c).abs() as usize)
                    .side(Side::Buy);
                Ok(vec![oi])
            }
            (c, n) if n < c => {
                let oi = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty((c - n).abs() as usize)
                    .side(Side::Sell);
                Ok(vec![oi])
            }
            (_, _) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::prelude::*;

    #[test]
    fn no_current_position() {
        let om = OrderManager::new();
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = om.make_orders(position, 0).unwrap();
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Buy);
    }

    #[test]
    fn accumulation() {
        let om = OrderManager::new();
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = om.make_orders(position, 5).unwrap();
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 5);
        assert_eq!(oi[0].side, Side::Buy);
    }

    #[test]
    fn decumulation() {
        let om = OrderManager::new();
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = om.make_orders(position, 15).unwrap();
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 5);
        assert_eq!(oi[0].side, Side::Sell);
    }

    #[test]
    fn no_change() {
        let om = OrderManager::new();
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = om.make_orders(position, 10).unwrap();
        assert_eq!(oi.len(), 0);
    }

    #[test]
    fn long_to_short() {
        let om = OrderManager::new();
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: -15,
        };
        let oi = om.make_orders(position, 10).unwrap();
        println!("{:?}", oi);
        assert_eq!(oi.len(), 2);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Sell);
        assert_eq!(oi[1].qty, 15);
        assert_eq!(oi[1].side, Side::Sell);
    }

    #[test]
    fn short_to_long() {
        let om = OrderManager::new();
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 15,
        };
        let oi = om.make_orders(position, -10).unwrap();
        println!("{:?}", oi);
        assert_eq!(oi.len(), 2);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Buy);
        assert_eq!(oi[1].qty, 15);
        assert_eq!(oi[1].side, Side::Buy);
    }
}
