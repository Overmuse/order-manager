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
        let position_collection: Collection<PositionIntent> =
            db.collection_with_type("position-intents");
        let orders_collection: Collection<OrderIntent> = db.collection_with_type("order-intents");
        let current_position: Option<PositionIntent> = position_collection
            .find_one(
                doc! {
                    "strategy": &position_intent.strategy,
                    "ticker": &position_intent.ticker
                },
                None,
            )
            .await
            .context("Failed to lookup current position")?;
        let pending_orders = orders_collection
            .find(
                doc! {
                    "symbol": &position_intent.ticker
                },
                None,
            )
            .await
            .context("Failed to lookup pending orders")?;
        let pending_qty = pending_orders
            .map(|order| {
                let order = order.unwrap();
                match order.side {
                    Side::Buy => (order.qty as i32),
                    Side::Sell => -(order.qty as i32),
                }
            })
            .fold(0, |acc, x| async move { acc + x })
            .await;
        let current_qty = match current_position {
            Some(position_intent) => position_intent.qty,
            None => 0,
        };
        let orders = self
            .make_orders(position_intent, current_qty + pending_qty)
            .context("Failed to create orders")?;
        orders_collection
            .insert_many(orders.clone(), None)
            .await
            .context("Failed to insert orders into database")?;

        Ok(Some(orders))
    }

    async fn register_order(&self, msg: OrderEvent) -> Result<()> {
        let db = self.db()?;
        let order_collection: Collection<Order> = db.collection_with_type("orders");
        order_collection
            .find_one_and_update(
                doc! {
                    "client_order_id": msg.order.client_order_id.clone().expect("Every order should have a client_order_id")
                },
                doc! {
                    "$set": {
                        "updated_at": bson::to_bson(&msg.order.updated_at).context("Failed to serialize updated_at")?,
                        "submitted_at": bson::to_bson(&msg.order.submitted_at).context("Failed to serialize submitted_at")?,
                        "filled_at": bson::to_bson(&msg.order.filled_at).context("Failed to serialize filled_at")?,
                        "expired_at": bson::to_bson(&msg.order.expired_at).context("Failed to serialize expired_at")?,
                        "canceled_at": bson::to_bson(&msg.order.canceled_at).context("Failed to serialize canceled_at")?,
                        "failed_at": bson::to_bson(&msg.order.failed_at).context("Failed to serialize failed_at")?,
                        "replaced_at": bson::to_bson(&msg.order.replaced_at).context("Failed to serialize replaced_at")?,
                        "replaced_by": bson::to_bson(&msg.order.replaced_by).context("Failed to serialize replaced_by")?,
                        "replaces": bson::to_bson(&msg.order.replaces).context("Failed to serialize replaces")?,
                        "filled_qty": bson::to_bson(&(msg.order.filled_qty as i32)).context("Failed to serialize filled_qty")?,
                        "filled_avg_price": bson::to_bson(&msg.order.filled_avg_price).context("Failed to serialize filled_avg_price")?,
                        "status": bson::to_bson(&msg.order.status).context("Failed to serialize status")?,
                    }
                },
                mongodb::options::FindOneAndUpdateOptions::builder()
                    .upsert(true)
                    .build(),
            )
            .await
            .context("Failed to update order")?;
        Ok(())
        //let query = msg.order.save();
        //query.execute(self.pool()?).await.map(|_| ())?;
        //let res = match msg.event {
        //    Event::Canceled { .. } => (),
        //    Event::Expired { .. } => (),
        //    Event::Fill { .. } => {}
        //    Event::New => {}
        //    Event::OrderCancelRejected => (),
        //    Event::OrderReplaceRejected => (),
        //    Event::PartialFill { .. } => {}
        //    Event::Rejected { .. } => (),
        //    Event::Replaced { .. } => (),
        //    _ => (),
        //};
        //Ok(res)
    }

    pub fn order_with_policy(&self, _position: PositionIntent) -> OrderIntent {
        todo!()
    }

    pub fn make_orders(
        &self,
        position: PositionIntent,
        current_qty: i32,
    ) -> Result<Vec<OrderIntent>> {
        if current_qty != 0 {
            let qty_diff = position.qty - current_qty;
            if current_qty.signum() != position.qty.signum() {
                // In this case, we are going from a net long (short) position to a net short
                // (long) position. Alpaca doesn't allow such an order, so we need to split our
                // order into two – one that bring us to net zero and one that establishes the
                // desired position
                let side = match position.qty {
                    x if x < 0 => Side::Sell,
                    x if x > 0 => Side::Buy,
                    _ => {
                        // Want a net zero position
                        if current_qty > 0 {
                            Side::Sell
                        } else {
                            Side::Buy
                        }
                    }
                };
                let first_order = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(current_qty.abs() as usize)
                    .side(side.clone());
                let second_order = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(position.qty.abs() as usize)
                    .side(side);
                Ok(vec![first_order, second_order])
            } else {
                if qty_diff == 0 {
                    Ok(Vec::new())
                } else if qty_diff > 0 {
                    let oi = OrderIntent::new(&position.ticker)
                        .client_order_id(Uuid::new_v4().to_string())
                        .qty(qty_diff.abs() as usize)
                        .side(Side::Buy);
                    Ok(vec![oi])
                } else {
                    let oi = OrderIntent::new(&position.ticker)
                        .client_order_id(Uuid::new_v4().to_string())
                        .qty(qty_diff.abs() as usize)
                        .side(Side::Sell);
                    Ok(vec![oi])
                }
            }
        } else {
            if position.qty == 0 {
                Ok(Vec::new())
            } else if position.qty > 0 {
                let oi = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(position.qty.abs() as usize)
                    .side(Side::Buy);
                Ok(vec![oi])
            } else {
                let oi = OrderIntent::new(&position.ticker)
                    .client_order_id(Uuid::new_v4().to_string())
                    .qty(position.qty.abs() as usize)
                    .side(Side::Sell);
                Ok(vec![oi])
            }
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
