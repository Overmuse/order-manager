use crate::db::{
    dependent_order::{
        delete_dependent_order_by_client_order_id, get_dependent_order_by_client_order_id,
        save_dependent_order,
    },
    order::{pending_orders_by_ticker, upsert_order},
    order_intent::{delete_order_intent_by_id, pending_order_intents_by_ticker, save_order_intent},
    position::{get_position_by_ticker, upsert_position},
};
use crate::{order_generator::make_orders, DependentOrder, Input, PositionIntent};
use alpaca::{
    common::Side,
    orders::OrderIntent,
    stream::{AlpacaMessage, Event, OrderEvent},
};
use anyhow::{Context, Result};
use futures::StreamExt;
use mongodb::Database;
use tracing::{debug, trace};

pub struct OrderManager {
    db: Database,
}

impl OrderManager {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    #[tracing::instrument(skip(self, msg))]
    pub async fn handle_message(&self, msg: Input) -> Result<Option<OrderIntent>> {
        match msg {
            Input::PositionIntent(position_intent) => {
                debug!("Received PositionIntent: {:?}", position_intent);
                self.handle_position_intent(position_intent)
                    .await
                    .context("Failed to handle position intent")
            }
            Input::AlpacaMessage(AlpacaMessage::TradeUpdates(order_event)) => {
                debug!("Received OrderEvent: {:?}", order_event);
                self.register_order(order_event)
                    .await
                    .context("Failed to register order")
            }
            Input::AlpacaMessage(_) => unreachable!(),
        }
    }

    async fn handle_position_intent(
        &self,
        position_intent: PositionIntent,
    ) -> Result<Option<OrderIntent>> {
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
        trace!(
            "Pending order qty: {}\nPending order intent qty: {}\nCurrent qty: {}",
            pending_order_qty,
            pending_order_intent_qty,
            current_qty
        );
        // TODO: Deal with pending quantities
        match make_orders(position_intent, current_qty) {
            (None, None) => Ok(None),
            (Some(sent), None) => {
                save_order_intent(&self.db, sent.clone()).await?;
                Ok(Some(sent))
            }
            (Some(sent), Some(saved)) => {
                save_order_intent(&self.db, sent.clone()).await?;
                let dependent_order = DependentOrder {
                    client_order_id: sent
                        .client_order_id
                        .clone()
                        .expect("Every order-intent we send has a client_order_id"),
                    order: saved,
                };
                save_dependent_order(&self.db, dependent_order).await?;
                Ok(Some(sent))
            }
            (None, Some(_)) => unreachable!(),
        }
    }

    async fn register_order(&self, msg: OrderEvent) -> Result<Option<OrderIntent>> {
        trace!("Beginning order registration");
        let client_order_id = &msg.order.client_order_id;
        upsert_order(&self.db, msg.order.clone()).await?;
        delete_order_intent_by_id(&self.db, client_order_id).await?;
        let res = match msg.event {
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
                let dependent_order =
                    get_dependent_order_by_client_order_id(&self.db, client_order_id).await?;
                if dependent_order.is_some() {
                    delete_dependent_order_by_client_order_id(&self.db, client_order_id).await?;
                    let order_intent = dependent_order.expect("Guaranteed to exist").order;
                    save_order_intent(&self.db, order_intent.clone()).await?;
                    Ok(Some(order_intent))
                } else {
                    Ok(None)
                }
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
                Ok(None)
            }
            _ => Ok(None),
        };
        trace!("Finishing order registration");
        res
    }
}
