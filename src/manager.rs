//use crate::db::get_ticker_position_by_strategy;
//use crate::db;
use crate::policy::Policy;
use crate::PositionIntent;
use alpaca::common::Side;
use alpaca::orders::OrderIntent;
use alpaca::stream::{Event, OrderEvent};
use sqlx::postgres::PgPool;

pub struct OrderManager {
    pub policy: Policy,
    _pool: Option<PgPool>,
}

impl OrderManager {
    pub fn new() -> Self {
        Self {
            policy: Policy::default(),
            _pool: None,
        }
    }

    pub fn register_order(&mut self, msg: OrderEvent) {
        match msg.event {
            Event::Canceled { timestamp } => todo!(),
            Event::Expired { timestamp } => todo!(),
            Event::Fill {
                timestamp,
                price,
                qty,
            } => todo!(),
            //Event::New => db::post_order_fill(&self.pool, msg.order),
            Event::OrderCancelRejected => todo!(),
            Event::OrderReplaceRejected => todo!(),
            Event::PartialFill {
                timestamp,
                price,
                qty,
            } => todo!(),
            Event::Rejected { timestamp } => todo!(),
            Event::Replaced { timestamp } => todo!(),
            _ => todo!(),
        }
    }

    pub fn order_with_policy(&self, _position: PositionIntent) -> OrderIntent {
        todo!()
    }

    pub fn make_orders(
        &self,
        position: PositionIntent,
        current_position: Option<PositionIntent>,
    ) -> Result<Vec<OrderIntent>, Box<dyn std::error::Error + Send>> {
        match current_position {
            Some(current_position) => {
                let qty_diff = position.qty - current_position.qty;
                if current_position.qty.signum() != position.qty.signum() {
                    // In this case, we are going from a net long (short) position to a net short
                    // (long) position. Alpaca doesn't allow such an order, so we need to split our
                    // order into two – one that bring us to net zero and one that establishes the
                    // desired position
                    let side = match position.qty {
                        x if x < 0 => Side::Sell,
                        x if x > 0 => Side::Buy,
                        _ => {
                            // Want a net zero position
                            if current_position.qty > 0 {
                                Side::Sell
                            } else {
                                Side::Buy
                            }
                        }
                    };
                    let first_order = OrderIntent::new(&position.ticker)
                        .qty(current_position.qty.abs() as usize)
                        .side(side.clone());
                    let second_order = OrderIntent::new(&position.ticker)
                        .qty(position.qty.abs() as usize)
                        .side(side);
                    Ok(vec![first_order, second_order])
                } else {
                    if qty_diff == 0 {
                        Ok(Vec::new())
                    } else if qty_diff > 0 {
                        let oi = OrderIntent::new(&position.ticker)
                            .qty(qty_diff.abs() as usize)
                            .side(Side::Buy);
                        Ok(vec![oi])
                    } else {
                        let oi = OrderIntent::new(&position.ticker)
                            .qty(qty_diff.abs() as usize)
                            .side(Side::Sell);
                        Ok(vec![oi])
                    }
                }
            }
            None => {
                if position.qty == 0 {
                    Ok(Vec::new())
                } else if position.qty > 0 {
                    let oi = OrderIntent::new(&position.ticker)
                        .qty(position.qty.abs() as usize)
                        .side(Side::Buy);
                    Ok(vec![oi])
                } else {
                    let oi = OrderIntent::new(&position.ticker)
                        .qty(position.qty.abs() as usize)
                        .side(Side::Sell);
                    Ok(vec![oi])
                }
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
        let oi = om.make_orders(position, None).unwrap();
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Buy);
    }

    #[test]
    fn accumulation() {
        let om = OrderManager::new();
        let current_position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 5,
        };
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = om.make_orders(position, Some(current_position)).unwrap();
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 5);
        assert_eq!(oi[0].side, Side::Buy);
    }

    #[test]
    fn decumulation() {
        let om = OrderManager::new();
        let current_position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 15,
        };
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = om.make_orders(position, Some(current_position)).unwrap();
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 5);
        assert_eq!(oi[0].side, Side::Sell);
    }

    #[test]
    fn no_change() {
        let om = OrderManager::new();
        let current_position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = om.make_orders(position, Some(current_position)).unwrap();
        assert_eq!(oi.len(), 0);
    }

    #[test]
    fn long_to_short() {
        let om = OrderManager::new();
        let current_position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: -15,
        };
        let oi = om.make_orders(position, Some(current_position)).unwrap();
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
        let current_position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: -10,
        };
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 15,
        };
        let oi = om.make_orders(position, Some(current_position)).unwrap();
        println!("{:?}", oi);
        assert_eq!(oi.len(), 2);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Buy);
        assert_eq!(oi[1].qty, 15);
        assert_eq!(oi[1].side, Side::Buy);
    }
}
