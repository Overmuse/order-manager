use crate::PositionIntent;
use alpaca::{common::Side, orders::OrderIntent};
use uuid::Uuid;

pub fn make_orders(position: PositionIntent, owned_qty: i32, pending_qty: i32) -> Vec<OrderIntent> {
    let desired_change = position.qty - owned_qty - pending_qty;
    let max_size = if owned_qty == 0 {
        None
    } else {
        Some(-owned_qty)
    };
    make_order_vec(&position.ticker, desired_change, max_size)
}

fn make_order(ticker: &str, qty: i32) -> OrderIntent {
    let side = if qty > 0 { Side::Buy } else { Side::Sell };
    OrderIntent::new(ticker)
        .client_order_id(Uuid::new_v4().to_string())
        .qty(qty.abs() as usize)
        .side(side)
}

fn make_order_vec(
    ticker: &str,
    total_change: i32,
    max_order_size: Option<i32>,
) -> Vec<OrderIntent> {
    if total_change == 0 {
        return Vec::new();
    }
    if let Some(max_order_size) = max_order_size {
        let num_orders = (total_change / max_order_size).max(0) as usize;
        let mut order_vec: Vec<_> = (0..num_orders)
            .map(|_| make_order(ticker, max_order_size))
            .collect();
        order_vec.push(make_order(
            ticker,
            total_change - max_order_size * num_orders as i32,
        ));
        order_vec
    } else {
        vec![make_order(ticker, total_change)]
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::prelude::*;

    #[test]
    fn no_current_position() {
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = make_orders(position, 0, 0);
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Buy);
    }

    #[test]
    fn accumulation() {
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = make_orders(position, 5, 0);
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 5);
        assert_eq!(oi[0].side, Side::Buy);
    }

    #[test]
    fn decumulation() {
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = make_orders(position, 15, 0);
        assert_eq!(oi.len(), 1);
        assert_eq!(oi[0].qty, 5);
        assert_eq!(oi[0].side, Side::Sell);
    }

    #[test]
    fn no_change() {
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
        };
        let oi = make_orders(position, 10, 0);
        assert_eq!(oi.len(), 0);
    }

    #[test]
    fn long_to_short() {
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: -15,
        };
        let oi = make_orders(position, 10, 0);
        println!("{:?}", oi);
        assert_eq!(oi.len(), 3);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Sell);
        assert_eq!(oi[1].qty, 10);
        assert_eq!(oi[1].side, Side::Sell);
        assert_eq!(oi[2].qty, 5);
        assert_eq!(oi[2].side, Side::Sell);
    }

    #[test]
    fn short_to_long() {
        let position = PositionIntent {
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 15,
        };
        let oi = make_orders(position, -10, 0);
        println!("{:?}", oi);
        assert_eq!(oi.len(), 3);
        assert_eq!(oi[0].qty, 10);
        assert_eq!(oi[0].side, Side::Buy);
        assert_eq!(oi[1].qty, 10);
        assert_eq!(oi[1].side, Side::Buy);
        assert_eq!(oi[2].qty, 5);
        assert_eq!(oi[2].side, Side::Buy);
    }
}
