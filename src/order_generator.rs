use crate::PositionIntent;
use alpaca::{
    common::{OrderType, Side},
    orders::OrderIntent,
};
use uuid::Uuid;

pub fn make_orders(
    position: PositionIntent,
    owned_qty: i32,
) -> (Option<OrderIntent>, Option<OrderIntent>) {
    //TODO: deal with pending qty
    if position.qty == owned_qty {
        // No trading needed
        (None, None)
    } else {
        let signum_product = owned_qty.signum() * position.qty.signum();
        if signum_product >= 0 {
            // No restrictions on trading, just send the diff in qty
            let qty = position.qty - owned_qty;
            let trade = order_intent(&position.id, &position.ticker, qty, position.limit_price);
            (Some(trade), None)
        } else {
            // Quantities have different signs
            let sent = order_intent(
                &position.id,
                &position.ticker,
                -owned_qty,
                position.limit_price,
            );
            let saved = order_intent(
                &position.id,
                &position.ticker,
                position.qty,
                position.limit_price,
            );
            (Some(sent), Some(saved))
        }
    }
}

fn order_intent(prefix: &str, ticker: &str, qty: i32, limit_price: Option<f64>) -> OrderIntent {
    let side = if qty > 0 { Side::Buy } else { Side::Sell };
    let order_type = match limit_price {
        Some(limit) => OrderType::Limit { limit_price: limit },
        None => OrderType::Market,
    };
    OrderIntent::new(ticker)
        .client_order_id(format!("{}_{}", prefix, Uuid::new_v4().to_string()))
        .qty(qty.abs() as usize)
        .order_type(order_type)
        .side(side)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::prelude::*;

    #[test]
    fn no_current_position() {
        let position = PositionIntent {
            id: "A".into(),
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
            limit_price: None,
        };
        let (sent, saved) = make_orders(position, 0);
        let sent = sent.unwrap();
        assert_eq!(sent.qty, 10);
        assert_eq!(sent.side, Side::Buy);
        assert!(saved.is_none())
    }

    #[test]
    fn accumulation() {
        let position = PositionIntent {
            id: "A".into(),
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
            limit_price: None,
        };
        let (sent, saved) = make_orders(position, 5);
        let sent = sent.unwrap();
        assert_eq!(sent.qty, 5);
        assert_eq!(sent.side, Side::Buy);
        assert!(saved.is_none())
    }

    #[test]
    fn decumulation() {
        let position = PositionIntent {
            id: "A".into(),
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
            limit_price: None,
        };
        let (sent, saved) = make_orders(position, 15);
        let sent = sent.unwrap();
        assert_eq!(sent.qty, 5);
        assert_eq!(sent.side, Side::Sell);
        assert!(saved.is_none())
    }

    #[test]
    fn no_change() {
        let position = PositionIntent {
            id: "A".into(),
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 10,
            limit_price: None,
        };
        let (sent, saved) = make_orders(position, 10);
        assert!(sent.is_none());
        assert!(saved.is_none());
    }

    #[test]
    fn long_to_short() {
        let position = PositionIntent {
            id: "A".into(),
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: -15,
            limit_price: None,
        };
        let (sent, saved) = make_orders(position, 10);
        let sent = sent.unwrap();
        let saved = saved.unwrap();
        assert_eq!(sent.qty, 10);
        assert_eq!(sent.side, Side::Sell);
        assert_eq!(saved.qty, 15);
        assert_eq!(saved.side, Side::Sell);
    }

    #[test]
    fn short_to_long() {
        let position = PositionIntent {
            id: "A".into(),
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 15,
            limit_price: None,
        };
        let (sent, saved) = make_orders(position, -10);
        let sent = sent.unwrap();
        let saved = saved.unwrap();
        assert_eq!(sent.qty, 10);
        assert_eq!(sent.side, Side::Buy);
        assert_eq!(saved.qty, 15);
        assert_eq!(saved.side, Side::Buy);
    }

    #[test]
    fn limit_order() {
        let position = PositionIntent {
            id: "A".into(),
            strategy: "A".into(),
            timestamp: Utc::now(),
            ticker: "AAPL".into(),
            qty: 15,
            limit_price: Some(100.0),
        };
        let (sent, _) = make_orders(position, 0);
        let sent = sent.unwrap();
        assert_eq!(sent.qty, 15);
        assert_eq!(sent.side, Side::Buy);
        assert_eq!(sent.order_type, OrderType::Limit { limit_price: 100.0 })
    }
}
