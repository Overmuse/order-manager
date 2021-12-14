use alpaca::{Order, OrderStatus, Side};
use chrono::{DateTime, Utc};
use postgres_types::{FromSql, ToSql};
use serde::Serialize;
use std::convert::TryFrom;
use tokio_postgres::Row;
use uuid::Uuid;

#[derive(Copy, Clone, Debug, Serialize, PartialEq, Eq, ToSql, FromSql)]
#[serde(rename_all = "snake_case")]
#[postgres(name = "status")]
pub enum Status {
    #[postgres(name = "unreported")]
    Unreported,
    #[postgres(name = "accepted")]
    Accepted,
    #[postgres(name = "partially_filled")]
    PartiallyFilled,
    #[postgres(name = "filled")]
    Filled,
    #[postgres(name = "cancelled")]
    Cancelled,
    #[postgres(name = "dead")]
    Dead,
}

#[derive(Clone, Debug, Serialize)]
pub struct Trade {
    pub id: Uuid,
    pub broker_id: Option<Uuid>,
    pub ticker: String,
    pub quantity: i32,
    pub pending_quantity: i32,
    pub datetime: DateTime<Utc>,
    pub status: Status,
}

impl Trade {
    #[tracing::instrument(skip(id, ticker, quantity))]
    pub fn new(id: Uuid, ticker: String, quantity: i32) -> Self {
        tracing::trace!(%id, %ticker, %quantity, "New Trade");
        Self {
            id,
            broker_id: None,
            ticker,
            quantity,
            pending_quantity: quantity,
            datetime: Utc::now(),
            status: Status::Unreported,
        }
    }

    pub fn accepted(&mut self) {
        self.status = Status::Accepted;
    }

    pub fn partially_filled(&mut self) {
        self.status = Status::PartiallyFilled;
    }

    pub fn filled(&mut self) {
        self.status = Status::Filled;
    }

    pub fn cancelled(&mut self) {
        self.status = Status::Cancelled;
    }

    pub fn dead(&mut self) {
        self.status = Status::Dead;
    }

    pub fn set_broker_id(&mut self, broker_id: Uuid) {
        self.broker_id = Some(broker_id);
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            Status::Unreported | Status::Accepted | Status::PartiallyFilled,
        )
    }
}

impl From<Order> for Trade {
    fn from(order: Order) -> Trade {
        let (quantity, pending_quantity) = match order.side {
            Side::Buy => (order.qty as i32, (order.qty - order.filled_qty) as i32),
            Side::Sell => (-(order.qty as i32), -((order.qty - order.filled_qty) as i32)),
        };
        let status = match order.status {
            OrderStatus::Canceled => Status::Cancelled,
            OrderStatus::Filled => Status::Filled,
            OrderStatus::PartiallyFilled => Status::PartiallyFilled,
            OrderStatus::Expired | OrderStatus::Replaced | OrderStatus::Rejected | OrderStatus::Suspended => {
                Status::Dead
            }
            _ => Status::Accepted,
        };

        Trade {
            id: Uuid::parse_str(&order.client_order_id).expect("Failed to convert id to UUID"),
            broker_id: Some(order.id),
            ticker: order.symbol,
            quantity,
            pending_quantity,
            datetime: order.created_at,
            status,
        }
    }
}

impl TryFrom<Row> for Trade {
    type Error = tokio_postgres::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            broker_id: row.try_get("broker_id")?,
            ticker: row.try_get("ticker")?,
            quantity: row.try_get("quantity")?,
            pending_quantity: row.try_get("pending_quantity")?,
            datetime: row.try_get("datetime")?,
            status: row.try_get("status")?,
        })
    }
}
