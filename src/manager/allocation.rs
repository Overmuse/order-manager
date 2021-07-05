use super::Lot;
use position_intents::AmountSpec;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Error, Formatter};
use tracing::trace;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) enum Owner {
    House,
    Strategy(String, Option<String>),
}

impl Display for Owner {
    fn fmt(&self, formatter: &mut Formatter) -> std::result::Result<(), Error> {
        match self {
            Owner::House => formatter.write_str("House"),
            Owner::Strategy(strategy, sub_strategy) => {
                if let Some(sub_strategy) = sub_strategy {
                    formatter.write_str(strategy)?;
                    formatter.write_str(":")?;
                    formatter.write_str(sub_strategy)
                } else {
                    formatter.write_str(strategy)
                }
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Claim {
    pub id: Uuid,
    pub strategy: String,
    pub sub_strategy: Option<String>,
    pub ticker: String,
    pub amount: AmountSpec,
}

impl Claim {
    #[tracing::instrument(skip(strategy, sub_strategy, ticker, amount))]
    pub(super) fn new(
        strategy: String,
        sub_strategy: Option<String>,
        ticker: String,
        amount: AmountSpec,
    ) -> Self {
        trace!(%strategy, ?sub_strategy, %ticker, ?amount, "New Claim");
        Self {
            id: Uuid::new_v4(),
            strategy,
            sub_strategy,
            ticker,
            amount,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Allocation {
    pub id: Uuid,
    pub owner: Owner,
    pub claim_id: Option<Uuid>,
    pub lot_id: Uuid,
    pub ticker: String,
    pub shares: Decimal,
    pub basis: Decimal,
}

impl Allocation {
    #[tracing::instrument(skip(owner, claim_id, lot_id, ticker, shares, basis))]
    pub(crate) fn new(
        owner: Owner,
        claim_id: Option<Uuid>,
        lot_id: Uuid,
        ticker: String,
        shares: Decimal,
        basis: Decimal,
    ) -> Self {
        trace!(%owner, ?claim_id, %lot_id, %ticker, %shares, %basis, "New Allocation");
        Self {
            id: Uuid::new_v4(),
            owner,
            claim_id,
            lot_id,
            ticker,
            shares,
            basis,
        }
    }
}

#[tracing::instrument(skip(claims, lot))]
pub(super) fn split_lot(claims: &[Claim], lot: &Lot) -> Vec<Allocation> {
    let mut remaining_shares = lot.shares;
    let mut remaining_basis = lot.shares * lot.price;
    let mut out = Vec::new();
    for claim in claims {
        let (basis, shares) = match claim.amount {
            AmountSpec::Dollars(dollars) => {
                if dollars.is_zero() {
                    continue;
                }
                let mut allocated_dollars = dollars.abs().min(remaining_basis.abs());
                if dollars.is_sign_negative() {
                    allocated_dollars.set_sign_negative(true)
                }
                (allocated_dollars, allocated_dollars / lot.price)
            }
            AmountSpec::Shares(shares) => {
                if shares.is_zero() {
                    continue;
                }
                let mut allocated_shares = shares.abs().min(remaining_shares.abs());
                if shares.is_sign_negative() {
                    allocated_shares.set_sign_negative(true)
                }
                (allocated_shares * lot.price, allocated_shares)
            }
            AmountSpec::Zero => (Decimal::ZERO, Decimal::ZERO),
            _ => unimplemented!(),
        };
        out.push(Allocation::new(
            Owner::Strategy(claim.strategy.clone(), claim.sub_strategy.clone()),
            Some(claim.id),
            lot.id,
            lot.ticker.clone(),
            shares,
            basis,
        ));
        remaining_shares -= shares;
        remaining_basis -= basis;
    }
    if remaining_shares.ne(&Decimal::ZERO) {
        out.push(Allocation::new(
            Owner::House,
            None,
            lot.id,
            lot.ticker.clone(),
            remaining_shares,
            remaining_basis,
        ));
    }

    out
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Position {
    pub owner: Owner,
    pub ticker: String,
    pub shares: Decimal,
    pub basis: Decimal,
}

impl Position {
    #[tracing::instrument(skip(owner, ticker, shares, basis))]
    pub fn new(owner: Owner, ticker: String, shares: Decimal, basis: Decimal) -> Self {
        trace!(%owner, %ticker, %shares, %basis, "New Position");
        Self {
            owner,
            ticker,
            shares,
            basis,
        }
    }

    pub fn is_long(&self) -> bool {
        self.shares > Decimal::ZERO
    }

    pub fn is_short(&self) -> bool {
        self.shares < Decimal::ZERO
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_split_lot_with_remainder() {
        let lot = Lot::new(
            "A".into(),
            "AAPL".into(),
            Utc::now(),
            Decimal::new(100, 0),
            Decimal::new(10, 0),
        );
        let claims = vec![
            Claim::new(
                "A".into(),
                None,
                "AAPL".into(),
                AmountSpec::Dollars(Decimal::new(400, 0)),
            ),
            Claim::new(
                "B".into(),
                Some("B2".into()),
                "AAPL".into(),
                AmountSpec::Shares(Decimal::new(25, 1)),
            ),
        ];
        let allocations = split_lot(&claims, &lot);
        assert_eq!(allocations.len(), 3);
        assert_eq!(
            allocations[0],
            Allocation {
                id: allocations[0].id,
                owner: Owner::Strategy("A".into(), None),
                claim_id: Some(claims[0].id),
                lot_id: lot.id,
                ticker: "AAPL".into(),
                shares: Decimal::new(4, 0),
                basis: Decimal::new(400, 0)
            }
        );
        assert_eq!(
            allocations[1],
            Allocation {
                id: allocations[1].id,
                owner: Owner::Strategy("B".into(), Some("B2".into())),
                claim_id: Some(claims[1].id),
                lot_id: lot.id,
                ticker: "AAPL".into(),
                shares: Decimal::new(25, 1),
                basis: Decimal::new(250, 0)
            }
        );
        assert_eq!(
            allocations[2],
            Allocation {
                id: allocations[2].id,
                owner: Owner::House,
                claim_id: None,
                lot_id: lot.id,
                ticker: "AAPL".into(),
                shares: Decimal::new(35, 1),
                basis: Decimal::new(350, 0)
            }
        );
    }
}
