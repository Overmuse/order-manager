use super::{Claim, Lot, Owner};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use tokio_postgres::Row;
use tracing::trace;
use trading_base::Amount;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Allocation {
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
    pub fn new(
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

impl TryFrom<Row> for Allocation {
    type Error = tokio_postgres::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let owner = row.try_get("owner")?;
        let owner = if owner == "House" {
            Owner::House
        } else {
            Owner::Strategy(owner, row.try_get("sub_owner")?)
        };
        Ok(Allocation {
            id: row.try_get("id")?,
            owner,
            claim_id: row.try_get("claim_id")?,
            lot_id: row.try_get("lot_id")?,
            ticker: row.try_get("ticker")?,
            shares: row.try_get("shares")?,
            basis: row.try_get("basis")?,
        })
    }
}

fn should_allocate(lot: &Lot, claim: &Claim) -> bool {
    if claim.amount.is_zero() {
        return false;
    }
    if claim.ticker != lot.ticker {
        return false;
    }
    if claim.amount.is_sign_positive() && lot.shares.is_sign_negative() {
        return false;
    }
    if claim.amount.is_sign_negative() && lot.shares.is_sign_positive() {
        return false;
    }

    if let Some(limit_price) = claim.limit_price {
        if (lot.shares.is_sign_positive() && lot.price > limit_price)
            || (lot.shares.is_sign_negative() && lot.price < limit_price)
        {
            return false;
        }
    }
    true
}

#[tracing::instrument(skip(claims, lot))]
pub fn split_lot(claims: &[Claim], lot: &Lot) -> Vec<Allocation> {
    let mut remaining_shares = lot.shares;
    let mut remaining_basis = lot.shares * lot.price;
    let mut out = Vec::new();
    for claim in claims {
        if !should_allocate(lot, claim) {
            continue;
        }

        let (basis, shares) = match claim.amount {
            Amount::Dollars(dollars) => {
                let mut allocated_dollars = dollars.abs().min(remaining_basis.abs());
                if dollars.is_sign_negative() {
                    allocated_dollars.set_sign_negative(true)
                }
                (allocated_dollars, (allocated_dollars / lot.price).round_dp(8))
            }
            Amount::Shares(shares) => {
                let mut allocated_shares = shares.abs().min(remaining_shares.abs());
                if shares.is_sign_negative() {
                    allocated_shares.set_sign_negative(true)
                }
                (allocated_shares * lot.price, allocated_shares)
            }
            Amount::Zero => (Decimal::ZERO, Decimal::ZERO),
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

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_should_allocate() {
        let lot = Lot::new(
            Uuid::new_v4(),
            "AAPL".into(),
            Utc::now(),
            Decimal::new(100, 0),
            Decimal::new(10, 0),
        );

        let zero_claim1 = Claim::new(
            "A".into(),
            None,
            "AAPL".into(),
            Amount::Shares(Decimal::ZERO),
            None,
            None,
        );
        let zero_claim2 = Claim::new(
            "A".into(),
            None,
            "AAPL".into(),
            Amount::Dollars(Decimal::ZERO),
            None,
            None,
        );
        let zero_claim3 = Claim::new("A".into(), None, "AAPL".into(), Amount::Zero, None, None);

        assert!(!should_allocate(&lot, &zero_claim1));
        assert!(!should_allocate(&lot, &zero_claim2));
        assert!(!should_allocate(&lot, &zero_claim3));

        let limit_claim = Claim::new(
            "A".into(),
            None,
            "AAPL".into(),
            Amount::Shares(Decimal::ONE),
            Some(Decimal::TEN),
            None,
        );
        assert!(!should_allocate(&lot, &limit_claim));

        let wrong_sign_claim = Claim::new(
            "A".into(),
            None,
            "AAPL".into(),
            Amount::Shares(-Decimal::ONE),
            None,
            None,
        );
        assert!(!should_allocate(&lot, &wrong_sign_claim));

        let wrong_ticker_claim = Claim::new("A".into(), None, "AAP".into(), Amount::Shares(Decimal::ONE), None, None);
        assert!(!should_allocate(&lot, &wrong_ticker_claim));

        let okay_claim = Claim::new(
            "A".into(),
            None,
            "AAPL".into(),
            Amount::Shares(Decimal::ONE),
            None,
            None,
        );
        assert!(should_allocate(&lot, &okay_claim));
    }

    #[test]
    fn test_split_lot_with_remainder() {
        let lot = Lot::new(
            Uuid::new_v4(),
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
                Amount::Dollars(Decimal::new(-400, 0)),
                None,
                None,
            ),
            Claim::new(
                "B".into(),
                None,
                "AAPL".into(),
                Amount::Dollars(Decimal::new(400, 0)),
                None,
                None,
            ),
            Claim::new(
                "C".into(),
                Some("B2".into()),
                "AAPL".into(),
                Amount::Shares(Decimal::new(25, 1)),
                None,
                None,
            ),
        ];
        let allocations = split_lot(&claims, &lot);
        assert_eq!(allocations.len(), 3);
        assert_eq!(
            allocations[0],
            Allocation {
                id: allocations[0].id,
                owner: Owner::Strategy("B".into(), None),
                claim_id: Some(claims[1].id),
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
                owner: Owner::Strategy("C".into(), Some("B2".into())),
                claim_id: Some(claims[2].id),
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
