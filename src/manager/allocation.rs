use super::Lot;
use position_intents::AmountSpec;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(super) enum Owner {
    House,
    Strategy(String, Option<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Claim {
    pub id: Uuid,
    pub strategy: String,
    pub sub_strategy: Option<String>,
    pub amount: AmountSpec,
}

impl Claim {
    pub(super) fn new(strategy: String, sub_strategy: Option<String>, amount: AmountSpec) -> Self {
        Self {
            id: Uuid::new_v4(),
            strategy,
            sub_strategy,
            amount,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(super) struct Allocation {
    pub owner: Owner,
    pub claim_id: Option<Uuid>,
    pub lot_id: Uuid,
    pub ticker: String,
    pub shares: Decimal,
    pub basis: Decimal,
}

impl Allocation {
    pub(super) fn new(
        owner: Owner,
        claim_id: Option<Uuid>,
        lot_id: Uuid,
        ticker: String,
        shares: Decimal,
        basis: Decimal,
    ) -> Self {
        Self {
            owner,
            claim_id,
            lot_id,
            ticker,
            shares,
            basis,
        }
    }
}

#[tracing::instrument]
pub(super) fn split_lot(claims: &[Claim], lot: &Lot) -> Vec<Allocation> {
    let mut remaining_shares = lot.shares;
    let mut remaining_basis = lot.shares * lot.price;
    let mut out = Vec::new();
    for claim in claims {
        let (basis, shares) = match claim.amount {
            AmountSpec::Dollars(dollars) => {
                let mut allocated_dollars = dollars.abs().min(remaining_basis.abs());
                if dollars.is_sign_negative() {
                    allocated_dollars.set_sign_negative(true)
                }
                (allocated_dollars, allocated_dollars / lot.price)
            }
            AmountSpec::Shares(shares) => {
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
pub(super) struct Position {
    pub owner: Owner,
    pub ticker: String,
    pub shares: Decimal,
    pub basis: Decimal,
}

impl Position {
    pub(super) fn new(owner: Owner, ticker: String, shares: Decimal, basis: Decimal) -> Self {
        Self {
            owner,
            ticker,
            shares,
            basis,
        }
    }

    pub(super) fn from_allocations(allocations: &[Allocation]) -> Self {
        let ticker = allocations[0].ticker.clone();
        let owner = allocations[0].owner.clone();
        let (shares, basis) =
            allocations
                .iter()
                .fold((Decimal::ZERO, Decimal::ZERO), |acc, allocation| {
                    if allocation.ticker != ticker {
                        panic!("Cannot build position out of allocations from different tickers")
                    }
                    if allocation.owner != owner {
                        panic!("Cannout build position out of allocations of different owners")
                    }
                    (acc.0 + allocation.shares, acc.1 + allocation.basis)
                });
        Self::new(owner, ticker, shares, basis)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_split_lot_with_remainder() {
        let lot = Lot::new(
            "AAPL".into(),
            Utc::now(),
            Decimal::new(100, 0),
            Decimal::new(10, 0),
        );
        let claims = vec![
            Claim::new("A".into(), None, AmountSpec::Dollars(Decimal::new(400, 0))),
            Claim::new(
                "B".into(),
                Some("B2".into()),
                AmountSpec::Shares(Decimal::new(25, 1)),
            ),
        ];
        let allocations = split_lot(&claims, &lot);
        assert_eq!(allocations.len(), 3);
        assert_eq!(
            allocations[0],
            Allocation::new(
                Owner::Strategy("A".into(), None),
                Some(claims[0].id),
                lot.id,
                "AAPL".into(),
                Decimal::new(4, 0),
                Decimal::new(400, 0)
            )
        );
        assert_eq!(
            allocations[1],
            Allocation::new(
                Owner::Strategy("B".into(), Some("B2".into())),
                Some(claims[1].id),
                lot.id,
                "AAPL".into(),
                Decimal::new(25, 1),
                Decimal::new(250, 0)
            )
        );
        assert_eq!(
            allocations[2],
            Allocation::new(
                Owner::House,
                None,
                lot.id,
                "AAPL".into(),
                Decimal::new(35, 1),
                Decimal::new(350, 0)
            )
        );
    }
}
