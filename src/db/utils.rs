use rust_decimal::Decimal;
use trading_base::Amount;

pub fn split_amount_spec(amount_spec: Amount) -> (Decimal, &'static str) {
    match amount_spec {
        Amount::Dollars(dollars) => (dollars, "dollars"),
        Amount::Shares(shares) => (shares, "shares"),
        Amount::Zero => (Decimal::ZERO, "zero"),
    }
}

pub fn unite_amount_spec(amount: Decimal, unit: &str) -> Amount {
    match unit {
        "dollars" => Amount::Dollars(amount),
        "shares" => Amount::Shares(amount),
        "zero" => Amount::Zero,
        _ => unreachable!(),
    }
}
