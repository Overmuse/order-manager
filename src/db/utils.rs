use position_intents::AmountSpec;
use rust_decimal::Decimal;

pub fn split_amount_spec(amount_spec: AmountSpec) -> (Decimal, &'static str) {
    match amount_spec {
        AmountSpec::Dollars(dollars) => (dollars, "dollars"),
        AmountSpec::Shares(shares) => (shares, "shares"),
        AmountSpec::Percent(percent) => (percent, "percent"),
        AmountSpec::Zero => (Decimal::ZERO, "zero"),
    }
}

pub fn unite_amount_spec(amount: Decimal, unit: &str) -> AmountSpec {
    match unit {
        "dollars" => AmountSpec::Dollars(amount),
        "shares" => AmountSpec::Shares(amount),
        "percent" => AmountSpec::Percent(amount),
        "zero" => AmountSpec::Zero,
        _ => unreachable!(),
    }
}
