use super::utils::{split_amount_spec, unite_amount_spec};
use anyhow::Result;
use tokio_postgres::GenericClient;
use tracing::trace;
use trading_base::{Identifier, PositionIntent};
use uuid::Uuid;

#[tracing::instrument(skip(client))]
pub async fn get_scheduled_indents<T: GenericClient>(client: &T) -> Result<Vec<PositionIntent>> {
    trace!("Getting scheduled intents");
    client
        .query("SELECT * FROM scheduled_intents", &[])
        .await?
        .into_iter()
        .map(|row| {
            let identifier = match row.try_get("ticker")? {
                "all_" => Identifier::All,
                s => Identifier::Ticker(s.to_string()),
            };
            Ok(PositionIntent {
                id: row.try_get("id")?,
                strategy: row.try_get("strategy")?,
                sub_strategy: row.try_get("sub_strategy")?,
                timestamp: row.try_get("time_stamp")?,
                identifier,
                amount: unite_amount_spec(row.try_get("amount")?, row.try_get("unit")?),
                update_policy: serde_plain::from_str(row.try_get("update_policy")?)?,
                decision_price: row.try_get("decision_price")?,
                limit_price: row.try_get("limit_price")?,
                stop_price: row.try_get("stop_price")?,
                before: row.try_get("before")?,
                after: row.try_get("after")?,
            })
        })
        .collect()
}

#[tracing::instrument(skip(client, scheduled_intent))]
pub async fn save_scheduled_intent<T: GenericClient>(
    client: &T,
    scheduled_intent: &PositionIntent,
) -> Result<()> {
    trace!("Saving scheduled intent");
    let (amount, unit) = split_amount_spec(&scheduled_intent.amount);
    let ticker = match &scheduled_intent.identifier {
        Identifier::All => "all_",
        Identifier::Ticker(ticker) => ticker,
    };
    client
        .execute(
            "INSERT INTO scheduled_intents (
                id,
                strategy,
                sub_strategy,
                time_stamp,
                ticker,
                amount,
                unit,
                update_policy,
                decision_price,
                limit_price,
                stop_price,
                before,
                after
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            &[
                &scheduled_intent.id,
                &scheduled_intent.strategy,
                &scheduled_intent.sub_strategy,
                &scheduled_intent.timestamp,
                &ticker,
                &amount,
                &unit,
                &serde_plain::to_string(&scheduled_intent.update_policy)?,
                &scheduled_intent.decision_price,
                &scheduled_intent.limit_price,
                &scheduled_intent.stop_price,
                &scheduled_intent.before,
                &scheduled_intent.after,
            ],
        )
        .await?;
    Ok(())
}

#[tracing::instrument(skip(client, id))]
pub async fn delete_scheduled_intent<T: GenericClient>(client: &T, id: Uuid) -> Result<()> {
    trace!(%id, "Deleting scheduled intent");
    client
        .execute("DELETE FROM scheduled_intents WHERE id = $1", &[&id])
        .await?;
    Ok(())
}
