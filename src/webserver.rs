use crate::db;
use std::convert::Infallible;
use std::sync::Arc;
use tokio_postgres::Client;
use warp::Filter;

type Db = Arc<Client>;

fn with_db(db: Db) -> impl Filter<Extract = (Db,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}

#[tracing::instrument(skip(db))]
async fn get_allocations(db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    let allocations = db::get_allocations(db).await.map_err(|_| warp::reject())?;
    Ok(warp::reply::json(&allocations))
}

#[tracing::instrument(skip(db))]
async fn get_lots(db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    let lots = db::get_lots(db).await.map_err(|_| warp::reject())?;
    Ok(warp::reply::json(&lots))
}

#[tracing::instrument(skip(db))]
async fn get_claims(db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    let claims = db::get_claims(db).await.map_err(|_| warp::reject())?;
    Ok(warp::reply::json(&claims))
}

#[tracing::instrument(skip(db))]
async fn get_pending_orders(db: Db) -> Result<impl warp::Reply, warp::Rejection> {
    let pending_orders = db::get_pending_orders(db)
        .await
        .map_err(|_| warp::reject())?;
    Ok(warp::reply::json(&pending_orders))
}

#[tracing::instrument(skip(db))]
pub async fn run(db: Db) {
    let health = warp::path!("health").map(|| "");
    let allocations = warp::path("allocations")
        .and(warp::get())
        .and(with_db(db.clone()))
        .and_then(get_allocations);
    let lots = warp::path("lots")
        .and(warp::get())
        .and(with_db(db.clone()))
        .and_then(get_lots);
    let claims = warp::path("claims")
        .and(warp::get())
        .and(with_db(db.clone()))
        .and_then(get_claims);
    let pending_orders = warp::path("pending_orders")
        .and(warp::get())
        .and(with_db(db.clone()))
        .and_then(get_pending_orders);
    let routes = warp::get()
        .and(health)
        .or(allocations)
        .or(lots)
        .or(claims)
        .or(pending_orders);
    warp::serve(routes).run(([0, 0, 0, 0], 8000)).await
}
