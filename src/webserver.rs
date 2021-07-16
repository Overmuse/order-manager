use crate::db;
use crate::types::Owner;
use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use tokio_postgres::Client;
use uuid::Uuid;
use warp::reply::{json, Reply};
use warp::{any, body, get, path, put, reject, serve, Filter, Rejection};

type Db = Arc<Client>;

fn with_db(db: Db) -> impl Filter<Extract = (Db,), Error = Infallible> + Clone {
    any().map(move || db.clone())
}

#[tracing::instrument(skip(db))]
async fn get_allocations(db: Db) -> Result<impl Reply, Rejection> {
    let allocations = db::get_allocations(db).await.map_err(|_| reject())?;
    Ok(json(&allocations))
}

#[tracing::instrument(skip(db))]
async fn set_allocation_owner(id: Uuid, owner: Owner, db: Db) -> Result<impl Reply, Rejection> {
    let allocations = db::set_allocation_owner(db, id, owner)
        .await
        .map_err(|_| reject())?;
    Ok(json(&allocations))
}

#[tracing::instrument(skip(db))]
async fn get_lots(db: Db) -> Result<impl Reply, Rejection> {
    let lots = db::get_lots(db).await.map_err(|_| reject())?;
    Ok(json(&lots))
}

#[tracing::instrument(skip(db))]
async fn get_claims(db: Db) -> Result<impl Reply, Rejection> {
    let claims = db::get_claims(db).await.map_err(|_| reject())?;
    Ok(json(&claims))
}

#[tracing::instrument(skip(db))]
async fn get_pending_orders(db: Db) -> Result<impl Reply, Rejection> {
    let pending_orders = db::get_pending_orders(db).await.map_err(|_| reject())?;
    Ok(json(&pending_orders))
}

#[tracing::instrument(skip(db))]
pub async fn run(port: u16, db: Db) {
    let health = path!("health").map(|| "");
    let get_allocations = path("allocations")
        .and(get())
        .and(with_db(db.clone()))
        .and_then(get_allocations);
    let set_allocation_owner = path!("allocations" / Uuid)
        .and(put())
        .and(body::json())
        .and(with_db(db.clone()))
        .and_then(set_allocation_owner);
    let lots = path("lots")
        .and(get())
        .and(with_db(db.clone()))
        .and_then(get_lots);
    let claims = path("claims")
        .and(get())
        .and(with_db(db.clone()))
        .and_then(get_claims);
    let pending_orders = path("pending_orders")
        .and(get())
        .and(with_db(db.clone()))
        .and_then(get_pending_orders);
    let routes = get()
        .and(health)
        .or(get_allocations)
        .or(set_allocation_owner)
        .or(lots)
        .or(claims)
        .or(pending_orders);
    let address = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);
    serve(routes).run(address).await
}
