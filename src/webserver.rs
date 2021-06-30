use warp::Filter;

pub struct WebServer;

impl WebServer {
    pub fn new() -> Self {
        Self
    }

    pub async fn run(self) {
        let health = warp::path!("health").map(|| "");
        let allocations = warp::get()
            .and(warp::path("allocations"))
            .map(|| warp::reply::json(&()));
        let routes = warp::get().and(health).or(allocations);
        warp::serve(routes).run(([0, 0, 0, 0], 8000)).await
    }
}
