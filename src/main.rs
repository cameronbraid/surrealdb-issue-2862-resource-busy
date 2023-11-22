use surrealdb::{engine::any::Any, Surreal};
use surrealdb::{error, Error};
use tokio::task::JoinSet;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let surreal = surrealdb::engine::any::connect(format!("ws://localhost:12773"))
        .await
        .unwrap();

    surreal.use_ns("test").use_db("test").await.unwrap();

    write_task(surreal.clone()).await;

    verify_task(surreal.clone()).await;
}

async fn verify_task(db: Surreal<Any>) {
    let len: Option<usize> = db
        .query("select value array::len(arr) from foo:1")
        .await
        .unwrap()
        .check()
        .unwrap()
        .take(0)
        .unwrap();
    assert_eq!(len, Some(1000));
}
async fn write_task(surreal: Surreal<Any>) {
    surreal
        .query("delete foo return none parallel")
        .await
        .unwrap()
        .check()
        .unwrap();

    let mut join_set = JoinSet::new();
    for i in 0..1000 {
        join_set.spawn({
            let db = surreal.clone();
            async move {
                let mut retries = 0;
                loop {
                    match db
                        .query(
                            "
                            update foo:1 set arr += [1] return none; 
                        ",
                        )
                        .await
                        .unwrap()
                        .check()
                    {
                        // retry https://github.com/surrealdb/surrealdb/issues/2862
                        Err(Error::Api(error::Api::Query(d)))
                            if d.contains("Resource busy:") && retries < 50 =>
                        {
                            // retry
                            retries += 1;
                            eprintln!("#{i} retry {retries}");
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                        Ok(_) => {
                            // ok
                            break;
                        }
                        Err(e) => {
                            // fail
                            panic!("{:?}", e);
                        }
                    }
                }
            }
        });
    }
    while let Some(res) = join_set.join_next().await {
        res.unwrap();
    }
}
