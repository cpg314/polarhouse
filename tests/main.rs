use polars::prelude::*;
use yare::parameterized;

use polarhouse::{ClickhouseClient, GetOptions, HttpClient, TableCreationOptions};

fn create_df() -> anyhow::Result<DataFrame> {
    let name = Series::new("name", &["Batman", "Superman"]);
    let age = Series::new("age", &[Some(30), None]);
    let powers = Series::new(
        "powers",
        &[
            Series::new("", ["intelligence"]),
            Series::new("", ["flying", "vision"]),
        ],
    );
    let is_rich = Series::new("is_rich", &[Some(true), None]);
    let address = StructChunked::new(
        "address",
        &[
            StructChunked::new(
                "city",
                &[
                    Series::new("city", &["Gotham", "New York"]),
                    Series::new("state", &[None, Some("NY")]),
                ],
            )?
            .into_series(),
            Series::new("country", &["USA", "USA"]),
        ],
    )?
    .into_series();
    Ok([name, is_rich, age, powers, address].into_iter().collect())
}

async fn retrieve(
    df: DataFrame,
    table_name: &str,
    ch: impl ClickhouseClient,
) -> anyhow::Result<()> {
    // Retrieve dataframe from Clickhouse
    let df2 = polarhouse::get_df_query(
        klickhouse::SelectBuilder::new(table_name).select("*"),
        Default::default(),
        &ch,
    )
    .await?;
    assert_eq!(df, df2);
    println!("{}", df2);

    // Do not unflatten structs
    let df2 = polarhouse::get_df_query(
        klickhouse::SelectBuilder::new(table_name).select("*"),
        GetOptions {
            unflatten_structs: false,
            ..Default::default()
        },
        &ch,
    )
    .await?;
    println!("{}", df2);
    assert_eq!(df2.get_column_names().len(), 7);

    // A query that returns no results
    let df2 = polarhouse::get_df_query(
        klickhouse::SelectBuilder::new(table_name)
            .select("*")
            .where_("name = 'invalid'"),
        Default::default(),
        &ch,
    )
    .await?;
    assert!(df2.is_empty());

    // Get types from Clickhouse, which allows retrieving booleans as bools rather than u8.
    let table = polarhouse::ClickhouseTable::from_server(table_name, &ch).await?;
    let df2 = table
        .get_df_query(klickhouse::SelectBuilder::new(table_name).select("*"), &ch)
        .await?;
    println!("{}", df2);
    Ok(())
}
#[parameterized(http = {true}, native = {false})]
#[test_macro(tokio::test)]
async fn test(http: bool) -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    polars::enable_string_cache();

    let ch = klickhouse::Client::connect("localhost:9000", Default::default()).await?;

    // Setup
    let table_name = &format!("superheroes_{:?}", http);
    ch.execute(format!("DROP TABLE IF EXISTS {}", table_name))
        .await?;

    // Create dataframe
    let df = create_df()?;
    println!("{}", df);

    println!("{:?}", df.schema());

    // Insert dataframe into Clickhouse
    let table = polarhouse::ClickhouseTable::from_polars_schema(
        table_name,
        df.schema(),
        Default::default(),
        ["age", "is_rich", "address.city.state"],
    )?;
    table
        .create(
            TableCreationOptions {
                primary_keys: &["name"],
                ..Default::default()
            },
            &ch,
        )
        .await?;
    table.insert_df(df.clone(), Default::default(), &ch).await?;

    println!("Retrieve data",);
    if http {
        let ch_http = HttpClient::new("http://localhost:8123", "default", None);
        retrieve(df, table_name, ch_http).await?;
    } else {
        retrieve(df.clone(), table_name, ch).await?;
    }

    Ok(())
}
