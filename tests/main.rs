use polarhouse::TableCreationOptions;
use polars::prelude::*;

#[tokio::test]
async fn test() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    polars::enable_string_cache();

    let ch = klickhouse::Client::connect("localhost:19000", Default::default()).await?;

    // Setup
    let table_name = "superheros";
    ch.execute(format!("DROP TABLE IF EXISTS {}", table_name))
        .await?;

    // Create dataframe
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
    let df: DataFrame = [name, is_rich, age, powers, address].into_iter().collect();
    println!("{}", df);

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

    // Retrieve dataframe from Clickhouse
    let df2 = polarhouse::get_df_query(
        klickhouse::SelectBuilder::new(table_name).select("*"),
        Default::default(),
        &ch,
    )
    .await?;
    assert_eq!(df, df2);
    println!("{}", df2);

    // Get types from Clickhouse, which allows retrieving booleans as bools rather than u8.
    let table = polarhouse::ClickhouseTable::from_server(table_name, &ch).await?;
    let df2 = table
        .get_df_query(klickhouse::SelectBuilder::new(table_name).select("*"), &ch)
        .await?;
    println!("{}", df2);

    Ok(())
}
