**Polarhouse** connects together

- [Polars](https://pola.rs/) Dataframes (backed by the [Apache Arrow](https://arrow.apache.org/) columnar format)
- the [Clickhouse](https://clickhouse.com/) columnar database.

More specifically, it allows inserting Dataframes into tables, and vice-versa retrieving queries as Dataframes.

Communication with Clickhouse is made through the [klickhouse] crate.

### Polars to Clickhouse

```rust
let ch = klickhouse::Client::connect("localhost:9000", Default::default()).await?;

// Load dataframe from a parquet file
let df: DataFrame = LazyFrame::scan_parquet("test.parquet", Default::default())?.collect()?;
info!("{}", df);

// Deduce table schema from the dataframe.
// We specify the primary key manually.
let table = ClickhouseTable::from_polars_schema("test", df.schema(), vec!["country", "name"])?
            .with_client(ch.clone());

// Create a table corresponding to the dataframe
table.create().await?;

// Insert the dataframe into the table
table.insert_df(&df).await?;

```

### Clickhouse to Polars

```rust
let ch = klickhouse::Client::connect("localhost:9000", Default::default()).await?;

// Read the schema from the database.
// This would correspond to the output of `ClickhouseTable::from_polars_schema`.
let table2 = ClickhouseTable::from_table("test", ch).await?;
// Retrieve the dataframe
let df = table2.get_df().await?;
info!("{}", df2);
// Retrieve parts of the dataframe with a custom `klickhouse::SelectBuilder` query
let df: DataFrame = table2.get_df_query(/* ... */).await?;
```

## Status

<p style="background:rgba(255,181,77,0.16);padding:0.75em;">
This is for now only a proof of concept.
</p>

An alternative solution would be to write an [Arrow Database Connectivity](https://arrow.apache.org/docs/format/ADBC.html) driver for Clickhouse, and use [Polars' ADBC support](https://docs.pola.rs/user-guide/io/database/).
