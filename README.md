**Polarhouse** connects together

- [Polars](https://pola.rs/) Dataframes (backed by the [Apache Arrow](https://arrow.apache.org/) columnar format)
- the [Clickhouse](https://clickhouse.com/) columnar database.

More specifically, it allows:

- inserting Polars Dataframes into Clickhouse tables (and creating these if necessary).
- and vice-versa retrieving Clickhouse query results as Polars Dataframes.

Polarhouse uses the native TCP Clickhouse protocol via the [`klickhouse`](https://github.com/Protryon/klickhouse) crate. It maps the Polars and Clickhouse types, and builds Polars `Series` (resp. Clickhouse columns) after transforming the data if necessary.

```
Polars
┌──────────┬─────────┬──────┬───────────────────────────┐
│ name     ┆ is_rich ┆ age  ┆ address                   │
│ ---      ┆ ---     ┆ ---  ┆ ---                       │
│ str      ┆ u8      ┆ i32  ┆ struct[2]                 │
╞══════════╪═════════╪══════╪═══════════════════════════╡
│ Batman   ┆ 1       ┆ 30   ┆ {{"Chicago","IL"},"USA"}  │
│ Superman ┆ null    ┆ null ┆ {{"New York","NY"},"USA"} │
└──────────┴─────────┴──────┴───────────────────────────┘
Clickhouse
┌─name─────┬─is_rich─┬──age─┬─address.city.city─┬─address.city.state─┬─address.country─┐
│ Batman   │ true    │   30 │ Chicago           │ IL                 │ USA             │
│ Superman │ null    │ null │ New York          │ NY                 │ USA             │
└──────────┴─────────┴──────┴───────────────────┴────────────────────┴─────────────────┘
```

## Polars to Clickhouse

### Rust

```rust
let ch = klickhouse::Client::connect("localhost:9000", Default::default()).await?;

let df: DataFrame = ...

// Deduce table schema from the dataframe
let table = polarhouse::ClickhouseTable::from_polars_schema(table_name, df.schema(), [])?;

// Create Clickhouse table corresponding to the Dataframe (optional)
table.create(&ch, TableCreateOptions { primary_keys: &["name"] , ..Default::default() }).await?;

// Insert dataframe contents into table
table.insert_df(df, &ch).await?;
```

## Clickhouse to Polars

### Rust

```rust
let ch = klickhouse::Client::connect("localhost:9000", Default::default()).await?;

// Retrieve Clickhouse query results as a Dataframe.
let df: DataFrame = polarhouse::get_df_query(
    klickhouse::SelectBuilder::new(table_name).select("*"),
    Default::default(),
    &ch,
).await?;
```

### Python

```python
from polarhouse import Client
client = await Client.connect("localhost:9000", caching=True)
df = await self.client.get_df_query("SELECT * from superheros")

```

## Status

<p style="background:rgba(255,181,77,0.16);padding:0.75em;">
This is for now only a proof of concept.
</p>

## Alternative solutions

- Use the `Arrow`, `ArrowStream` or `Parquet` [Clickhouse input/output formats](https://clickhouse.com/docs/en/interfaces/formats), which can be read and written from Polars.
- Write an [Arrow Database Connectivity](https://arrow.apache.org/docs/format/ADBC.html) driver for Clickhouse, and use [Polars' ADBC support](https://docs.pola.rs/user-guide/io/database/).
- Clickhouse to Polars: [ConnectorX](https://github.com/sfu-db/connector-x) (uses the [MySQL interface](https://clickhouse.com/docs/en/interfaces/mysql)).

## Tests

```
$ docker run --network host --rm --name clickhouse clickhouse/clickhouse-server:latest
$ cargo nextest run -r --nocapture
```

## Supported types

- [x] Integers
- [x] Floating points
- [x] Strings
- [x] Booleans
- [x] Categorical (Polars) / Low cardinality (Clickhouse)
- [x] Structs (Polars), which get flattened into Clickhouse, with fields names separated by `.`
- [x] Nullables
- [x] Lists (Polars) / Arrays (Clickhouse)
- [x] UUIDs (mapped to Strings in Polars)
- [ ] Arrays (Polars)
- [ ] Tuples
- [ ] DateTime
- [ ] Time
- [ ] Duration
- [ ] ...
