use itertools::Itertools;
use klickhouse::IndexMap;
use polars::prelude::*;
use tracing::*;

use crate::Error;

/// Flatten StructChunked columns
/// col1 -> field1, field2
/// is transformed into
/// col1.field1, col1.field2
pub fn flatten(df: DataFrame) -> Result<DataFrame, Error> {
    Ok(df
        .get_columns()
        .iter()
        .flat_map(|col| -> Box<dyn Iterator<Item = Series>> {
            if let Ok(s) = col.struct_() {
                flatten_structchunked(s)
            } else {
                Box::new(std::iter::once(col.clone()))
            }
        })
        .collect())
}

fn flatten_structchunked(s: &StructChunked) -> Box<dyn Iterator<Item = Series> + '_> {
    Box::new(
        s.fields()
            .iter()
            .flat_map(|field| -> Box<dyn Iterator<Item = Series>> {
                if let Ok(s) = field.struct_() {
                    flatten_structchunked(s)
                } else {
                    Box::new(std::iter::once(field.clone()))
                }
            })
            .map(|mut field| {
                field.rename(&format!("{}.{}", s.name(), field.name()));
                field
            }),
    )
}

pub fn flatten_schema(schema: &Schema) -> Result<Schema, Error> {
    debug!(?schema, "Flattening schema");
    let schema = flatten(DataFrame::from(schema))?.schema();
    debug!(?schema, "Flattened schema");
    Ok(schema)
}

/// Inverse of `flatten`.
pub fn unflatten(series: IndexMap<String, Series>) -> Result<IndexMap<String, Series>, Error> {
    // Mapping Prefix -> columns
    let mut structs = IndexMap::<
        String, /* prefix */
        Vec<(String /* full name */, String /* suffix */)>,
    >::default();
    series
        .keys()
        .filter(|k| k.contains('.'))
        .map(|k| {
            let mut it = k.split('.');
            let prefix = it.next().unwrap();
            (prefix.to_string(), (k.clone(), it.join(".")))
        })
        .for_each(|(k, v)| structs.entry(k).or_default().push(v));
    // Create StructChunked
    let mut structs: IndexMap<String, Series> = structs
        .into_iter()
        .map(|(prefix, fields)| -> Result<_, Error> {
            let fields: IndexMap<String, Series> = fields
                .into_iter()
                .map(|(name, suffix)| {
                    let mut s = series.get(&name).unwrap().clone();
                    s.rename(&suffix);
                    (suffix, s)
                })
                .collect();
            let fields: Vec<Series> = unflatten(fields)?.into_values().collect();
            let series = StructChunked::new(&prefix, &fields)?.into_series();
            Ok((prefix, series))
        })
        .try_collect()?;
    // Create result, preserving order
    Ok(series
        .into_iter()
        .filter_map(|(name, field)| {
            if let Some((prefix, s)) = name
                .split('.')
                .next()
                .and_then(|prefix| structs.remove(prefix).map(|s| (prefix, s)))
            {
                Some((prefix.to_string(), s))
            } else if name.contains('.') {
                // Already inserted
                None
            } else {
                Some((name, field))
            }
        })
        .collect())
}
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn flatten() -> anyhow::Result<()> {
        let regular = Series::new("col0", &[1, 2]);
        let level1 =
            StructChunked::new("col1", &[Series::new("field1", &["v1", "v2"])])?.into_series();
        let level2 = StructChunked::new(
            "col2",
            &[
                StructChunked::new(
                    "field1",
                    &[
                        Series::new("subfield1", &["v1", "v2"]),
                        Series::new("subfield2", &["v3", "v4"]),
                    ],
                )?
                .into_series(),
                Series::new("field2", &["v1", "v2"]),
            ],
        )?
        .into_series();

        let df: DataFrame = [level1, regular, level2].into_iter().collect();
        println!("{}", df);
        let df2 = super::flatten(df.clone())?;
        println!("{}", df2);
        assert_eq!(
            df2.get_column_names(),
            vec![
                "col1.field1",
                "col0",
                "col2.field1.subfield1",
                "col2.field1.subfield2",
                "col2.field2",
            ]
        );

        let series: IndexMap<String, Series> = df2
            .get_columns()
            .iter()
            .map(|col| (col.name().to_string(), col.clone()))
            .collect();
        let df3: DataFrame = super::unflatten(series)?.into_values().collect();

        assert_eq!(df3, df);

        Ok(())
    }
}
