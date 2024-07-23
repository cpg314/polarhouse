use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use log::*;
use pyo3::{
    exceptions::{PyException, PyIOError},
    prelude::*,
};
use pyo3_polars::PyDataFrame;

use polarhouse::polars::prelude::*;
use polarhouse::GetOptions;

#[pyclass]
struct Client {
    inner: klickhouse::Client,
    cache: Option<PathBuf>,
}

#[pymethods]
impl Client {
    #[staticmethod]
    #[pyo3(signature = (address, database="default".into(), username="default".into(), password="".into(), caching=false))]
    fn connect(
        py: Python,
        address: String,
        database: String,
        username: String,
        password: String,
        caching: bool,
    ) -> PyResult<&PyAny> {
        info!("Connecting to Clickhouse server at {}", address);
        if caching {
            info!("Caching enabled");
        }
        let locals = pyo3_asyncio::TaskLocals::with_running_loop(py)?.copy_context(py)?;
        let cache = caching.then(dirs::cache_dir).flatten().and_then(|c| {
            let c = c.join("polarhouse");
            std::fs::create_dir_all(&c).ok()?;
            Some(c)
        });

        pyo3_asyncio::tokio::future_into_py_with_locals(py, locals.clone(), async move {
            klickhouse::Client::connect(
                address,
                klickhouse::ClientOptions {
                    username,
                    default_database: database,
                    password,
                },
            )
            .await
            .map_err(|e| PyException::new_err(format!("Failed to connect to Clickhouse: {:?}", e)))
            .map(|inner| Client { inner, cache })
        })
    }
    #[pyo3(signature = (filename, unflatten_structs=true))]
    fn get_df_query_file<'p>(
        &self,
        py: Python<'p>,
        filename: PathBuf,
        unflatten_structs: bool,
    ) -> PyResult<&'p PyAny> {
        let sql = std::fs::read_to_string(&filename).map_err(|e| {
            PyIOError::new_err(format!("Failed to read from {:?}: {}", filename, e))
        })?;
        self.get_df_query(py, sql, unflatten_structs)
    }
    #[pyo3(signature = (query, unflatten_structs=true))]
    fn get_df_query<'p>(
        &self,
        py: Python<'p>,
        query: String,
        unflatten_structs: bool,
    ) -> PyResult<&'p PyAny> {
        let locals = pyo3_asyncio::TaskLocals::with_running_loop(py)?.copy_context(py)?;
        let ch = self.inner.clone();
        let cache = if let Some(cache) = &self.cache {
            let mut s = DefaultHasher::new();
            query.hash(&mut s);
            unflatten_structs.hash(&mut s);
            let hash = s.finish();
            let filename = cache.join(hash.to_string());
            if filename.is_file() {
                info!("Reading from cache {:?}", filename);
                let reader = std::fs::read(&filename)?;
                let reader = std::io::Cursor::new(reader);
                match ParquetReader::new(reader).finish() {
                    Ok(df) => {
                        return pyo3_asyncio::tokio::future_into_py(py, async move {
                            Ok(PyDataFrame(df))
                        });
                    }
                    Err(_) => {
                        std::fs::remove_file(&filename)?;
                    }
                }
            }
            Some(filename)
        } else {
            None
        };
        pyo3_asyncio::tokio::future_into_py_with_locals(py, locals.clone(), async move {
            info!("Sending query");
            let start = std::time::Instant::now();
            debug!("{}", query);
            let mut df = polarhouse::get_df_query(
                query,
                GetOptions {
                    unflatten_structs,
                    ..Default::default()
                },
                &ch,
            )
            .await
            .map_err(|e| PyIOError::new_err(format!("{:?}", e)))?;
            info!("Received response in {:?}", start.elapsed());
            if let Some(cache) = cache {
                debug!("Saving to cache {:?}", cache);
                let mut file = std::fs::File::create(cache)?;
                let _ = ParquetWriter::new(&mut file).finish(&mut df);
            }
            Ok(PyDataFrame(df))
        })
    }
}

#[pymodule]
#[pyo3(name = "polarhouse")]
fn polarhouse_module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    pyo3_log::init();
    // It appears that enabling the string cache from Python has no effect.
    polarhouse::polars::enable_string_cache();
    m.add_class::<Client>()?;
    Ok(())
}
