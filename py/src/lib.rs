use pyo3::{
    exceptions::{PyException, PyIOError},
    prelude::*,
};
use pyo3_polars::PyDataFrame;

#[pyclass]
struct Client(klickhouse::Client);

#[pymethods]
impl Client {
    #[staticmethod]
    #[pyo3(signature = (address, database="default".into(), username="default".into(), password="".into()))]
    fn connect(
        py: Python,
        address: String,
        database: String,
        username: String,
        password: String,
    ) -> PyResult<&PyAny> {
        let locals = pyo3_asyncio::TaskLocals::with_running_loop(py)?.copy_context(py)?;
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
            .map_err(|e| PyException::new_err(format!("Failed to connect to Clickhouse: {}", e)))
            .map(Client)
        })
    }
    fn get_df_query<'p>(&self, py: Python<'p>, query: String) -> PyResult<&'p PyAny> {
        let locals = pyo3_asyncio::TaskLocals::with_running_loop(py)?.copy_context(py)?;
        let ch = self.0.clone();
        pyo3_asyncio::tokio::future_into_py_with_locals(py, locals.clone(), async move {
            polarhouse::get_df_query(query, Default::default(), &ch)
                .await
                .map_err(|e| PyIOError::new_err(e.to_string()))
                .map(PyDataFrame)
        })
    }
}

#[pymodule]
#[pyo3(name = "polarhouse")]
fn polarhouse_module(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // It appears that enabling the string cache from Python has no effect.
    polarhouse::polars::enable_string_cache();
    m.add_class::<Client>()?;
    Ok(())
}
