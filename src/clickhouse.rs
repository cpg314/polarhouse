use futures::{stream, Stream, StreamExt, TryStreamExt};
use klickhouse::block::Block;
use tokio::io::AsyncBufReadExt;

use crate::{Error, HttpError};

#[derive(Clone)]
pub enum Client {
    Native(klickhouse::Client),
    Http(http::HttpClient),
}
impl Client {
    pub async fn connect(
        address: &str,
        default_database: Option<&str>,
        username: &str,
        password: Option<&str>,
    ) -> Result<Self, Error> {
        if address.starts_with("http://") || address.starts_with("https://") {
            Ok(Self::Http(http::HttpClient::new(
                address,
                default_database,
                username,
                password,
            )))
        } else {
            klickhouse::Client::connect(
                address,
                klickhouse::ClientOptions {
                    username: username.into(),
                    password: password.unwrap_or_default().into(),
                    default_database: default_database.unwrap_or("default").into(),
                },
            )
            .await
            .map(Self::Native)
            .map_err(Error::from)
        }
    }
}
// Manual dynamic dispatch boilerplace because the trait is not object-safe
impl ClientGeneric for Client {
    fn sends_initial_block(&self) -> bool {
        match self {
            Client::Native(c) => c.sends_initial_block(),
            Client::Http(c) => c.sends_initial_block(),
        }
    }
    async fn insert_native_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
        blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> Result<impl Stream<Item = Result<Block, Error>>, Error> {
        match self {
            Client::Native(c) => Ok(c
                .insert_native_raw(query, blocks)
                .await?
                .map_err(Error::from)
                .boxed()),
            Client::Http(c) => Ok(c
                .insert_native_raw(query, blocks)
                .await?
                .map_err(Error::from)
                .boxed()),
        }
    }
    async fn query_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
    ) -> Result<impl Stream<Item = Result<Block, Error>> + Unpin, Error> {
        match self {
            Client::Native(c) => Ok(c.query_raw(query).await?.map_err(Error::from).boxed()),
            Client::Http(c) => Ok(c.query_raw(query).await?.map_err(Error::from).boxed()),
        }
    }
}

pub trait ClientGeneric {
    fn sends_initial_block(&self) -> bool;
    fn insert_native_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
        blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> impl std::future::Future<Output = Result<impl Stream<Item = Result<Block, Error>>, Error>>;
    fn query_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
    ) -> impl std::future::Future<Output = Result<impl Stream<Item = Result<Block, Error>> + Unpin, Error>>;
    fn execute(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
    ) -> impl std::future::Future<Output = Result<(), Error>> {
        // From the implementation of klickhouse::Client::execute
        async {
            let mut stream = self.query::<klickhouse::RawRow>(query).await?;
            while let Some(next) = stream.next().await {
                next?;
            }
            Ok(())
        }
    }
    fn query<T: klickhouse::Row>(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
    ) -> impl std::future::Future<Output = Result<impl Stream<Item = Result<T, Error>> + Unpin, Error>>
    {
        // From the implementation of klickhouse::Client::query
        async {
            let raw = self.query_raw(query).await?;
            Ok(raw.flat_map(|block| match block {
                Ok(mut block) => stream::iter(
                    block
                        .take_iter_rows()
                        .filter(|x| !x.is_empty())
                        .map(|m| T::deserialize_row(m).map_err(Error::from))
                        .collect::<Vec<_>>(),
                ),
                Err(e) => stream::iter(vec![Err(e)]),
            }))
        }
    }
}
impl ClientGeneric for klickhouse::Client {
    fn sends_initial_block(&self) -> bool {
        true
    }
    async fn insert_native_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
        blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> Result<impl Stream<Item = Result<Block, Error>>, Error> {
        Ok(self
            .insert_native_raw(query, blocks)
            .await?
            .map_err(Error::from))
    }
    async fn query_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
    ) -> Result<impl Stream<Item = Result<Block, Error>> + Unpin, Error> {
        Ok(self.query_raw(query).await?.map_err(Error::from))
    }
}

pub mod http {

    use super::*;

    /// Client for the Clickhouse HTTP interface, using the native format.
    pub struct HttpClient {
        builder: reqwest::RequestBuilder,
        database: String,
    }
    impl Clone for HttpClient {
        fn clone(&self) -> Self {
            Self {
                builder: self.builder.try_clone().unwrap(),
                database: self.database.clone(),
            }
        }
    }

    impl HttpClient {
        pub fn new(
            url: &str,
            default_database: Option<&str>,
            username: &str,
            password: Option<&str>,
        ) -> Self {
            Self {
                database: default_database.unwrap_or("default").into(),
                builder: reqwest::ClientBuilder::new()
                    .zstd(true)
                    .build()
                    .unwrap()
                    .post(url)
                    .header(reqwest::header::TRANSFER_ENCODING, "chunked")
                    .basic_auth(username, password),
            }
        }
    }

    impl ClientGeneric for HttpClient {
        fn sends_initial_block(&self) -> bool {
            false
        }
        async fn query_raw(
            &self,
            query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
        ) -> Result<impl Stream<Item = Result<Block, Error>> + Unpin, Error> {
            let resp = self
                .clone()
                .builder
                .query(&[("default_format", "Native"), ("database", &self.database)])
                .body(query.try_into()?.to_string())
                .send()
                .await
                .map_err(HttpError::from)?;
            if !resp.status().is_success() {
                return Err(HttpError::Server(resp.text().await.unwrap_or_default()).into());
            }
            let reader = tokio_util::io::StreamReader::new(
                resp.bytes_stream()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );
            let stream = stream::unfold(reader, |mut reader| async {
                match reader.fill_buf().await {
                    Err(e) => {
                        return Some((Err(HttpError::IO(e).into()), reader));
                    }
                    Ok(buf) if buf.is_empty() => {
                        return None;
                    }
                    _ => {}
                }
                Some((
                    Block::read(&mut reader, 0).await.map_err(Error::from),
                    reader,
                ))
            });
            // Repeat the first block in lieu of an initial block
            let mut stream = Box::pin(stream.fuse());
            match stream.next().await {
                None => Ok(stream.boxed()),
                Some(Err(e)) => Err(e),
                Some(Ok(s)) => Ok(Box::pin(
                    stream::repeat_with(move || Ok(s.clone()))
                        .take(2)
                        .chain(stream),
                )
                .boxed()),
            }
        }
        async fn insert_native_raw(
            &self,
            _query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError> + 'static,
            _blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
        ) -> Result<impl Stream<Item = Result<Block, Error>>, Error> {
            Err::<stream::Empty<_>, _>(HttpError::Insertion.into())
        }
    }
}
