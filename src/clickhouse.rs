use futures::{stream, Stream, StreamExt, TryStreamExt};
use klickhouse::block::Block;
use tokio::io::AsyncBufReadExt;

use crate::Error;

pub trait ClickhouseClient {
    fn sends_initial_block() -> bool;
    fn insert_native_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
        blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> impl std::future::Future<Output = Result<impl Stream<Item = Result<Block, Error>>, Error>>;
    fn query_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
    ) -> impl std::future::Future<Output = Result<impl Stream<Item = Result<Block, Error>> + Unpin, Error>>;
    fn execute(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
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
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
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
impl ClickhouseClient for klickhouse::Client {
    fn sends_initial_block() -> bool {
        true
    }
    async fn insert_native_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
        blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
    ) -> Result<impl Stream<Item = Result<Block, Error>>, Error> {
        Ok(self
            .insert_native_raw(query, blocks)
            .await?
            .map_err(Error::from))
    }
    async fn query_raw(
        &self,
        query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
    ) -> Result<impl Stream<Item = Result<Block, Error>> + Unpin, Error> {
        Ok(self.query_raw(query).await?.map_err(Error::from))
    }
}

pub mod http {

    use super::*;

    /// Client for the Clickhouse HTTP interface, using the native format.
    pub struct HttpClient {
        builder: reqwest::RequestBuilder,
    }
    impl Clone for HttpClient {
        fn clone(&self) -> Self {
            Self {
                builder: self.builder.try_clone().unwrap(),
            }
        }
    }

    impl HttpClient {
        pub fn new(url: &str, username: &str, password: Option<&str>) -> Self {
            Self {
                builder: reqwest::Client::new()
                    .post(url)
                    .header(reqwest::header::TRANSFER_ENCODING, "chunked")
                    .basic_auth(username, password),
            }
        }
    }

    impl ClickhouseClient for HttpClient {
        fn sends_initial_block() -> bool {
            false
        }
        async fn query_raw(
            &self,
            query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
        ) -> Result<impl Stream<Item = Result<Block, Error>> + Unpin, Error> {
            let resp = self
                .clone()
                .builder
                .query(&[("default_format", "Native")])
                .body(query.try_into()?.to_string())
                .send()
                .await?;
            let reader = tokio_util::io::StreamReader::new(
                resp.bytes_stream()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );
            let stream = stream::unfold(reader, |mut reader| async {
                match reader.fill_buf().await {
                    Err(e) => {
                        return Some((Err(Error::HttpIO(e)), reader));
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
            _query: impl TryInto<klickhouse::ParsedQuery, Error = klickhouse::KlickhouseError>,
            _blocks: impl Stream<Item = Block> + Send + Sync + Unpin + 'static,
        ) -> Result<impl Stream<Item = Result<Block, Error>>, Error> {
            Err::<stream::Empty<_>, _>(Error::HttpInsertion)
        }
    }
}
