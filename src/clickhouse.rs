use futures::{stream, Stream, StreamExt, TryStreamExt};
use klickhouse::block::Block;

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
