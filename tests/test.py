import asyncio
import os
import shutil
import logging
import tempfile

from polarhouse import Client


async def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    client = await Client.connect("localhost:9000")

    # Query from string
    query = "SELECT * from superheroes"
    df = await client.get_df_query(query)
    print(df)

    # Query from file
    f = tempfile.NamedTemporaryFile()
    f.write(query.encode("utf-8"))
    f.flush()
    df2 = await client.get_df_query_file(f.name)

    assert df.equals(df2)

    # Caching
    shutil.rmtree(os.path.expanduser("~/.cache/polarhouse"))
    client = await Client.connect("localhost:9000", caching=True)
    for i in range(2):
        query = "SELECT * from superheroes"
        df2 = await client.get_df_query(query)
        assert df.equals(df2)


asyncio.run(main())
