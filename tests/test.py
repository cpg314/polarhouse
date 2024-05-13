import asyncio
import tempfile

from polarhouse import Client


async def main():
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

asyncio.run(main())
