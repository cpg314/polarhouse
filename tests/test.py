import asyncio
from polarhouse import Client


async def main():
    client = await Client.connect("localhost:9000")
    df = await client.get_df_query("SELECT * from superheroes")
    print(df)


asyncio.run(main())
