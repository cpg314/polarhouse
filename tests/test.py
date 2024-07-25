import os
import shutil
import logging
import unittest
import tempfile

from polarhouse import Client

# Run the main integration tests first to create the table
class Test(unittest.IsolatedAsyncioTestCase):
    query = "SELECT * from superheroes_true"

    async def asyncSetUp(self):
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)

    async def test_main(self):
        df = await self.client.get_df_query(Test.query)
        print(df)

    async def test_unflatten(self):
        df = await self.client.get_df_query(Test.query, unflatten_structs=False)
        print(df)
        assert len(df.columns) == 7

        # Similar result with the unnest polars method
        df = await self.client.get_df_query(Test.query)
        df = df.unnest("address").unnest("city")
        print(df)
        assert len(df.columns) == 7

    async def test_file(self):
        df = await self.client.get_df_query(Test.query)

        f = tempfile.NamedTemporaryFile()
        f.write(Test.query.encode("utf-8"))
        f.flush()
        df2 = await self.client.get_df_query_file(f.name)

        assert df.equals(df2)

    async def test_caching(self):
        df = await self.client.get_df_query(Test.query)
        shutil.rmtree(os.path.expanduser("~/.cache/polarhouse"))
        client = await Client.connect("localhost:9000", caching=True)
        for i in range(2):
            df2 = await client.get_df_query(Test.query)
            assert df.equals(df2)

    async def test_error(self):
        with self.assertRaises(OSError):
            await self.client.get_df_query("invalid")


class NativeTest(Test):
    async def asyncSetUp(self):
        self.client = await Client.connect("localhost:9000")
        Test.setUp(self)


class HttpTest(Test):
    async def asyncSetUp(self):
        self.client = await Client.connect("http://localhost:8123")
        Test.setUp(self)


if __name__ == "__main__":
    suite = unittest.TestSuite([unittest.defaultTestLoader.loadTestsFromTestCase(t) for t in [HttpTest, NativeTest]])
    unittest.TextTestRunner(verbosity=2).run(suite)
