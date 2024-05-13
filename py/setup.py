from setuptools import setup
import os

setup(
    name="polarhouse",
    version=os.environ.get("CARGO_MAKE_CRATE_VERSION"),
    py_modules=["polarhouse"],
    packages=[""],
    package_data={"": ["polarhouse.so"]},
)
