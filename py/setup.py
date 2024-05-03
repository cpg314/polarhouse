from setuptools import setup
import os

setup(
    name="polarhouse",
    version=os.environ.get("VERSION"),
    py_modules=["polarhouse"],
    packages=[""],
    package_data={"": ["polarhouse.so"]},
)
