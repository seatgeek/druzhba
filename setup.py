#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 SeatGeek
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

tests_require = ["nose>=1.3.7", "mock>=2.0.0"]

setuptools.setup(
    name="druzhba",
    version="0.1.1",
    author="todo",
    author_email="todo",
    description="A friendly ETL pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seatgeek/druzhba",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        # "License :: OSI Approved :: MIT License",  # TODO
        "Operating System :: OS Independent",
        "Private :: Do Not Upload",  # TODO
    ],
    python_requires=">=3.6",
    setup_requires=[
        "cython>=0.29.7"  # TODO: find a way to not pre-install this (needed for mssql)
    ],
    # TODO: move specific DBs into extras
    install_requires=[
        "boto3>=1.10.34",
        "botocore>=1.13.35",
        "fastavro>=0.21.22,<0.22",
        "Jinja2>=2.10",
        "psycopg2-binary>=2.7.3.2",
        "pyaml>=17.10.0",
        "pymssql<3.0",  # TODO: replace with PyODBC
        "pymysql>=0.7.11",
        "sentry-sdk>=0.11.0,<0.14",
        "statsd>=3.3.0",
    ],
    extras_require={
        "dev": ["autoflake", "black", "isort"],
        "test": tests_require
    },
    entry_points={"console_scripts": ["druzhba=druzhba.main:main"]},
    scripts=[],
    tests_require=tests_require
)
