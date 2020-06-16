#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 SeatGeek
import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

package_vars = {}
with open("druzhba/_version.py", "r") as f:
    exec(f.read(), package_vars)

tests_require = [
    "nose>=1.3.7",
    "mock>=2.0.0",
    "pylint==2.5.3",
]

setuptools.setup(
    name="druzhba",
    version=package_vars['__version__'],
    author="Seatgeek and Contributors",
    author_email="druzhba-maintainers@seatgeek.com",
    description="A friendly data pipeline framework",
    license="MIT License",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/seatgeek/druzhba",
    packages=setuptools.find_packages(exclude=['test']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
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
        "dev": ["autoflake", "black", "isort", "sphinx", "recommonmark", "twine"],
        "test": tests_require
    },
    entry_points={"console_scripts": ["druzhba=druzhba.main:main"]},
    scripts=[],
    tests_require=tests_require
)
