#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-surveymonkey",
    version="1.0.2",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_surveymonkey"],
    install_requires=[
        "singer-python==5.6.0",
        "requests==2.31.0",
    ],
    extras_require={
        'dev': [
            'ipdb',
            'pylint',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-surveymonkey=tap_surveymonkey:main
    """,
    packages=["tap_surveymonkey"],
    package_data={
        "schemas": ["tap_surveymonkey/schemas/*.json"]
    },
    include_package_data=True,
)