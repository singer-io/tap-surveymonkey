#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-surveymonkey",
    version="0.1.5",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_surveymonkey"],
    install_requires=[
        "singer-python==5.6.0",
        "requests==2.22.0",
    ],
    extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint==2.4.4',
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
