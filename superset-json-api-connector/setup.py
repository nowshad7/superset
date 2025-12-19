from setuptools import setup, find_packages
import os

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="superset-json-api-connector",
    version="0.1.0",
    author="Robiul Hasan Nowshad",
    author_email="nowshad21aug@gmail.com",
    description="SQLAlchemy dialect for connecting Superset to JSON APIs",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/superset-json-api-connector",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "sqlalchemy.dialects": [
            "jsonapi = superset_json_api.dialect:JSONAPIDialect",
        ]
    },
    keywords="superset, sqlalchemy, json, api, connector, dialect",
    project_urls={
        "Bug Reports": "https://github.com/yourusername/superset-json-api-connector/issues",
        "Source": "https://github.com/yourusername/superset-json-api-connector",
    },
)
