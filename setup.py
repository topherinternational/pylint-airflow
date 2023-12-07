"""Setup of pylint-airflow package"""
from pathlib import Path

from setuptools import setup, find_packages

requirements = ["pylint>=2.14.0,<4.0.0"]

readme_path = Path(__file__).resolve().parent / "README.rst"
with open(readme_path, encoding="utf-8") as readme_file:
    long_description = readme_file.read()

setup(
    name="pylint-airflow",
    url="https://github.com/BasPH/pylint-airflow",
    description="A Pylint plugin to lint Apache Airflow code.",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    version="0.2.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=requirements,
    python_requires=">=3.7",
    keywords=["pylint", "airflow", "plugin"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
