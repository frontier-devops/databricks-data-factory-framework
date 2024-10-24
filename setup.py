from setuptools import setup, find_packages

setup(
    name="data_factory_framework",
    version='v0.1.0',
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        "setuptools>=72.2.0",
        "databricks-connect>=15.3.1",
        "psutil>=6.0.0",
        "wheel>=0.44.0"
    ]
)
