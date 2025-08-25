# setup.py
# This file is required by Apache Beam to package and distribute your pipeline code.

import setuptools

# This is the main setup function for your project.
# It uses setuptools to define the project's metadata and package structure.
setuptools.setup(
    name='mongo-to-parquet',
    version='1.0.0',
    description='Apache Beam pipeline to transform MongoDB JSON data to Parquet.',
    author='Your Name',  # Replace with your name
    # Tells setuptools to automatically find all Python packages (directories with an __init__.py file)
    # in the current directory. This will include your main.py, schema.py, and mappings.py.
    packages=setuptools.find_packages(),
    # Although we're using a separate requirements.txt file in our deploy script,
    # this list can be used for local development and to provide a fallback.
    install_requires=[],
)

