import setuptools
import dna_util

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="dna_util",
    version=dna_util.__version__,
    author="Erich Wellinger",
    author_email="erich@airdna.co",
    description="AirDNA Data Science Functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/airdnallc/dna_util",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3"
    ),
    include_package_data=True,
    install_requires=[
        "pyaml",
        "pandas",
        "s3fs"
    ]
)
