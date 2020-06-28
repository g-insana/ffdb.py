import setuptools

with open("README_brief.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ffdb",
    version="2.4.1",
    author="Dr Giuseppe Insana",
    author_email="insana@insana.net",
    description="ffdb",
    long_description=long_description,
    long_description_content_type="text/markdown",
    scripts=["scripts/indexer.py", "scripts/extractor.py",
             "scripts/remover.py", "scripts/merger.py"],
    url="https://github.com/g-insana/ffdb.py",
    license="AGPL",
    py_modules=["ffdb"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "sortedcontainers",
        "requests",
        "pycryptodomex"
    ],
    python_requires=">=3.5",
)
