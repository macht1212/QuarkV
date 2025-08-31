from pathlib import Path
from setuptools import Extension, setup

SRC = Path(__file__).parent / "src"

ext_modules = [
    Extension(
        name="kvxdb_core",
        sources=[str(SRC / "_kvxdb.c"), str(SRC / "kvxdb.c")],
        include_dirs=[str(SRC)],
    )
]

setup(ext_modules=ext_modules)
