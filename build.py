# build.py — poetry build hook
from pathlib import Path
from setuptools import Extension

SRC = Path(__file__).parent / "src"

def build(setup_kwargs: dict) -> None:
    setup_kwargs.update(
        dict(
            ext_modules=[
                Extension(
                    name="kvxdb_core",
                    sources=[str(SRC / "_kvxdb.c"), str(SRC / "kvxdb.c")],
                    include_dirs=[str(SRC)],
                )
            ],
            zip_safe=False,
        )
    )
