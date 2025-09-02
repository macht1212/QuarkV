# build.py
from __future__ import annotations

import os
import shutil
from pathlib import Path

from Cython.Build import cythonize
from setuptools import Distribution, Extension
from setuptools.command.build_ext import build_ext


def build() -> None:
    # describe your Cython extensions
    extensions = [
        Extension(
            "*",
            ["src/kvxdb/core/*.pyx"],  # adjust if paths change
            # extra_compile_args=["-O3"],         # optional
            # extra_link_args=[],
            # include_dirs=[],
            # libraries=[],
        )
    ]

    ext_modules = cythonize(
        extensions,
        language_level=3,
        compiler_directives={"binding": True},  # optional but handy
    )

    # run setuptools' build_ext in an isolated dist
    dist = Distribution({"name": "kvxdb", "ext_modules": ext_modules})
    cmd = build_ext(dist)
    cmd.ensure_finalized()
    cmd.run()

    # copy built .so files back into src/… so Poetry can pick them up
    for output in cmd.get_outputs():
        output = Path(output)
        dest = Path("src") / output.relative_to(cmd.build_lib)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(output, dest)

        # make sure copied files are readable (preserve + add read bits)
        st = os.stat(dest)
        os.chmod(dest, st.st_mode | ((st.st_mode & 0o444) >> 2))


if __name__ == "__main__":
    build()
