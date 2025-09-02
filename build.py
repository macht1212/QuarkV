# build.py
from __future__ import annotations

import os
import shutil
from pathlib import Path

from Cython.Build import cythonize
from setuptools import Distribution, Extension
from setuptools.command.build_ext import build_ext


def build() -> None:
    # Описываем Cython-расширения
    extensions = [
        Extension(
            "*",
            ["src/kvxdb/core/*.pyx"],
        )
    ]

    ext_modules = cythonize(
        extensions,
        language_level=3,
        compiler_directives={"binding": True},
    )

    # Запускаем build_ext программно
    dist = Distribution({"name": "kvxdb", "ext_modules": ext_modules})
    cmd = build_ext(dist)
    cmd.ensure_finalized()
    cmd.run()

    # Копируем собранные *.so обратно под src/, чтобы их увидел editable-режим
    for output in cmd.get_outputs():
        output_path = Path(output)
        dest = Path("src") / output_path.relative_to(cmd.build_lib)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(output_path, dest)

        # Делает файл читаемым
        st = os.stat(dest)
        os.chmod(dest, st.st_mode | ((st.st_mode & 0o444) >> 2))


if __name__ == "__main__":
    build()
