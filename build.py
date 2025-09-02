# build.py
from __future__ import annotations

import os
import shutil
from pathlib import Path

from Cython.Build import cythonize
from setuptools import Distribution, Extension
from setuptools.command.build_ext import build_ext


def build() -> None:
    # ВАЖНО: имя расширения с префиксом src.kvxdb.core.*
    extensions = [
        Extension(
            "src.kvxdb.core.*",
            ["src/kvxdb/core/*.pyx"],
        )
    ]

    ext_modules = cythonize(
        extensions,
        language_level=3,
        compiler_directives={"binding": True},
    )

    # Сборка
    dist = Distribution({"name": "kvxdb", "ext_modules": ext_modules})
    cmd = build_ext(dist)
    cmd.ensure_finalized()
    cmd.run()

    # Копируем собранные *.so обратно под src/… (на случай, если build_lib отличается)
    for output in cmd.get_outputs():
        output_path = Path(output)
        try:
            rel = output_path.relative_to(cmd.build_lib)  # например: src/kvxdb/core/respParser.cpython-311-...so
        except ValueError:
            # если путь не под build_lib, формируем вручную
            # берём хвост начиная со "src/..."
            parts = output_path.parts
            if "src" in parts:
                idx = parts.index("src")
                rel = Path(*parts[idx:])  # src/kvxdb/core/...
            else:
                # fallback: кладём в src/kvxdb/core/
                rel = Path("src/kvxdb/core") / output_path.name

        dest = Path("src") / rel.relative_to("src")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(output_path, dest)

        # гарантируем читаемость
        st = os.stat(dest)
        os.chmod(dest, st.st_mode | ((st.st_mode & 0o444) >> 2))


if __name__ == "__main__":
    build()
