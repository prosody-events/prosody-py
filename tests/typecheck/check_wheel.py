"""Verify that a built wheel contains Prosody's PEP 561 typing artifacts."""

from pathlib import Path
import sys
from zipfile import ZipFile


REQUIRED = {
    "prosody/__init__.pyi",
    "prosody/context.pyi",
    "prosody/errors.pyi",
    "prosody/handler.pyi",
    "prosody/prosody.pyi",
    "prosody/py.typed",
    "prosody/state.pyi",
}


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_wheel.py WHEEL")

    wheel = Path(sys.argv[1])
    with ZipFile(wheel) as archive:
        names = set(archive.namelist())

    missing = sorted(REQUIRED - names)
    if missing:
        raise SystemExit(f"{wheel} is missing typing artifacts: {', '.join(missing)}")


if __name__ == "__main__":
    main()
