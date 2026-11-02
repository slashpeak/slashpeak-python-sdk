from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from slashpeak._version import __version__
from slashpeak.doctor import run_doctor


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="slashpeak", description="SlashPeak Job Router tools.")
    parser.add_argument("--version", action="version", version=f"slashpeak {__version__}")
    commands = parser.add_subparsers(dest="command")
    doctor = commands.add_parser("doctor", help="check the key, the network path and reservation access")
    doctor.add_argument("--latency", action="store_true", help="also measure 20 warm round trips")
    args = parser.parse_args(argv)
    if args.command == "doctor":
        return run_doctor(latency=args.latency)
    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
