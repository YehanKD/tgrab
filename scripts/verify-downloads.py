#!/usr/bin/env python3
"""Verify tgrab downloads: find files whose 512 KiB chunks are zero-filled.

A preallocated file that never received its bytes still reports the full size,
so a size check alone calls it complete. This walks each file chunk by chunk
and reports (or removes) the ones with holes.

  verify-downloads.py DIR [DIR...]            # dry run
  verify-downloads.py --delete DIR [DIR...]   # remove corrupt files
"""
import argparse
import os
import sys

CHUNK = 512 * 1024
ZERO = bytes(CHUNK)


def scan(path):
    """Return (chunks, zero_chunks, first_zero_index)."""
    n = z = first = 0
    try:
        size = os.path.getsize(path)
    except OSError:
        return 0, 0, -1
    if size < CHUNK:
        return 0, 0, -1
    with open(path, "rb", buffering=0) as f:
        while True:
            block = f.read(CHUNK)
            if not block:
                break
            if len(block) == CHUNK and block == ZERO:
                z += 1
                if first == 0:
                    first = n + 1
            n += 1
    return n, z, first if z else -1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+")
    ap.add_argument("--delete", action="store_true", help="remove corrupt files")
    ap.add_argument("--quiet", action="store_true", help="summary only")
    args = ap.parse_args()

    total = corrupt = 0
    wasted = 0
    bad = []

    for d in args.dirs:
        if not os.path.isdir(d):
            print(f"skip (not a dir): {d}", file=sys.stderr)
            continue
        for name in sorted(os.listdir(d)):
            p = os.path.join(d, name)
            if not os.path.isfile(p):
                continue
            total += 1
            if name.endswith(".part"):
                bad.append((p, 0, 0, "stale .part"))
                continue
            chunks, zeros, first = scan(p)
            if zeros:
                corrupt += 1
                wasted += zeros * CHUNK
                bad.append((p, zeros, chunks, f"hole from chunk {first}"))

    for p, zeros, chunks, why in ([] if args.quiet else bad):
        size = os.path.getsize(p) / 1e6
        print(f"{os.path.basename(p)[:64]:64s} {size:9.1f} MB  {why}")

    print(f"\nchecked {total} files, {corrupt} corrupt, {wasted/1e9:.2f} GB of zeros")

    if args.delete and bad:
        for p, *_ in bad:
            os.remove(p)
        print(f"deleted {len(bad)} files")
    elif bad:
        print("dry run — rerun with --delete to remove them")


if __name__ == "__main__":
    main()
