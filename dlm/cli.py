"""Command-line client:  python -m dlm.cli --addrs host1:50051,host2:50051 acquire my-lock"""

from __future__ import annotations

import argparse
import sys

from .client import DLMClient, DLMError, Lease


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="dlm")
    p.add_argument("--addrs", default="localhost:50051,localhost:50052,localhost:50053")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("acquire", help="try to take a lock")
    a.add_argument("resource")
    a.add_argument("--owner", required=True)
    a.add_argument("--ttl-ms", type=int, default=10_000)
    a.add_argument("--wait", type=float, default=0.0, help="seconds to keep trying")

    r = sub.add_parser("release", help="release a lock you hold")
    r.add_argument("resource")
    r.add_argument("--owner", required=True)
    r.add_argument("--token", type=int, required=True)

    n = sub.add_parser("renew", help="extend a lease")
    n.add_argument("resource")
    n.add_argument("--owner", required=True)
    n.add_argument("--token", type=int, required=True)
    n.add_argument("--ttl-ms", type=int, default=10_000)

    s = sub.add_parser("status", help="show the current holder (local read, may be stale)")
    s.add_argument("resource")

    args = p.parse_args(argv)
    with DLMClient(args.addrs.split(",")) as client:
        try:
            if args.cmd == "acquire":
                lease = client.acquire(args.resource, ttl_ms=args.ttl_ms, owner=args.owner, wait_timeout=args.wait)
                if lease is None:
                    print(f"HELD by {client.status(args.resource)['holder']!r}")
                    return 1
                print(f"ACQUIRED token={lease.token} expires_at_ms={lease.expires_at_ms}")
            elif args.cmd == "release":
                ok = client.release(Lease(args.resource, args.owner, args.token, 0))
                print("RELEASED" if ok else "NOT_HELD (already released, expired, or taken over)")
                return 0 if ok else 1
            elif args.cmd == "renew":
                ok = client.renew(Lease(args.resource, args.owner, args.token, 0), args.ttl_ms)
                print("RENEWED" if ok else "LOST (expired or taken over)")
                return 0 if ok else 1
            else:
                print(client.status(args.resource))
        except DLMError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
