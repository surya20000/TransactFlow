import sys
import os
import types

# Make the repo root importable from any test file without per-file sys.path hacks
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# psycopg2 requires native libs — stub it out so tests run locally without Postgres
try:
    import psycopg2  # noqa: F401
except ImportError:
    stub = types.ModuleType("psycopg2")
    sys.modules.setdefault("psycopg2", stub)
