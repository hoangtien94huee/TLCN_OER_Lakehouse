#!/usr/bin/env python3
"""Compatibility shim for existing DAG imports.

Use `src.silver.transformer` as the canonical implementation.
"""

try:
    from src.silver.transformer import SilverTransformer, main
except ImportError:
    from silver.transformer import SilverTransformer, main  # type: ignore

__all__ = ["SilverTransformer", "main"]


if __name__ == "__main__":
    main()
