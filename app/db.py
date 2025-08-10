"""
db.py - module that contains the simplest key-value storage 
with support for key lifetime (TTL).
"""


from typing import Tuple


db: dict[bytes, Tuple[bytes, float | None]] = {}
