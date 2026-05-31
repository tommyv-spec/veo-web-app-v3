"""Fernet symmetric encryption for storing per-user secrets in DB.

Reads FERNET_SECRET env var (base64-url Fernet key, generated via
`cryptography.fernet.Fernet.generate_key()`). Key rotation requires
re-encrypting all stored rows under the new key — script TBD.
"""
import os
from cryptography.fernet import Fernet


def _cipher() -> Fernet:
    key = os.environ.get("FERNET_SECRET")
    if not key:
        raise RuntimeError(
            "FERNET_SECRET env var is required for encryption. "
            "Generate one via `python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"`."
        )
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt(plaintext: str) -> str:
    """Returns the base64-url ciphertext as a Python str."""
    return _cipher().encrypt(plaintext.encode("utf-8")).decode("ascii")


def decrypt(ciphertext: str) -> str:
    """Returns the plaintext str. Raises if the token is invalid or expired."""
    return _cipher().decrypt(ciphertext.encode("ascii")).decode("utf-8")
