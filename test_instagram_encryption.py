"""Fernet encryption round-trip + missing-secret safeguard."""
import os
import importlib.util
import pathlib
import pytest


def _load_encryption():
    spec = importlib.util.spec_from_file_location(
        "encryption", pathlib.Path(__file__).parent / "encryption.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fresh_secret():
    from cryptography.fernet import Fernet
    return Fernet.generate_key().decode()


def test_encrypt_decrypt_round_trip(monkeypatch):
    monkeypatch.setenv("FERNET_SECRET", _fresh_secret())
    enc = _load_encryption()
    ciphertext = enc.encrypt("hello world")
    assert ciphertext != "hello world"
    assert enc.decrypt(ciphertext) == "hello world"


def test_encrypt_raises_without_secret(monkeypatch):
    monkeypatch.delenv("FERNET_SECRET", raising=False)
    enc = _load_encryption()
    with pytest.raises(RuntimeError, match="FERNET_SECRET"):
        enc.encrypt("anything")


def test_decrypt_raises_without_secret(monkeypatch):
    monkeypatch.delenv("FERNET_SECRET", raising=False)
    enc = _load_encryption()
    with pytest.raises(RuntimeError, match="FERNET_SECRET"):
        enc.decrypt("anything")


def test_decrypt_rejects_garbage(monkeypatch):
    monkeypatch.setenv("FERNET_SECRET", _fresh_secret())
    enc = _load_encryption()
    with pytest.raises(Exception):
        enc.decrypt("not-a-fernet-token")
