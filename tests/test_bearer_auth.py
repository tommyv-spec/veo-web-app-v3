# -*- coding: utf-8 -*-
# Standalone: python tests/test_bearer_auth.py  (run from code/)
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import auth

class FakeQuery:
    def __init__(self, result): self._r = result
    def filter(self, *a, **k): return self
    def first(self): return self._r

class FakeDB:
    def __init__(self, token_row): self._t = token_row
    def query(self, model): return FakeQuery(self._t)

class FakeUser:
    is_active = True
class FakeToken:
    user = FakeUser()

class FakeRequest:
    def __init__(self, header): self.headers = {"authorization": header} if header else {}

# 1. no header -> None
assert auth.validate_bearer_token(FakeRequest(None), FakeDB(None)) is None
# 2. non-bearer header -> None
assert auth.validate_bearer_token(FakeRequest("Basic abc"), FakeDB(None)) is None
# 3. valid bearer + active token/user -> user
u = auth.validate_bearer_token(FakeRequest("Bearer tok123"), FakeDB(FakeToken()))
assert u is FakeToken.user, u
# 4. bearer but token not found -> HTTPException 401
from fastapi import HTTPException
try:
    auth.validate_bearer_token(FakeRequest("Bearer bad"), FakeDB(None))
    assert False, "expected 401"
except HTTPException as e:
    assert e.status_code == 401 and "API token" in e.detail
# 5. inactive user -> 403
class InactiveUser: is_active = False
class TokInactive: user = InactiveUser()
try:
    auth.validate_bearer_token(FakeRequest("Bearer tok"), FakeDB(TokInactive()))
    assert False, "expected 403"
except HTTPException as e:
    assert e.status_code == 403
print("test_bearer_auth: ALL PASS")
