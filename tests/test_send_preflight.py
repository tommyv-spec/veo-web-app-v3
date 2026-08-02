# -*- coding: utf-8 -*-
# Standalone: python tests/test_send_preflight.py  (run from code/)
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import send_to_platform as stp

GOOD = '''
## Images

### Image 1
- **Image prompt:**
```
Use the uploaded character reference image for the main character. Talking head. Aspect ratio 9:16.
```

## Storyboard

### Scene 1
- **image:** image_1
- **line:** the number one anti-inflammatory compound is not turmeric
'''

BAD_NO_SCENES = '''
## Images

### Image 1
- **Image prompt:**
```
A kitchen. Aspect ratio 9:16.
```
'''

ok, err = stp.preflight_text(GOOD)
assert ok, err
ok, err = stp.preflight_text(BAD_NO_SCENES)
assert not ok
assert "no '### Scene N' storyboard blocks" in err or "No scenes found" in err, err

# error normalization
class FakeResp:
    def __init__(self, status, body): self.status_code = status; self._b = body; self.text = str(body)
    def json(self): return self._b

msg, det = stp.normalize_detail(FakeResp(400, {"detail": "Parse error: X"}))
assert msg == "Parse error: X" and det is None
msg, det = stp.normalize_detail(FakeResp(409, {"detail": {"error": "duplicate_batch_name", "message": "A batch named 'X' already exists. Pick a different name."}}))
assert det["error"] == "duplicate_batch_name" and "already exists" in msg

e = stp.classify_http_error(FakeResp(401, {"detail": "Not authenticated"}))
assert e.exit_code == stp.EXIT_AUTH
e = stp.classify_http_error(FakeResp(409, {"detail": {"error": "duplicate_batch_name", "message": "x"}}))
assert e.exit_code == stp.EXIT_DUPLICATE
e = stp.classify_http_error(FakeResp(409, {"detail": {"error": "Not all scenes are ready to promote", "missing_count": 2}}))
assert e.exit_code == stp.EXIT_NOT_READY
e = stp.classify_http_error(FakeResp(400, {"detail": "Parse error: Scene 3: missing image"}))
assert e.exit_code == stp.EXIT_PARSE
e = stp.classify_http_error(FakeResp(400, {"detail": "Ingredient(s) with type=character/product declare a Reference path... Unresolved ingredients:\n  • 'Donna'"}))
assert e.exit_code == stp.EXIT_INGREDIENT
e = stp.classify_http_error(FakeResp(500, {"detail": "Import failed due to a server-side error: KeyError: 'x'"}))
assert e.exit_code == stp.EXIT_UNKNOWN
print("test_send_preflight: ALL PASS")
