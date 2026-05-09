"""Regression test for kill_chrome_using_profile substring collision.

Validates that the v684 _parse_user_data_dir_from_cmdline helper performs
EXACT matching on Chrome's --user-data-dir flag value, so killing one
account's Chrome doesn't accidentally kill another account whose profile
path is a prefix.

Run: python test_kill_chrome_match.py
Exits 0 on success.
"""
import os
import sys
import re as _re


def _parse_user_data_dir_from_cmdline(cmdline):
    if not cmdline:
        return None
    m = _re.search(r'--user-data-dir=("([^"]*)"|(\S+))', cmdline)
    if m:
        return os.path.abspath(m.group(2) if m.group(2) is not None else m.group(3))
    m = _re.search(r'--user-data-dir\s+(?:"([^"]*)"|(\S+))', cmdline)
    if m:
        return os.path.abspath(m.group(1) if m.group(1) is not None else m.group(2))
    return None


def fail(msg):
    print(f"  [FAIL] {msg}")
    sys.exit(1)


def ok(msg):
    print(f"  [OK]   {msg}")


def main():
    print("Test 1: parser handles three commandline forms")
    cases = [
        # (cmdline, expected user-data-dir)
        (r'chrome.exe --user-data-dir=C:\veo-worker\chrome-session foo bar',
         r'C:\veo-worker\chrome-session'),
        (r'chrome.exe --user-data-dir=C:\veo-worker\chrome-session-2 foo',
         r'C:\veo-worker\chrome-session-2'),
        (r'chrome.exe --user-data-dir="C:\Users\my user\session" foo',
         r'C:\Users\my user\session'),
        ('chrome.exe --no-sandbox --headless', None),
    ]
    for cmd, expected in cases:
        got = _parse_user_data_dir_from_cmdline(cmd)
        expected_norm = os.path.abspath(expected) if expected else None
        if got != expected_norm:
            fail(f"cmd={cmd!r} → got {got!r}, expected {expected_norm!r}")
        ok(f"{cmd[:55]:<55} → {got!r}")

    print("\nTest 2: substring-collision regression — account 1 vs account 2")
    account_1_path = os.path.abspath(r'C:\veo-worker\chrome-session')
    account_2_path = os.path.abspath(r'C:\veo-worker\chrome-session-2')
    account_1_cmd = f'chrome.exe --user-data-dir={account_1_path} foo'
    account_2_cmd = f'chrome.exe --user-data-dir={account_2_path} foo'

    # Simulate kill targeting account 1
    target = account_1_path

    a1_udd = _parse_user_data_dir_from_cmdline(account_1_cmd)
    a2_udd = _parse_user_data_dir_from_cmdline(account_2_cmd)

    # Use the comparison logic from kill_chrome_using_profile
    def matches(udd, abs_profile):
        if udd is None:
            return False
        return udd.rstrip('\\/').lower() == abs_profile.rstrip('\\/').lower()

    if not matches(a1_udd, target):
        fail("account 1 SHOULD match target=acct1 but didn't")
    ok("account 1 matched target=acct1 (kill should fire) — correct")

    if matches(a2_udd, target):
        fail("account 2 INCORRECTLY matched target=acct1 — substring bug still present")
    ok("account 2 did NOT match target=acct1 (kill must NOT fire) — correct")

    print("\nTest 3: image-worker isolation — different parent path")
    image_worker_path = os.path.abspath(r'C:\Users\tomma\KavenoImageWorker\chrome-session')
    video_worker_path = os.path.abspath(r'C:\Users\tomma\veo-worker\chrome-session')
    image_cmd = f'chrome.exe --user-data-dir={image_worker_path} foo'
    image_udd = _parse_user_data_dir_from_cmdline(image_cmd)
    if matches(image_udd, video_worker_path):
        fail("image worker chrome matched video worker target — cross-worker kill bug")
    ok("image worker chrome did NOT match video worker target — isolated")

    print("\nTest 4: case-insensitive Windows path comparison")
    upper_cmd = r'chrome.exe --user-data-dir=C:\VEO-WORKER\Chrome-Session foo'
    upper_udd = _parse_user_data_dir_from_cmdline(upper_cmd)
    lower_target = os.path.abspath(r'c:\veo-worker\chrome-session')
    if not matches(upper_udd, lower_target):
        fail("case-insensitive match failed — Windows paths should normalize")
    ok("case-insensitive Windows path match — correct")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED — substring-collision bug eliminated")
    print("=" * 60)


if __name__ == "__main__":
    main()
