"""No-browser checks for the Flow worker's process-owned singleton."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


WORKER = Path(__file__).resolve().parents[1] / "static" / "flow_worker.py"
SOURCE = WORKER.read_text(encoding="utf-8")
GUARD_CALL = "if not _acquire_flow_worker_singleton():"
BOOTSTRAP_CALL = "_patchright_ok = _ensure_patchright()"
BUSY_MESSAGE = "Flow worker singleton is already owned; exiting"


class FlowWorkerSingletonTests(unittest.TestCase):
    def test_direct_entry_acquires_before_any_browser_bootstrap(self):
        self.assertTrue(
            GUARD_CALL in SOURCE,
            "flow_worker.py has no process-owned singleton entry guard",
        )
        self.assertLess(SOURCE.index(GUARD_CALL), SOURCE.index(BOOTSTRAP_CALL))
        self.assertIn(BUSY_MESSAGE, SOURCE[:SOURCE.index(BOOTSTRAP_CALL)])

    def test_second_direct_process_exits_cleanly_while_owner_lives(self):
        if GUARD_CALL not in SOURCE:
            self.skipTest("RED: worker has no early singleton guard yet")

        # Run only the real source prefix. It includes the singleton entry guard
        # but stops before dependency install/import or browser-driver bootstrap.
        prefix = SOURCE[:SOURCE.index(BOOTSTRAP_CALL)]
        probe = prefix + (
            "\nimport time as _probe_time\n"
            "print('SINGLETON_OWNER_READY', flush=True)\n"
            "_probe_time.sleep(float(sys.argv[1]))\n"
        )

        with tempfile.TemporaryDirectory() as temp_home:
            probe_path = Path(temp_home) / "singleton_probe.py"
            probe_path.write_text(probe, encoding="utf-8")
            env = os.environ.copy()
            env["HOME"] = temp_home
            env["USERPROFILE"] = temp_home
            env["PYTHONUNBUFFERED"] = "1"

            owner = subprocess.Popen(
                [sys.executable, str(probe_path), "10"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
            try:
                self.assertEqual("SINGLETON_OWNER_READY", owner.stdout.readline().strip())
                contender = subprocess.run(
                    [sys.executable, str(probe_path), "0"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    env=env,
                )
                self.assertEqual(0, contender.returncode)
                self.assertIn(BUSY_MESSAGE, contender.stdout)
                self.assertNotIn("SINGLETON_OWNER_READY", contender.stdout)
            finally:
                owner.terminate()
                owner.wait(timeout=5)
                owner.stdout.close()

            released = subprocess.run(
                [sys.executable, str(probe_path), "0"],
                capture_output=True,
                text=True,
                timeout=5,
                env=env,
            )
            self.assertEqual(0, released.returncode)
            self.assertIn("SINGLETON_OWNER_READY", released.stdout)

    def test_self_update_releases_only_for_replacement_handoff(self):
        start = SOURCE.index("⬆ Updated")
        end = SOURCE.index("# Local-dev override", start)
        handoff = SOURCE[start:end]
        release = handoff.index("_release_flow_worker_singleton()")
        self.assertLess(release, handoff.index("subprocess.Popen"))
        self.assertLess(release, handoff.index("os.execv"))
        self.assertIn("if not _acquire_flow_worker_singleton():", handoff)


if __name__ == "__main__":
    unittest.main()
