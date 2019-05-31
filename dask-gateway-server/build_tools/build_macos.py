import glob
import os
import shutil
import subprocess
import sys
import tempfile


PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST_DIR = os.path.join(PACKAGE_DIR, "dist")


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def main():
    sdists = glob.glob(os.path.join(DIST_DIR, "*.tar.gz"))
    if not sdists:
        fail("Must build sdist beforehand")
    elif len(sdists) > 1:
        fail("Must have only one sdist built")

    sdist = sdists[0]
    env = os.environ.copy()
    env.update({"GOOS": "darwin", "GOARCH": "amd64"})

    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.check_call(
            ["pip", "wheel", "--no-deps", "--wheel-dir", tmpdir, sdist], env=env
        )
        wheels = glob.glob(os.path.join(tmpdir, "*.whl"))
        if len(wheels) != 1:
            fail("Should only be one newly created wheels")
        wheel = wheels[0]
        fn = os.path.basename(wheel)
        parts = fn.split("-")
        assert parts[-1] == "any.whl"
        parts[-1] = "macosx_10_6_x86_64.whl"
        renamed = os.path.join(DIST_DIR, "-".join(parts))
        shutil.move(wheel, renamed)


if __name__ == "__main__":
    main()
