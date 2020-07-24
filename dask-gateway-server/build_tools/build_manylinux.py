import glob
import os
import subprocess
import sys

GOLANG = "https://storage.googleapis.com/golang/go{}.linux-amd64.tar.gz"

SCRIPT = """\
cd /tmp
curl {golang} --silent --location | tar -xz
export PATH="/tmp/go/bin:$PATH" HOME=/tmp
for py in {pythons}; do
    "/opt/python/$py/bin/pip" wheel --no-deps --wheel-dir /tmp /dist/*.tar.gz
done
ls *.whl | xargs -n1 --verbose auditwheel repair --wheel-dir /dist
ls -al /dist
"""

PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIST_DIR = os.path.join(PACKAGE_DIR, "dist")


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def main():
    go_version = "1.14"
    python_versions = "cp37-cp37m"

    golang = GOLANG.format(go_version)
    pythons = " ".join(python_versions.split(","))

    sdists = glob.glob(os.path.join(DIST_DIR, "*.tar.gz"))
    if not sdists:
        fail("Must build sdist beforehand")
    elif len(sdists) > 1:
        fail("Must have only one sdist built")

    subprocess.check_call(
        [
            "docker",
            "run",
            "-it",
            "--rm",
            "--volume",
            "{}:/dist:rw".format(DIST_DIR),
            "--user",
            "{}:{}".format(os.getuid(), os.getgid()),
            "quay.io/pypa/manylinux1_x86_64:latest",
            "bash",
            "-o",
            "pipefail",
            "-euxc",
            SCRIPT.format(golang=golang, pythons=pythons),
        ]
    )


if __name__ == "__main__":
    main()
