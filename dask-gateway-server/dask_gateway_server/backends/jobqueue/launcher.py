import json
import os
import shutil
import subprocess
import sys


def finish(**kwargs):
    json.dump(kwargs, sys.stdout)
    sys.stdout.flush()


def run_command(cmd, env, stdin=None):
    if stdin is not None:
        stdin = stdin.encode("utf8")
        STDIN = subprocess.PIPE
    else:
        STDIN = None

    proc = subprocess.Popen(
        cmd,
        env=env,
        cwd=os.path.expanduser("~"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=STDIN,
    )

    stdout, stderr = proc.communicate(stdin)

    finish(
        ok=True,
        returncode=proc.returncode,
        stdout=stdout.decode("utf8", "replace"),
        stderr=stderr.decode("utf8", "replace"),
    )


def start(cmd, env, stdin=None, staging_dir=None, files=None):
    if staging_dir:
        try:
            os.makedirs(staging_dir, mode=0o700, exist_ok=False)
            for name, value in files.items():
                with open(os.path.join(staging_dir, name), "w") as f:
                    f.write(value)
        except Exception as exc:
            finish(
                ok=False,
                error=f"Error setting up staging directory {staging_dir}: {exc}",
            )
            return
    run_command(cmd, env, stdin=stdin)


def stop(cmd, env, staging_dir=None):
    if staging_dir:
        if not os.path.exists(staging_dir):
            return
        try:
            shutil.rmtree(staging_dir)
        except Exception as exc:
            finish(
                ok=False,
                error=f"Error removing staging directory {staging_dir}: {exc}",
            )
            return
    run_command(cmd, env)


def main():
    try:
        kwargs = json.load(sys.stdin)
    except ValueError as exc:
        finish(ok=False, error=str(exc))
        return

    action = kwargs.pop("action", None)
    if action == "start":
        start(**kwargs)
    elif action == "stop":
        stop(**kwargs)
    else:
        finish(ok=False, error="Valid actions are 'start' and 'stop'")


if __name__ == "__main__":
    main()
