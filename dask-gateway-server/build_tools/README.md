dask-gateway-server includes Golang source code to build the dask-gateway-proxy
binary.

Golang can cross-compile, so if you are on linux/mac/windows, we can compile to
a platform and architecture of choice. See https://freshman.tech/snippets/go/cross-compile-go-programs/

1. build --sdist --wheel (pure-python aka. py3-none)
2. compile golang for linux and mac
3. inject goland binary to wheel


Question 1: When we compile the golang code to a binary, how broad is its
            compatibility? It would be great to have "all linux" and "all mac".
Question 2: When we augment the wheel to include the golang binary, how do we do it and how do we name the wheel? Should it be "linux_x86_64", "linux_aarch64", "darwin_x86_64", "darwin_aarch64"?

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
