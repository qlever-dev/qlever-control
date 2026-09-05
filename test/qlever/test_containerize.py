import os

from qlever.containerize import Containerize


def _cmd(**kwargs) -> str:
    return Containerize.containerize_command(
        "echo hello",
        "docker",
        "run -d",
        "adfreiburg/qlever",
        "qlever.server.test",
        volumes=[("$(pwd)", "/index")],
        ports=[(7001, 7001)],
        working_directory="/index",
        **kwargs,
    )


# Without a seccomp profile (the default), no `--security-opt` is added, so
# the container engine's default profile applies.
def test_containerize_command_without_seccomp_profile():
    cmd = _cmd()
    assert "--security-opt" not in cmd
    assert "seccomp" not in cmd
    cmd = _cmd(seccomp_profile=None)
    assert "--security-opt" not in cmd


# With a seccomp profile, `--security-opt seccomp=<abspath>` is added, where
# a relative path is made absolute w.r.t. the current working directory.
def test_containerize_command_with_seccomp_profile():
    cmd = _cmd(seccomp_profile="/etc/qlever/seccomp.json")
    assert " --security-opt seccomp=/etc/qlever/seccomp.json" in cmd
    cmd = _cmd(seccomp_profile="seccomp.json")
    expected = os.path.join(os.getcwd(), "seccomp.json")
    assert f" --security-opt seccomp={expected}" in cmd
    # The option comes after `--init` and before the image name.
    assert cmd.index("--init") < cmd.index("--security-opt")
    assert cmd.index("--security-opt") < cmd.index("adfreiburg/qlever")
