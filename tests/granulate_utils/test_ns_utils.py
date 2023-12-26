import os
from pathlib import Path

from pytest import TempdirFactory

from granulate_utils.linux.ns import resolve_proc_root_links


# Here for comparison purposes
def resolve_proc_root_links_old(proc_root: str, ns_path: str) -> str:
    """
    Resolves "ns_path" which (possibly) resides in another mount namespace.

    If ns_path contains absolute symlinks, it can't be accessed merely by /proc/pid/root/ns_path,
    because the resolved absolute symlinks will "escape" the /proc/pid/root base.

    To work around that, we resolve the path component by component; if any component "escapes", we
    add the /proc/pid/root prefix once again.
    """
    assert ns_path[0] == "/", f"expected {ns_path!r} to be absolute"
    parts = Path(ns_path).parts

    path = proc_root
    seen = set()
    for part in parts[1:]:  # skip the / (or multiple /// as .parts gives them)
        next_path = os.path.join(path, part)
        while os.path.islink(next_path):
            if next_path in seen:
                raise RuntimeError("Symlink loop from %r" % os.path.join(path, part))
            seen.add(next_path)
            link = os.readlink(next_path)
            if os.path.isabs(link):
                # absolute - prefix with proc_root
                next_path = proc_root + link
            else:
                # relative: just join
                next_path = os.path.join(os.path.dirname(next_path), link)
        path = next_path

    return path


def test_resolve_proc_root_links_compound_links(tmpdir_factory: TempdirFactory):
    """
    We construct the following case:
    {tmpdir}/link
        link -> {tmpdir}/a/c
        a -> {tmpdir}/b
    Eventually we expect {tmpdir}/link to resolve to {tmpdir}/b/c
    """
    tmpdir = Path(tmpdir_factory.mktemp("tmpdir"))

    link = tmpdir.joinpath("link")
    link.symlink_to(tmpdir / "a" / "c", target_is_directory=True)

    a = tmpdir / "a"
    a.symlink_to(tmpdir / "b", target_is_directory=True)

    expected_resolved_path = tmpdir / "b" / "c"
    expected_resolved_path.mkdir(parents=True)

    # Make sure we got the directory structure correct
    assert link.resolve() == expected_resolved_path

    # Make sure resolve_proc_root_links got it correct as well
    proc_root = "/proc/self/root"
    assert resolve_proc_root_links(proc_root, str(link)) == proc_root + str(expected_resolved_path)

    # Make sure the old implementation got it wrong
    assert resolve_proc_root_links_old(proc_root, str(link)) == proc_root + str(a / "c")


def test_resolve_proc_root_links_relative_compound_links(tmpdir_factory: TempdirFactory):
    """
    We construct the following case:
    {tmpdir}/link
        link -> a/c
        a -> b
    Eventually we expect {tmpdir}/link to resolve to {tmpdir}/b/c
    """
    tmpdir = Path(tmpdir_factory.mktemp("tmpdir"))

    link = tmpdir.joinpath("link")
    link.symlink_to("a/c", target_is_directory=True)

    a = tmpdir / "a"
    a.symlink_to("b", target_is_directory=True)

    expected_resolved_path = tmpdir / "b" / "c"
    expected_resolved_path.mkdir(parents=True)

    # Make sure we got the directory structure correct
    assert link.resolve() == expected_resolved_path

    # Make sure resolve_proc_root_links got it correct as well
    proc_root = "/proc/self/root"
    assert resolve_proc_root_links(proc_root, str(link)) == proc_root + str(expected_resolved_path)

    # Make sure the old implementation got it wrong
    assert resolve_proc_root_links_old(proc_root, str(link)) == proc_root + str(a / "c")
