"""
Create a release.

Source: https://github.com/tox-dev/tox/blob/master/tasks/release.py
"""
import datetime as dt
from pathlib import Path

from git import Commit, Head, Remote, Repo, TagReference
from packaging.version import Version

ROOT_SRC_DIR = Path(__file__).parents[1]
CHANGE_LOG = ROOT_SRC_DIR / "CHANGELOG.md"


def main(version_str: str) -> None:
    version = Version(version_str)
    repo = Repo(str(ROOT_SRC_DIR))

    if repo.is_dirty():
        raise RuntimeError(
            "Current repository is dirty. Please commit any changes and try again."
        )

    upstream, release_branch = create_release_branch(repo, version)
    release_commit = create_release_commit(repo, version)
    tag = tag_release_commit(release_commit, repo, version)
    print("push release commit")
    repo.git.push(upstream.name, release_branch)
    print("push release tag")
    repo.git.push(upstream.name, tag)
    print("All done! ✨ 🍰 ✨")
    print("WARNING: you have switched to the release branch")


def create_release_commit(repo: Repo, version: Version) -> Commit:
    with CHANGE_LOG.open("r") as change_log:
        change_log_content = change_log.read()

    new_change_log_content = (
        f"## [{version}] - {dt.date.today()}\n\n" + change_log_content
    )

    with CHANGE_LOG.open("w") as change_log:
        change_log.write(new_change_log_content)

    changes = change_log_content.split("##", 1)[0]
    repo.index.add((str(CHANGE_LOG),))
    release_commit = repo.index.commit(f"Release {version}\n\n{changes}")
    return release_commit


def create_release_branch(repo: Repo, version: Version) -> tuple[Remote, Head]:
    print("create release branch from upstream main")
    upstream = get_upstream(repo)
    upstream.fetch()
    branch_name = f"release-{version}"
    release_branch = repo.create_head(
        branch_name, upstream.refs.main, force=True
    )
    upstream.push(refspec=f"{branch_name}:{branch_name}", force=True)
    release_branch.set_tracking_branch(
        repo.refs[f"{upstream.name}/{branch_name}"]
    )
    release_branch.checkout()
    return upstream, release_branch


def get_upstream(repo: Repo) -> Remote:
    for remote in repo.remotes:
        for url in remote.urls:
            if url.endswith("sodadata/soda-spark.git"):
                return remote
    raise RuntimeError("could not find sodadata/soda-spark.git remote")


def tag_release_commit(
    release_commit: Commit, repo: Repo, version: Version
) -> TagReference:
    print("tag release commit")
    existing_tags = [x.name for x in repo.tags]
    if version in existing_tags:
        print(f"delete existing tag {version}")
        repo.delete_tag(version)
    print(f"create tag {version}")
    tag = repo.create_tag(version, ref=release_commit, force=True)
    return tag


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(prog="release")
    parser.add_argument("--version", required=True)
    options = parser.parse_args()
    main(options.version)
