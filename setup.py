import textwrap

from setuptools import setup

setup(
    use_scm_version={
        "write_to": "src/sodaspark/version.py",
        "write_to_template": textwrap.dedent(
            """
             from __future__ import unicode_literals


             __version__ = {version!r}
             """,
        ).lstrip(),
    },
    package_dir={"": "src"},
)
