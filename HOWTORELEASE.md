# Prerequisites

* Push and merge rights for
  [soda-spark repo](https://github.com/sodadata/soda-spark/branches),
  also referred to as the *upstream*.
* A UNIX system that has:
  - ``git`` able to push to upstream

# Release

Run the release command and make sure you pass in the desired release number:

``` bash
$ python -m pip install gitpython
$ python scripts/release.py
```

Create a pull request and wait until it the CI passes. Now make sure you merge
the PR and delete the release branch. The CI will automatically pick the tag up
and release it, wait to appear in PyPI. Only merge if the later happens.
