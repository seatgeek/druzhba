Release Proceure
================

*The remainder of this guide is meant to document the Druzhba release process
for Druzhba maintainers. It will be of minimal use to the end user or even most
contributers.*

Versioning system
-----------------

We use a versioning scheme closely related, but simpler than SEMVER. Versions
are numbered ``1.2.3`` or ``1.2.3-rc4`` where 1 is the Major version, 2 is the
Minor version, 3 is the patch version, and 4 is the release candidate version.

Release candidates are only published for minor and major versions.

Patch versions should be transparent to the user except when correcting obvious
bugs that don't require major code changes. They are intended for performance
tweaks, bug fixes, documentation updates, and security updates.

Minor versions include backwards compatible interface changes such as adding
support for a new source database, adding configuration options, or creating new
ways of expressing existing configuration options provided. An end user should
be able to upgrade between minor versions without changing configuration.

Major versions are "breaking" in that they can require configuration changes by
end users.

Before any release, update ``setup.py`` to match the correct version!
TODO: use https://github.com/warner/python-versioneer or similar. Also, please
ensure the integration test suite passes and run `bandit
<https://bandit.readthedocs.io/en/latest/>`_ being sure to manually inspect any
SQL injection warnings that we are not passing unsafe data into those queries.
 
General Branch Outline
----------------------

There are three branches that are relevant to the release procedures: the
`master` branch, the branch for the current minor version (currently `dev-0.1`),
and the branch for the next minor version (`dev-0.2`). These instructions will
use those names to describe the current and next minor branches. These
instructions will not describe procedures for major releases.

Actual releases are indicated by git tags so looking at the ``dev-0.1`` branch
you should see commits labeled with tags ``v0.1.0``, ``v0.1.1``, ``v0.1.2``,
etc. **In these examples we assume ``v0.1.2`` is the current release of
Druzhba.** In this case there could be a no-longer maintained branch with
`v0.0.0`, `v0.0.1`, etc. tags on various commits.

Generally, the branch for the next minor version should be kept up to date with
``master`` until a minor release is imminent at which point there may briefly be
an additional next-next dev branch (``dev-0.3``) that can track master, although
it's fine to leave `master` as that branch until ``0.2.0`` is released.

Between minor release dev process
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Most development work should be targeting the next minor release (in our example
``0.2.0``) and will be PRed against ``master``. After merging to ``master`` the
branch maintainer of ``dev-0.2`` should fast-forward merge ``master`` into
``dev-0.2``

Patch release
-------------

Urgent bug fixes, security patches, etc. and other very minor changes can be
incorporated into a ``0.1.x`` release. To do so, generally, after the PR is
merged to ``master``, you will be able to cherry-pick the PR commit (we're
configured to squash PRs) onto the ``dev-0.1`` branch and then make a new
``0.1.x`` release.

Assuming there are no breaking merge conflicts (discussed below), after the
patch release is released the `dev-0.1` maintainer should (non-ff) merge
`dev-0.1` back into master. This should be an empty merge and operates as an
extra safeguard to ensure no changes get dropped between branches.

.. code-block:: bash

  # These are the steps executed in the console to make a patch release.
  # We assume that we have just merged a PR we want applied to a patch branch
  # to the master branch and its commit hash is 90067de
  git checkout dev-0.1
  git cherry-pick 90067de
  # Now we run unit and integration tests locally, a final time before releasing.
  # It is assumed that those tests are currently passing.
  git tag -a "v0.1.3" -m "Release 0.1.3"
  git push origin "v0.1.3"
  git checkout master
  git merge --no-ff dev-0.1 # if this is possible (see below)

Unfortunately we recognize that sometimes incompatible refactors are made to the
`master` branch in which case the cherry-pick will not work and the equivalent
change will need to be made to the `dev-0.1` branch manually. In that case it is
advisable not to merge `dev-0.1` back into `master`.

Minor release process
---------------------

When we're ready to create a new minor release the ``dev-0.2`` maintainer should
announce internally on slack. From this point on ``master`` can begin working on
version 0.3 features and ``dev-0.2`` should not get fast-forward merged to
master.

Publishing a release candidate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The branch maintainer will tag the current head of `dev-0.2` with `v0.2.0-rc1`
and push that tag. Installations evaluating release candidates should now update
their dependencies to `druzhba==0.2.0-rc1` and allow our job to run for a few
days to ensure no unexpected problems.

Fixing issues in the release candidate
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If there are no problems then this section can be skipped.

Corrections to any bugs discovered in the release candidate should get PRed
against `dev-0.2` directly. When all known issues have been addressed the branch
maintainer can publish another release candidate and merge those changes back
into `master`. This, of course assumes `master` has not diverged so much that
the changes are incompatible, but if they have you're in for some manual fixes
regardless (GL;HF).

.. code-block:: bash

  git checkout dev-0.2
  # Confirm tests pass here
  git tag -a "v0.2.0-rc2" -m "Release Candidate 0.2.0-rc2"
  git push origin "v0.2.0-rc2"
  git checkout master
  git merge --no-ff dev-0.2

Final minor version release
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now that we have a release candidate we're happy with (let's assume `0.2.0-rc2`)
we need to release that as a release and not a candidate.

.. code-block:: bash

  git checkout "v0.2.0-rc2"
  git tag -a "v0.2.0" -m "Release 0.2.0"
  git push origin "v0.2.0"

Installations should now be upgrade to the latest release. We can
also now officially begin work on 0.3.

.. code-block:: bash

  git checkout master
  git checkout -b dev-0.3


New patch version cherry picks can now be added to the `dev-0.2` branch. Patch
releases for the 0.1.x series should only be made if a bug with major
operational risk or security implication is discovered.

Major version release process
-----------------------------

The process to release a new major version is identical to that for a minor
version except that the "next" branch would be `dev-1.0` (instead of `dev-0.2`
in our example above) and after release `master` becomes `dev-1.1`.

Releasing to Pypi
-----------------

In an appropriate Python3 environment, run:

.. code-block:: bash

  pip install -e .[dev]  # For Twine
  python setup.py sdist bdist_wheel
  # Needs envars, or enter a valid user/password or __token__/$TOKEN
  twine upload --verbose --repository testpypi dist/*
