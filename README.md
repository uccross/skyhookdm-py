# Overview

This repository is for the python package skyhookdm-py. This package contains the beginnings of a
python interface for [SkyhookDM][project-skyhook], which *stores and manages tabular data* in the
[Ceph][project-ceph] object storage system.

The python library is simply `skyhookdm`, and the source code is available in the aptly named
directory.

# Development Plans

At this moment, this repository is not the official python API for SkyhookDM. But, this repository
is hopefully where [my implementation][repo-declmercantile] will be coalesced with the [existing
python API for SkyhookDM][repo-skyhookpy].

# Development Environment

Dependency management for this repository is handled by [poetry][tool-poetry]. The number of
dependencies is purposefully kept to a minimum at the moment, and there are a few dependencies that
must be kept carefully in sync with Skyhook:
* [Apache Arrow][project-arrow] ([pyarrow][docs-pyarrow])
* [Flatbuffers][project-flatbuffers]

The reason for this is that these dependencies involve data formats, and some versions can
deprecate features or break backwards compatibility. While this isn't going to be the case for
every update, minor version updates should still be tested before transitioning to them.

Python version management is handled by [pyenv][tool-pyenv]. The main purpose is simply to make it
easy to isolate potentiall differences between python versions, but there is a secondary, very
important reason for needing to explicitly manage the python version: compatibility with the
`rados` library.  The storage subsystem used by Ceph is called [RADOS][article-rados], and the
[python rados library][docs-libradospy] is a binding to the librados code. As of this writing, the
newest version of python that librados supports is python 3.6.9 (to my knowledge). Also, newer
versions of numpy and arrow have dependencies on python >= 3.8, so the environment can be tricky to
change in some cases.

Finally, [Flake8][docs-flake8] is the python linter that my environment is setup to use, and so I
have included a .flake8 configuration file in this repository that holds my particular settings.

### Other dev configuration

I personally use VIM for my editor, and [here is my vim configuration][repo-configs]. I use flake8
with the syntastic VIM plugin, in case you're wondering how I use flake8.

<!-- Resources -->
[project-skyhook]:     https://sites.google.com/view/skyhook-programmable-storage
[project-ceph]:        https://ceph.io/ceph-storage/
[project-arrow]:       https://arrow.apache.org/
[project-flatbuffers]: https://google.github.io/flatbuffers/

[repo-skyhookpy]:      https://github.com/uccross/skyhookdm-pythonclient
[repo-declmercantile]: https://github.com/drin/decl-mercantile.git
[repo-configs]:        https://github.com/drin/configs/tree/master/vim

[tool-poetry]:         https://python-poetry.org/
[tool-pyenv]:          https://github.com/pyenv/pyenv

[docs-pyarrow]:        https://arrow.apache.org/docs/python/
[docs-libradospy]:     https://docs.ceph.com/docs/master/rados/api/python/
[docs-flake8]:         https://flake8.pycqa.org/en/latest/

[article-rados]:       https://ceph.io/geen-categorie/the-rados-distributed-object-store/
