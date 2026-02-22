# QLever

This repository provides a self-documenting and easy-to-use command-line tool
for QLever (pronounced "Clever"), a graph database implementing the
[RDF](https://www.w3.org/TR/rdf11-concepts/) and
[SPARQL](https://www.w3.org/TR/sparql11-overview/) standards. 
For a detailed description of what QLever is and what it can do, see 
[here](https://github.com/ad-freiburg/qlever).

# Documentation

View the latest documentation at <https://docs.qlever.dev/quickstart>.

# Installation

There are native packages available for
- [Debian and Ubuntu](https://docs.qlever.dev/quickstart/#debian-and-ubuntu)
- [macOS](https://docs.qlever.dev/quickstart/#macos-apple-silicon)

On other platforms simply install the `qlever` command-line
[python package using `pipx`/`uv`](https://docs.qlever.dev/quickstart/#others).
Note: QLever will be executed in a container which will come with a performance penalty.

# Use with your own dataset

To use QLever with your own dataset, you need a `Qleverfile`, like in the
example above. The easiest way to write a `Qleverfile` is to get one of the
existing ones (using `qlever setup-config ...`) and then
change it according to your needs. Pick one for a dataset that is similar to
yours and when in doubt, pick `olympics`. A
[reference of all options](https://docs.qlever.dev/qleverfile/) is available.

# For developers

The (Python) code for the script is in the `*.py` files in `src/qlever`. The
preconfigured Qleverfiles are in `src/qlever/Qleverfiles`.

If you want to make changes to the script, or add new commands, do as follows:

```
git clone https://github.com/ad-freiburg/qlever-control
cd qlever-control
pip install -e .
```

Then you can use `qlever` just as if you had installed it via `pip install
qlever`. Note that you don't have to rerun `pip install -e .` when you modify
any of the `*.py` files and not even when you add new commands in
`src/qlever/commands`. The executable created by `pip` simply links and refers
to the files in your working copy.

If you have bug fixes or new useful features or commands, please open a pull
request. If you have questions or suggestions, please open an issue.
