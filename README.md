# QLever

This repository provides a self-documenting and easy-to-use command-line tool
for QLever (pronounced "Clever"), a graph database implementing the
[RDF](https://www.w3.org/TR/rdf11-concepts/) and
[SPARQL](https://www.w3.org/TR/sparql11-overview/) standards. 
For a detailed description of what QLever is and what it can do, see 
[here](https://github.com/ad-freiburg/qlever).

# Installation

Simply do `pip install qlever` and make sure that the directory where `pip`
installs the package is in your `PATH`. Typically, `pip` will warn you when
that is not the case and tell you what to do. If you encounter an "Externally
managed Environment" error, try `pipx` instead of `pip`.

Type `qlever` without arguments to check that the installation worked. When
using it for the first time, you will see a warning at the top with
instructions on how to enable autocompletion. Do it, it makes using `qlever`
so much easier (`pip` cannot do that for you automatically, sorry).

## NOTE: "error: externally-managed-environment"

You may get this error when trying to install on some newer versions of Linux.
When this happens make sure you have pipx installed with `python3 -m pip
install pipx --user`, then do `pipx install qlever`.

# Usage

Create an empty directory, with a name corresponding to the dataset you want to
work with. For the following example, take `olympics`. Go to that directory
and do the following.

```
qlever setup-config olympics   # Get Qleverfile (config file) for this dataset
qlever get-data                # Download the dataset
qlever index                   # Build index data structures for this dataset
qlever start                   # Start a QLever server using that index
qlever query                   # Launch an example query
qlever ui                      # Launch the QLever UI
```

This will create a SPARQL endpoint for the [120 Years of
Olympics](https://github.com/wallscope/olympics-rdf) dataset. It is a great
dataset for getting started because it is small, but not trivial (around 2
million triples), and the downloading and indexing should only take a few
seconds.

Each command will also show you the command line it uses. That way you can
learn, on the side, how QLever works internally. If you just want to know the
command line for a particular command, without executing it, you can append
`--show` like this:

```
qlever index --show
```

There are many more commands and options, see `qlever --help` for general help,
`qlever <command> --help` for help on a specific command, or just use the
autocompletion.

# Use on macOS and Windows

By default, `qlever` uses [QLever's official Docker
image](https://hub.docker.com/r/adfreiburg/qlever). In principle, that image
runs on Linux, macOS, and Windows. On Linux, Docker runs natively
and incurs only a relatively small overhead regarding performance and RAM
consumption. On macOS and Windows, Docker runs in a virtual machine, which
incurs a significant and sometimes unpredictable overhead. For example, `qlever
index` might abort prematurely (without a proper error message) because the
virtual machine runs out of RAM.

For optimal performance, compile QLever from source on your machine. For Linux,
this is relatively straightforward: just follow the `RUN` instructions in the
[Dockerfile](https://github.com/ad-freiburg/qlever/blob/master/Dockerfile). For
macOS, this is more complicated, see [this
workflow](https://github.com/ad-freiburg/qlever/blob/master/.github/workflows/macos.yml).

# Use with your own dataset

To use QLever with your own dataset, you need a `Qleverfile`, like in the
example above. The easiest way to write a `Qleverfile` is to get one of the
existing ones (using `qlever setup-config ...` as explained above) and then
change it according to your needs (the variable names should be
self-explanatory). Pick one for a dataset that is similar to yours and when in
doubt, pick `olympics`.

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
`src/qlever/commands`. The exceutable created by `pip` simply links and refers
to the files in your working copy.

If you have bug fixes or new useful features or commands, please open a pull
request. If you have questions or suggestions, please open an issue.
