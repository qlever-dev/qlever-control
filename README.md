# QLever

This repository provides a self-documenting and easy-to-use command-line tool
for QLever (pronounced "Clever"), a graph database implementing the
[RDF](https://www.w3.org/TR/rdf11-concepts/) and
[SPARQL](https://www.w3.org/TR/sparql11-overview/) standards. 
For a detailed description of what QLever is and what it can do, see 
[here](https://github.com/ad-freiburg/qlever).

# Installation and Usage

See the [official documentation](https://docs.qlever.dev/quickstart/) for
installation and usage instructions. There are native packages available for
[Debian and Ubuntu](https://docs.qlever.dev/quickstart/#debian-and-ubuntu) as
well as [macOS](https://docs.qlever.dev/quickstart/#macos-apple-silicon). On
other platforms QLever is only available via Docker and the `qlever` command-line
tool has to be [installed with `pipx`/`uv`](https://docs.qlever.dev/quickstart/#others).

# Use on Windows

By default, `qlever` uses [QLever's official Docker
image](https://hub.docker.com/r/adfreiburg/qlever). In principle, that image
runs on Linux, macOS, and Windows. On Linux, Docker runs natively
and incurs only a relatively small overhead regarding performance and RAM
consumption. On macOS and Windows, Docker runs in a virtual machine, which
incurs a significant and sometimes unpredictable overhead. For example, `qlever
index` might abort prematurely (without a proper error message) because the
virtual machine runs out of RAM.

For optimal performance, use the [native packages](https://docs.qlever.dev/quickstart/#installing-qlever)
or compile QLever from source on your machine. For Linux, compiling is relatively
straightforward: just follow the `RUN` instructions in the
[Dockerfile](https://github.com/ad-freiburg/qlever/blob/master/Dockerfile). For
macOS, this is more complicated, see [this workflow](https://github.com/ad-freiburg/qlever/blob/master/.github/workflows/macos.yml).

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
`src/qlever/commands`. The executable created by `pip` simply links and refers
to the files in your working copy.

If you have bug fixes or new useful features or commands, please open a pull
request. If you have questions or suggestions, please open an issue.
