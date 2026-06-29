# Overview
In this directory (./sparql_conformance) lives code relevant
to execute the sparql conformance tests.

Code from the conformance tool https://github.com/ad-freiburg/sparql-conformance 
was copied in the directory.

Branch is based on https://github.com/qlever-dev/qlever-control/pull/190.

Only code under ./engines/ interacts with commands from other engines.

In ./commands are commands that can be executed using `sparql_conformance <command> -h`.


If the code for sparql conformance should live inside qlever-control, 
then logic for engine commands should be seperated from the commands executed 
in the cli by the user.
This would allow the CLI commands and the sparql conformance tool to use the
engine specific commands as they need it.

## Usage
```bash
# Quick execution for qlever
mkdir qlever
sparql_conformance setup qlever
sparql_conformance test 

# Same for jena, for each new QLeverfile we need a new directory
mkdir jena
sparql_conformance setup jena
sparql_conformance test

# Example only testing one test group: aggregates
mkdir qlever
sparql_conformance setup qlever
sparql_conformance test --include aggregates

# Example inspecting the engine state after loading test data:
mkdir qlever
sparql_conformance setup qlever
sparql_conformance analyze "COUNT 1"

# To visualize result files
sparql_conformance visualize
```

## How to add a new engine
When a new engine is being added to qlever-control it does not automatically work
with sparql_conformance commands. For that an `EngineManager` who calls the newly implemented commands
is needed.
