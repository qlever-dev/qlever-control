"""
Check the result file of a `benchmark-queries` run on the queries in
`olympics.tsv` or `olympics.yml`. When these queries change, update
`EXPECTED_RESULT_SIZES` too.

Usage: python check_benchmark_result.py <result file> <benchmark name>
"""

import sys

import yaml

EXPECTED_RESULT_SIZES = {"count triples": 1, "ten triples": 10}


def find_errors(result: dict, benchmark_name: str) -> list[str]:
    """The fields of the result file that are missing or wrong."""
    errors = []
    if result.get("name") != benchmark_name:
        errors.append(
            f"name is {result.get('name')!r}, expected {benchmark_name!r}"
        )
    for field in ("timeout", "index_time"):
        if result.get(field) is None:
            errors.append(f"{field} is missing")
    if (result.get("index_size") or 0) <= 0:
        errors.append(f"index_size is {result.get('index_size')!r}")
    queries = result.get("queries", [])
    result_sizes = {
        query["name"]: query.get("result_size") for query in queries
    }
    if result_sizes != EXPECTED_RESULT_SIZES:
        errors.append(
            f"result sizes are {result_sizes}, "
            f"expected {EXPECTED_RESULT_SIZES}"
        )
    for query in queries:
        if query["runtime_info"].get("client_time", 0) <= 0:
            errors.append(f"query {query['name']!r} has no client time")
    return errors


def main() -> None:
    """Print every error of the result file and fail if there is one."""
    result_file_name, benchmark_name = sys.argv[1:]
    with open(result_file_name) as result_file:
        result = yaml.safe_load(result_file)
    errors = find_errors(result, benchmark_name)
    for error in errors:
        print(f"{result_file_name}: {error}")
    if errors:
        sys.exit(1)
    print(f"{result_file_name}: all fields are as expected")


if __name__ == "__main__":
    main()
