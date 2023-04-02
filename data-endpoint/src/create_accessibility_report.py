#!/usr/bin/env python3
#
# Generate report with SPARQL endpoint accessibility.
#
import os
import csv
import json
import logging
import datetime
import argparse
import typing

from SPARQLWrapper import SPARQLWrapper, JSON

TIMEOUT_SECOND = 3

ReportItem = typing.TypedDict(
    "ReportItem",
    {
        "endpoint": str,
        "available": bool,
    },
)

Report = typing.TypedDict(
    "Report",
    {
        "metadata": typing.TypedDict("ReportMetadata", {"date": str, "timeout": int}),
        "data": list[ReportItem],
    },
)


def _read_arguments() -> dict[str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sparql-endpoints",
        required=True,
        help="Path to CSV file with SPARQL endpoints.",
    )
    parser.add_argument(
        "--output-directory",
        required=True,
        help="Path to directory where to write output.",
    )
    return vars(parser.parse_args())


def main(args):
    _initialize_logging()
    endpoints_generator = list_endpoints_from_csv_file(args["sparql_endpoints"])
    availability = test_endpoint_availability(endpoints_generator)
    write_report(availability, args["output_directory"])


def _initialize_logging():
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )


def list_endpoints_from_csv_file(path: str):
    # Ignore duplicit records.
    visited = set()
    with open(path, encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        next(reader)
        for row in reader:
            url = row[1]
            if url in visited:
                continue
            else:
                yield url


def test_endpoint_availability(endpoints) -> list[ReportItem]:
    result = []
    for url in endpoints:
        available = test_endpoint(url)
        if available:
            logging.info("'%s' is available.", url)
        else:
            logging.info("'%s' is unavailable.", url)
        result.append({"endpoint": url, "available": available})
    return result


def test_endpoint(url: str) -> bool:
    """Using a simple ASK query test the endpoint."""
    endpoint = SPARQLWrapper(url)
    endpoint.setTimeout(TIMEOUT_SECOND)
    endpoint.setQuery("ASK WHERE { ?s ?p ?o }")
    endpoint.setReturnFormat(JSON)
    try:
        endpoint.query().convert()
        return True
    except:
        return False


def write_report(endpoints: list[ReportItem], directory: str):
    today_as_string = datetime.datetime.today().strftime("%Y-%m-%d")
    report: Report = {
        "metadata": {"date": today_as_string, "timeout": TIMEOUT_SECOND},
        "data": endpoints,
    }
    file_name = f"{today_as_string}-sparql-available.json"
    os.makedirs(directory, exist_ok=True)
    _write_json(os.path.join(directory, file_name), report)


def _write_json(path: str, content):
    with open(path, "w", encoding="utf-8") as stream:
        json.dump(content, stream, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main(_read_arguments())
