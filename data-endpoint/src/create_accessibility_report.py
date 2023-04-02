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
import urllib.request
import contextlib 
import pathlib

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
        help="Path to CSV file, or URL, with SPARQL endpoints.",
    )
    parser.add_argument(
        "--output-directory",
        required=True,
        help="Path to directory where to write output.",
    )
    parser.add_argument(
        "--symlink",
        action="store_true",
        help="Create symlink to output file.",
    )
    return vars(parser.parse_args())


def main(args):
    initialize_logging()
    endpoints = list_endpoints_from_csv(args["sparql_endpoints"])
    availability = test_endpoint_availability(endpoints)
    file_name = write_report(availability, args["output_directory"])
    if args["symlink"]:
        symlink_report(args["output_directory"], file_name)



def initialize_logging():
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )


def list_endpoints_from_csv(path: str):
    # Ignore duplicit records.
    visited = set()
    with open_stream(path) as stream:
        reader = csv.reader(stream)
        next(reader)
        for row in reader:
            url = row[1]
            if url in visited:
                continue
            else:
                yield url

def open_stream(path:str):
    if path.startswith("http"):
        return url_as_lines(path)
    else:
        return open(path, encoding="utf-8", newline="")

@contextlib.contextmanager
def url_as_lines(url:str):
    with urllib.request.urlopen(url) as response:
        lines = [line.decode("utf-8") for line in response.readlines()]
        yield lines


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


def write_report(endpoints: list[ReportItem], directory: str) -> str:
    today_as_string = datetime.datetime.today().strftime("%Y-%m-%d")
    report: Report = {
        "metadata": {"date": today_as_string, "timeout": TIMEOUT_SECOND},
        "data": endpoints,
    }
    file_name = f"{today_as_string}-sparql-available.json"
    os.makedirs(directory, exist_ok=True)
    write_json(os.path.join(directory, file_name), report)
    return file_name


def write_json(path: str, content):
    with open(path, "w", encoding="utf-8") as stream:
        json.dump(content, stream, ensure_ascii=False, indent=2)


def symlink_report(directory:str, file_name: str) -> None:
    """Update symlink to newly created file."""
    source = pathlib.Path(directory) / file_name
    destination = pathlib.Path(directory) / "sparql-available.json"
    os.remove(destination)
    source.symlink_to(destination)

if __name__ == "__main__":
    main(_read_arguments())
