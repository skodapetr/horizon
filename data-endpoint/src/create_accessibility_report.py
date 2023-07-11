#!/usr/bin/env python3
#
# Generate report with SPARQL endpoint accessibility.
#
import enum
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

TIMEOUT_SECOND = 5

TIME_FORMAT = "%Y-%m-%d"

STATUS_UNAVAILABLE = "unavailable"

STATUS_QUERY_FAILED = "invalid"

STATUS_AVAILABLE = "available"


def read_arguments() -> dict[str, str]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sparql-endpoints",
        required=True,
        help="Path or URL to a file with endpoints.",
    )
    parser.add_argument(
        "--output-directory",
        required=True,
        help="Path to directory where to write output.",
    )
    parser.add_argument(
        "--symlink",
        action="store_true",
        help="Create symlink to created report. Use this to have stable file for latest results.",
    )
    return vars(parser.parse_args())


def main(args):
    initialize_logging()
    endpoints = load_endpoints(args["sparql_endpoints"])
    availability = test_endpoints_availability(endpoints)
    report_file = write_report(availability, args["output_directory"])
    if args["symlink"]:
        symlink_report(args["output_directory"], report_file)


def initialize_logging():
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )


def load_endpoints(path: str):
    with open_stream(path) as stream:
        content = json.load(stream)
    base = content["@context"]["@base"]
    return [
        {
            # Resource.
            "@id": base + item["@id"],
            # Relative, use to construct report URL.
            "relative": item["@id"],
            # SPARQL Endpoint URL.
            "url": item["url"],
        }
        for item in content["endpoint"]
    ]


def open_stream(path: str):
    if path.startswith("http"):
        return url_as_lines(path)
    else:
        return open(path, encoding="utf-8", newline="")


@contextlib.contextmanager
def url_as_lines(url: str):
    with urllib.request.urlopen(url) as response:
        lines = [line.decode("utf-8") for line in response.readlines()]
        yield lines


def test_endpoints_availability(endpoints):
    result = []
    for index, endpoint in enumerate(endpoints):
        url = endpoint["url"]
        available, can_query = test_endpoint(url)
        if not available:
            status = STATUS_UNAVAILABLE
        elif not can_query:
            status = STATUS_QUERY_FAILED
        else:
            status = STATUS_AVAILABLE
        logging.info(f"{index + 1}/{len(endpoints)} '{url}' : {status}")
        result.append({**endpoint, "status": status})
        if index > 3:
            break
    return result


def test_endpoint(url: str) -> tuple[bool, bool]:
    """Using a simple ASK query test the endpoint."""
    endpoint = SPARQLWrapper(url)
    endpoint.setTimeout(TIMEOUT_SECOND)
    endpoint.setQuery("SELECT ?s ?p ?o WHERE{ ?s ?p ?o } LIMIT 1")
    endpoint.setReturnFormat(JSON)
    try:
        response = endpoint.query().convert()
    except:
        return False, False
    if not type(response) == dict or "boolean" not in response:
        return True, False
    return True, True


def write_report(endpoints: list, directory: str) -> str:
    today = datetime.datetime.today()

    availability_map = {}
    last_date = None
    last_report = load_json(os.path.join(directory, "sparql.json"))
    if last_report is not None:
        last_date = last_report["metadata"]["created"]
        logging.info(f"Loaded last report from {last_date}.")
        availability_map = {
            report_item["@id"]: report_item for report_item in last_report["report"]
        }

    report_items = []
    for endpoint in endpoints:
        report_item = {
            "@id": endpoint["relative"],
            "endpoint": endpoint["@id"],
            "url": endpoint["url"],
            "status": endpoint["status"],
        }
        # Add information about last time we see in online.
        if not endpoint["status"] == STATUS_AVAILABLE:
            last_available = None
            last_report = availability_map.get(report_item["@id"], None)
            if last_report is not None:
                if last_report["status"] == STATUS_AVAILABLE:
                    last_available = last_date
                else:
                    last_available = last_report.get("lastAvailable", None)
            if last_available is not None:
                report_item["lastAvailable"] = last_available
        # Store to item list.
        report_items.append(report_item)

    report = {
        "@context": {
            "@base": "https://skodapetr.github.io/horizon/v1/data/sparql/2023-07-11/",
            "@vocab": "https://skodapetr.github.io/horizon/v1/schema#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "void": "http://rdfs.org/ns/void#",
            "url": {"@id": "void:sparqlEndpoint", "@type": "@id"},
            "endpoint": {"@type": "@id"},
            "lastAvailable": {"@type": "xsd:date"},
            "status": {
                "@type": "@id",
                "@context": {
                    "@base": "https://skodapetr.github.io/horizon/v1/resource/SparqlStatus/"
                },
            },
            "created": {
                "@type": "xsd:date",
                "@id": "dcterms:created",
            },
        },
        "@id": "",
        "metadata": {
            "@id": "",
            "created": today.strftime(TIME_FORMAT),
            "timeoutSecond": TIMEOUT_SECOND,
            "version": 1,
        },
        "report": report_items,
    }
    os.makedirs(directory, exist_ok=True)
    output_path = report_path(directory, today)
    write_json(output_path, report)
    return output_path


def report_path(directory: str, date: datetime.datetime):
    file_name = f"sparql-{date.strftime(TIME_FORMAT)}.json"
    return os.path.join(directory, file_name)


def load_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as stream:
        return json.load(stream)


def write_json(path: str, content):
    with open(path, "w", encoding="utf-8") as stream:
        json.dump(content, stream, ensure_ascii=False, indent=2)


def symlink_report(directory: str, target_path: str) -> None:
    """Update symlink to newly created file."""
    symlink = pathlib.Path(directory) / "sparql.json"
    symlink.unlink(missing_ok=True)
    symlink.symlink_to(target_path)


if __name__ == "__main__":
    main(read_arguments())
