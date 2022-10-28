"""Ingest sample data during docker-compose"""
import json
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests

workingdir = Path(__file__).parent.absolute()
data_root = workingdir.parent.parent / "coherence_stac"

app_host = sys.argv[1]

if not app_host:
    raise Exception("You must include full path/port to stac instance")


def post_or_put(url: str, data: dict):
    """Post or put data to url."""
    r = requests.post(url, json=data)
    if r.status_code == 409:
        new_url = url if data["type"] == "Collection" else url + f"/{data['id']}"
        # Exists, so update
        r = requests.put(new_url, json=data)
        # Unchanged may throw a 404
        if not r.status_code == 404:
            r.raise_for_status()
    else:
        r.raise_for_status()


def ingest_data(app_host: str = app_host, data_dir: Path = data_root):
    """ingest data."""
    with open(data_dir / "catalog.json") as f:
        catalog = json.load(f)

    post_or_put(urljoin(app_host, "/catalogs"), catalog)

    collection_paths = data_dir.glob("**/collection.json")
    for collection_path in collection_paths:
        # Add Collection
        with open(collection_path) as f:
            collection = json.load(f)
        post_or_put(urljoin(app_host, f"/catalogs/{catalog['id']}/collection"), collection)

        # Add Sub-Item
        item_paths = collection_path.parent.glob("*/*json")
        for item_path in item_paths:
            with open(item_path) as f:
                item = json.load(f)
            post_or_put(urljoin(app_host, f"/catalogs/{catalog['id']}/collection/{collection['id']}/items"), item)


if __name__ == "__main__":
    ingest_data()
