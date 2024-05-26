import json
import re
import sys

import requests
from requests.adapters import HTTPAdapter, Retry

re_next_link = re.compile(r'<(.+)>; rel="next"')
retries = Retry(total=5, backoff_factor=0.25, status_forcelist=[500, 502, 503, 504])
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=retries))

def get_next_link(headers):
    if "Link" in headers:
        match = re_next_link.match(headers["Link"])
        if match:
            return match.group(1)

def get_batch(batch_url):
    while batch_url:
        response = session.get(batch_url)
        response.raise_for_status()
        total = response.headers["x-total-results"]
        yield response, total
        batch_url = get_next_link(response.headers)


if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} <taxid>")
    sys.exit(1)

taxid = sys.argv[1]

url = f"https://rest.uniprot.org/uniprotkb/search?format=json&query=%28%28taxonomy_id%3A{taxid}%29%29&size=500"
progress = 0
for batch, total in get_batch(url):
    lines = batch.text.splitlines()
    if not progress:
        print(lines[0])

