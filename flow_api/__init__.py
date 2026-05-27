"""flow_api — drive Google Flow via its private JSON API from inside the logged-in
Patchright page, instead of clicking DOM and scraping data-index tiles.

Attribution is by media_id UUID read from the submit response, so a clip can never
be mis-attributed to the wrong tile (the recurring DOM-scrape bug).

Endpoints/keys/model-keys ported from the FlowKit reference (crisng95/flowkit) and
cross-checked against useapi.net's documented Flow API. Values that still need a
live-traffic confirmation for THIS account are marked NEEDS_CAPTURE in config.py;
run capture_helper.py to fill them before enabling FLOW_API_MODE=on.
"""
