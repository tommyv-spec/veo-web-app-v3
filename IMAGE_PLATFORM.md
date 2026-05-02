# Image Platform — setup & usage

This webapp now includes an **Images** tab for node-graph image generation via
the Flow UI worker.

## How it works

```
[Browser: /]
    │
    │  (1) Create node via UI (prompt, settings, parents)
    │
    ▼
Backend (FastAPI)  —  writes {node_<id>.json} to  data/_image_jobs/
                                                        │
                                                        │  (2) watch folder
                                                        ▼
                                         image_worker.py  (Patchright → Flow UI)
                                                        │
                                                        │  (3) writes
                                                        │   {node_<id>.done.json}
                                                        ▼
Backend  —  polls .done.json  →  updates DB  →  UI renders variants
```

## Run locally (two terminals)

**Terminal 1 — the webapp:**

```bash
cd veo-webapp-v359
python main.py
# opens on http://localhost:8000 (or configured port)
```

Find the watch folder path in the UI once you open the Images tab. It will be
something like:

```
<data_dir>/_image_jobs
```

(On Render or production: `/data/_image_jobs`. Locally: typically
`./data/_image_jobs`.)

**Terminal 2 — the image worker:**

```bash
cd veo-webapp-v359
python image_worker.py --watch "<absolute path to data/_image_jobs>"
```

The worker opens Chrome, navigates to Flow, logs in (manually the first time),
and then polls the folder. Each node you generate in the UI becomes a
`node_<id>.json` file that the worker picks up, runs through Flow UI, and
returns as `node_<id>.done.json` with the saved variant file paths.

## Data model

- **ImageNode** — one generation request (or uploaded reference).
  - `kind = "generated"` or `"upload"`
  - `status = draft | queued | generating | ready | failed`
  - `chosen_variant_id` — the variant the user picked
- **ImageVariant** — one of the N variants produced by a generation.
- **ImageEdge** — parent→child link. A child can have up to 3 parents
  (slot 0/1/2), each with an optional `role` label.

Uploaded images are **seed nodes**: `kind=upload`, `status=ready`, one variant
(the uploaded file). They can be used as parents for generated nodes the same
way as other ready nodes.

## API (prefix `/api/images`)

- `GET  /nodes` — list
- `GET  /nodes/{id}` — detail
- `POST /nodes` — create draft (body: prompt, settings, parents[])
- `PATCH /nodes/{id}` — edit while in draft/ready/failed
- `DELETE /nodes/{id}` — delete (refuses if node has children)
- `POST /nodes/{id}/generate` — kick off generation
- `POST /nodes/{id}/regenerate` — delete old variants + re-generate
- `POST /nodes/{id}/choose` — pick a variant (body: `{variant_id}`)
- `POST /uploads` — upload a reference image (creates a seed node)
- `GET  /graph` — nodes + edges (for graph view)
- `GET  /files/{path}` — serve an image file
- `GET  /worker/status` — queue counters + watch folder path

## Regeneration

Regenerating a node **deletes** the old variant files and DB rows, then
re-queues the job. This is by design — if you want to preserve a result,
pick it as the chosen variant before regenerating, then clone the node.

## Troubleshooting

- **Nodes stuck in "queued"** → the worker isn't running or isn't watching
  the right folder. Check Terminal 2 and the path shown in the Images tab's
  "Worker Status" card.
- **"Parent node N is not ready"** → you set a parent that hasn't had a
  variant chosen yet. Open that parent, pick a variant, then generate.
- **"Max 3 parents"** → remove a slot first.
