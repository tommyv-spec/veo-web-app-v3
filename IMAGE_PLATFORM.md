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
  (slot 0/1/2), each with an optional `role` label and an open
  `reference_instruction`.

Uploaded images are **seed nodes**: `kind=upload`, `status=ready`, one variant
(the uploaded file). They can be used as parents for generated nodes the same
way as other ready nodes.

## Reference-image prompt contract

Every new generation job carries two prompt fields:

- `prompt` — the legacy scene body for older local workers.
- `render_prompt` — the v2 prompt used by updated ChatGPT and Banana workers.

`render_prompt` numbers references in the exact attachment order and gives each
one a separate job before the scene brief:

```text
IMAGE REFERENCE CONTRACT v2

REFERENCE IMAGES
Image numbering below matches the attachment order.
Image 1 - Role: main character.
Use (fallback): identity reference ...
Image 2 - Role: product bottle.
Use (authoritative): Take: exact bottle. Apply to: her hand. Preserve: label.

SCENE TO CREATE
<the complete author-written image prompt>

OUTPUT
Aspect ratio: vertical 9:16.
```

The role text and the use instruction stay open-ended. In the parent picker,
`Use this image for...` can say exactly what to take, where to apply it, what to
preserve, and what to ignore. A useful open format is `Take: ...`,
`Apply to: ...`, `Preserve: ...`, `Ignore: ...`. That instruction is
authoritative. If it is left
blank, the platform supplies a safe fallback for known jobs: persona identity,
exact product appearance, prior-scene continuity, a second body/pose reference,
or a generic stated role. Each input gets its own job, so a body reference cannot
compete with the base scene. The scene body still decides the pose, action,
composition, location, style, edit, and any deliberate transfer between inputs.
This keeps one contract usable for identity locks, product placement, Pinterest
location or styling references, scene edits, compositing, pose/style references,
and continuity chains.

Both workers sort attachments by `slot_order` before upload. The first uploaded
file is `Image 1`, the second is `Image 2`, and so on. The Flow API upload also
keeps each file's real PNG, JPEG, or WebP MIME type. ChatGPT waits for every
attachment preview before sending the prompt and excludes user uploads when it
looks for the generated result.

The numbered role map follows the official multi-image guidance for
[GPT Image 2](https://developers.openai.com/cookbook/examples/multimodal/image-gen-models-prompting-guide)
and [Nano Banana](https://ai.google.dev/gemini-api/docs/image-generation).

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
