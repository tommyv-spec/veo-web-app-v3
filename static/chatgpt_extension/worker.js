const CHATGPT_URL = "https://chatgpt.com/";
const POLL_MS = 5000;
const HEARTBEAT_MS = 5000;

const ui = {
  status: document.getElementById("statusText"),
  dot: document.getElementById("statusDot"),
  expected: document.getElementById("expectedEmail"),
  current: document.getElementById("currentEmail"),
  lastJob: document.getElementById("lastJob"),
  log: document.getElementById("log"),
  start: document.getElementById("startButton"),
  stop: document.getElementById("stopButton"),
  chatgpt: document.getElementById("chatgptButton"),
  clear: document.getElementById("clearButton")
};

let config = null;
let running = false;
let busy = false;
let chatgptTabId = null;
let lastHeartbeat = 0;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function log(message) {
  const stamp = new Date().toLocaleTimeString();
  ui.log.textContent += `[${stamp}] ${message}\n`;
  ui.log.scrollTop = ui.log.scrollHeight;
}

function setState(kind, message) {
  ui.dot.className = `dot ${kind}`;
  ui.status.textContent = message;
  ui.start.disabled = running;
  ui.stop.disabled = !running;
}

function headers(json = false) {
  const result = { Authorization: `Bearer ${config.apiKey}` };
  if (json) result["Content-Type"] = "application/json";
  return result;
}

async function api(path, options = {}) {
  const response = await fetch(`${config.apiUrl}/api/images/worker${path}`, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || `${path} failed (${response.status}).`);
  return body;
}

async function waitForTab(tabId, timeoutMs = 45000) {
  const current = await chrome.tabs.get(tabId).catch(() => null);
  if (current && current.status === "complete") return current;
  return await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      chrome.tabs.onUpdated.removeListener(listener);
      reject(new Error("ChatGPT tab did not finish loading."));
    }, timeoutMs);
    const listener = (updatedId, changeInfo, tab) => {
      if (updatedId === tabId && changeInfo.status === "complete") {
        clearTimeout(timer);
        chrome.tabs.onUpdated.removeListener(listener);
        resolve(tab);
      }
    };
    chrome.tabs.onUpdated.addListener(listener);
  });
}

async function ensureChatgptTab(focus = false) {
  if (chatgptTabId != null) {
    const existing = await chrome.tabs.get(chatgptTabId).catch(() => null);
    if (existing) {
      if (focus) {
        await chrome.tabs.update(chatgptTabId, { active: true });
        await chrome.windows.update(existing.windowId, { focused: true });
      }
      return chatgptTabId;
    }
  }
  const saved = await chrome.storage.local.get("chatgptTabId");
  if (saved.chatgptTabId != null) {
    const existing = await chrome.tabs.get(saved.chatgptTabId).catch(() => null);
    if (existing && String(existing.url || "").startsWith(CHATGPT_URL)) {
      chatgptTabId = existing.id;
      return chatgptTabId;
    }
  }
  const tab = await chrome.tabs.create({ url: CHATGPT_URL, active: true });
  chatgptTabId = tab.id;
  await chrome.storage.local.set({ chatgptTabId });
  await waitForTab(chatgptTabId);
  return chatgptTabId;
}

async function accountEmail() {
  const tabId = await ensureChatgptTab(false);
  const response = await sendToChatgpt(tabId, { type: "KAVENO_ACCOUNT" });
  const email = response && response.email ? String(response.email).toLowerCase() : "";
  ui.current.textContent = email || "Not logged in";
  return email;
}

async function sendToChatgpt(tabId, message, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const response = await chrome.tabs.sendMessage(tabId, message);
      if (response) return response;
    } catch (error) {
      lastError = error;
    }
    await sleep(500);
  }
  throw new Error(`The ChatGPT tab is not ready${lastError ? `: ${lastError.message || lastError}` : "."}`);
}

async function verifyAccount() {
  const current = await accountEmail();
  if (!current) {
    setState("error", `Log into ${config.expectedEmail} in the ChatGPT tab.`);
    return false;
  }
  if (current !== config.expectedEmail) {
    setState("error", `Wrong ChatGPT account. Switch ${current} to ${config.expectedEmail}.`);
    return false;
  }
  return true;
}

async function heartbeat() {
  const now = Date.now();
  if (now - lastHeartbeat < HEARTBEAT_MS) return;
  await api(`/heartbeat?worker_id=${encodeURIComponent(config.workerId)}`, {
    method: "POST",
    headers: headers()
  });
  lastHeartbeat = now;
}

async function releaseClaims(goingOffline = false) {
  await api(`/release-claims?worker_id=${encodeURIComponent(config.workerId)}&going_offline=${goingOffline ? "true" : "false"}`, {
    method: "POST",
    headers: headers()
  });
}

function buildPrompt(job) {
  const body = String(job.render_prompt || job.prompt || "").trim();
  // v909: marked jobs already contain the numbered, backend-specific
  // reference contract. Older jobs keep the local fallback below.
  if (body.includes("IMAGE REFERENCE CONTRACT v2") || body.includes("IMAGE REFERENCE CONTRACT v1")) return body;
  const lines = [`Crea immagine: ${body}`];
  const refs = [...(job.input_images || [])].sort((a, b) => (a.slot_order || 0) - (b.slot_order || 0));
  refs.forEach((ref) => {
    const role = String(ref.role || "").trim();
    if (role) lines.push(`use the uploaded reference for ${role}.`);
  });
  const aspect = { "9:16": "vertical 9:16", "16:9": "horizontal 16:9" }[String(job.aspect_ratio || "").trim()];
  if (aspect) lines.push(`The image is ${aspect}.`);
  return lines.join("\n");
}

function blobToDataUrl(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error || new Error("Reference image could not be read."));
    reader.readAsDataURL(blob);
  });
}

function referenceMime(filename, responseMime) {
  const value = String(responseMime || "").toLowerCase();
  if (["image/png", "image/jpeg", "image/webp"].includes(value)) return value;
  const lower = String(filename || "").toLowerCase();
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
  if (lower.endsWith(".webp")) return "image/webp";
  return "image/png";
}

async function downloadReferences(job) {
  const refs = [...(job.input_images || [])].sort((a, b) => (a.slot_order || 0) - (b.slot_order || 0));
  const result = [];
  for (const ref of refs) {
    const response = await fetch(ref.url, { headers: headers() });
    if (!response.ok) throw new Error(`Reference download failed (${response.status}).`);
    const blob = await response.blob();
    result.push({
      filename: ref.filename || "reference.png",
      mime: referenceMime(ref.filename, blob.type),
      dataUrl: await blobToDataUrl(blob)
    });
  }
  return result;
}

function dataUrlToBlob(dataUrl) {
  const [head, encoded] = String(dataUrl).split(",", 2);
  const mime = ((head || "").match(/^data:([^;]+);base64$/) || [])[1] || "image/png";
  const binary = atob(encoded || "");
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) bytes[i] = binary.charCodeAt(i);
  return new Blob([bytes], { type: mime });
}

async function postStatus(nodeId, status, error = null) {
  const body = { status };
  if (error != null) body.error = error;
  await api(`/jobs/${nodeId}/status`, {
    method: "POST",
    headers: headers(true),
    body: JSON.stringify(body)
  });
}

async function processJob(job) {
  busy = true;
  setState("busy", `Generating image node ${job.id}…`);
  ui.lastJob.textContent = `${job.id}: generating`;
  log(`Claimed node ${job.id}.`);
  try {
    const tabId = await ensureChatgptTab(false);
    await chrome.tabs.update(tabId, { url: CHATGPT_URL });
    await waitForTab(tabId);
    const refs = await downloadReferences(job);
    const result = await sendToChatgpt(tabId, {
      type: "KAVENO_GENERATE",
      prompt: buildPrompt(job),
      refs
    }, 240000);
    if (!result || !result.ok || !result.dataUrl) {
      throw new Error((result && result.error) || "ChatGPT did not return an image.");
    }
    const form = new FormData();
    form.append("files", dataUrlToBlob(result.dataUrl), "variant_1.png");
    const upload = await fetch(`${config.apiUrl}/api/images/worker/jobs/${job.id}/variants`, {
      method: "POST",
      headers: headers(),
      body: form
    });
    if (!upload.ok) {
      const body = await upload.json().catch(() => ({}));
      throw new Error(body.detail || `Image upload failed (${upload.status}).`);
    }
    await postStatus(job.id, "completed");
    ui.lastJob.textContent = `${job.id}: completed`;
    log(`Node ${job.id} uploaded.`);
  } catch (error) {
    const message = error.message || String(error);
    await postStatus(job.id, "failed", message).catch(() => {});
    ui.lastJob.textContent = `${job.id}: failed`;
    log(`Node ${job.id} failed: ${message}`);
  } finally {
    busy = false;
  }
}

async function loop() {
  while (running) {
    try {
      if (!(await verifyAccount())) {
        await sleep(3000);
        continue;
      }
      await heartbeat();
      if (!busy) {
        setState("online", "Online. Waiting for a ChatGPT image job.");
        const body = await api(`/jobs/pending?worker_id=${encodeURIComponent(config.workerId)}&backend=chatgpt`, {
          headers: headers()
        });
        if (body.job) await processJob(body.job);
      }
    } catch (error) {
      setState("error", error.message || String(error));
      log(`Worker error: ${error.message || error}`);
    }
    await sleep(POLL_MS);
  }
}

async function start() {
  if (running) return;
  if (!config) {
    setState("error", "Not paired. Start setup from KavenoBuilder.");
    return;
  }
  running = true;
  setState("offline", "Checking the ChatGPT account…");
  await releaseClaims(false).catch((error) => log(`Startup release warning: ${error.message || error}`));
  loop();
}

async function stop() {
  running = false;
  setState("offline", "Stopped.");
  await releaseClaims(true).catch(() => {});
  log("Worker stopped.");
}

async function init() {
  const stored = await chrome.storage.local.get(["apiUrl", "apiKey", "expectedEmail", "workerId"]);
  if (!stored.apiUrl || !stored.apiKey || !stored.expectedEmail || !stored.workerId) {
    setState("error", "Not paired. Start setup from KavenoBuilder.");
    ui.start.disabled = true;
    return;
  }
  config = stored;
  ui.expected.textContent = config.expectedEmail;
  log(`Paired for ${config.expectedEmail}.`);
  await start();
}

ui.start.addEventListener("click", () => start());
ui.stop.addEventListener("click", () => stop());
ui.chatgpt.addEventListener("click", () => ensureChatgptTab(true));
ui.clear.addEventListener("click", () => { ui.log.textContent = ""; });
window.addEventListener("beforeunload", () => {
  if (running && config) {
    fetch(`${config.apiUrl}/api/images/worker/release-claims?worker_id=${encodeURIComponent(config.workerId)}&going_offline=true`, {
      method: "POST",
      headers: headers(),
      keepalive: true
    }).catch(() => {});
  }
});

init().catch((error) => {
  setState("error", error.message || String(error));
  log(`Startup failed: ${error.message || error}`);
});
