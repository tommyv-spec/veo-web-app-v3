const KAVENO_SELECTORS = {
  composer: "#prompt-textarea",
  fileInput: "input[type=file]",
  send: "button[data-testid=send-button], button[aria-label*='Send'], button[data-testid='composer-send-button']",
  generatedImage: "img[src*='estuary/content'], img[src*='backend-api/files'], img[src*='oaiusercontent'], img[alt*='Generated']",
  stop: "button[data-testid='stop-button'], button[aria-label*='Stop']"
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(getter, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const value = await getter();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await sleep(500);
  }
  throw new Error(`${label} timed out${lastError ? `: ${lastError.message || lastError}` : ""}`);
}

async function currentAccountEmail() {
  try {
    const response = await fetch("/backend-api/me", { credentials: "include" });
    if (!response.ok) return null;
    const body = await response.json();
    return (body && (body.email || (body.account && body.account.email))) || null;
  } catch (_) {
    return null;
  }
}

function dataUrlToFile(item) {
  const parts = String(item.dataUrl || "").split(",");
  if (parts.length < 2) throw new Error(`Invalid reference image: ${item.filename}`);
  const mimeMatch = parts[0].match(/^data:([^;]+);base64$/);
  const mime = item.mime || (mimeMatch && mimeMatch[1]) || "image/png";
  const bytes = atob(parts.slice(1).join(","));
  const array = new Uint8Array(bytes.length);
  for (let i = 0; i < bytes.length; i += 1) array[i] = bytes.charCodeAt(i);
  return new File([array], item.filename || "reference.png", { type: mime });
}

async function attachReferences(refs) {
  if (!refs || !refs.length) return;
  const input = await waitFor(
    () => document.querySelector(KAVENO_SELECTORS.fileInput),
    20000,
    "ChatGPT attachment input"
  );
  const transfer = new DataTransfer();
  refs.forEach((item) => transfer.items.add(dataUrlToFile(item)));
  input.files = transfer.files;
  input.dispatchEvent(new Event("input", { bubbles: true, composed: true }));
  input.dispatchEvent(new Event("change", { bubbles: true, composed: true }));
  await sleep(Math.max(1800, refs.length * 900));
}

async function fillComposer(prompt) {
  const composer = await waitFor(
    () => document.querySelector(KAVENO_SELECTORS.composer),
    30000,
    "ChatGPT composer"
  );
  composer.focus();
  let inserted = false;
  try {
    document.execCommand("selectAll", false, null);
    inserted = document.execCommand("insertText", false, prompt);
  } catch (_) {
    inserted = false;
  }
  if (!inserted || !String(composer.textContent || composer.value || "").trim()) {
    if ("value" in composer) composer.value = prompt;
    else composer.textContent = prompt;
  }
  composer.dispatchEvent(new InputEvent("input", {
    bubbles: true,
    composed: true,
    inputType: "insertText",
    data: prompt
  }));
  await sleep(500);
}

async function generatedImageAsDataUrl(src) {
  const response = await fetch(src, { credentials: "include" });
  if (!response.ok) throw new Error(`Generated image download failed (${response.status}).`);
  const blob = await response.blob();
  return await new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve({ dataUrl: reader.result, mime: blob.type || "image/png" });
    reader.onerror = () => reject(reader.error || new Error("Generated image could not be read."));
    reader.readAsDataURL(blob);
  });
}

async function generateImage(message) {
  const email = await currentAccountEmail();
  if (!email) throw new Error("ChatGPT is logged out. Sign in in this worker tab.");
  const baseline = document.querySelectorAll(KAVENO_SELECTORS.generatedImage).length;
  await attachReferences(message.refs || []);
  await fillComposer(message.prompt || "");
  const send = await waitFor(() => {
    const button = document.querySelector(KAVENO_SELECTORS.send);
    return button && !button.disabled ? button : null;
  }, 15000, "ChatGPT send button");
  send.click();

  const image = await waitFor(() => {
    const images = Array.from(document.querySelectorAll(KAVENO_SELECTORS.generatedImage));
    const stillGenerating = Boolean(document.querySelector(KAVENO_SELECTORS.stop));
    if (images.length <= baseline || stillGenerating) return null;
    const candidate = images[images.length - 1];
    return candidate && candidate.src ? candidate : null;
  }, 210000, "ChatGPT generated image");
  await sleep(1200);
  const result = await generatedImageAsDataUrl(image.src);
  return { ok: true, email, ...result };
}

let activeGeneration = false;
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (!message) return false;
  if (message.type === "KAVENO_ACCOUNT") {
    currentAccountEmail().then((email) => sendResponse({ ok: Boolean(email), email }))
      .catch((error) => sendResponse({ ok: false, email: null, error: error.message || String(error) }));
    return true;
  }
  if (message.type === "KAVENO_GENERATE") {
    if (activeGeneration) {
      sendResponse({ ok: false, error: "A ChatGPT image is already generating in this tab." });
      return false;
    }
    activeGeneration = true;
    generateImage(message).then(sendResponse).catch((error) => {
      sendResponse({ ok: false, error: error.message || String(error) });
    }).finally(() => { activeGeneration = false; });
    return true;
  }
  return false;
});
