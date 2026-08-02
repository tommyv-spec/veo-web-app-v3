const ONBOARDING_URL = "https://kavenobuilder.com/api/images/worker/extension/onboarding";
const WORKER_PAGE = chrome.runtime.getURL("worker.html");

async function openOnboarding() {
  await chrome.tabs.create({ url: ONBOARDING_URL, active: true });
}

async function openWorkerWindow() {
  const existing = await chrome.tabs.query({ url: WORKER_PAGE });
  if (existing.length) {
    const tab = existing[0];
    await chrome.tabs.update(tab.id, { active: true });
    if (tab.windowId != null) await chrome.windows.update(tab.windowId, { focused: true });
    return tab.id;
  }
  const win = await chrome.windows.create({
    url: WORKER_PAGE,
    type: "popup",
    focused: true,
    width: 470,
    height: 720
  });
  return win.tabs && win.tabs[0] ? win.tabs[0].id : null;
}

async function redeemPairing(message) {
  const apiUrl = String(message.apiUrl || "").replace(/\/$/, "");
  if (!apiUrl || !message.ticket) throw new Error("Pairing information is incomplete.");
  const response = await fetch(`${apiUrl}/api/images/worker/extension/redeem`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ticket: message.ticket })
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail || `Pairing failed (${response.status}).`);
  const workerId = `chatgpt-ext-${crypto.randomUUID()}`;
  await chrome.storage.local.set({
    apiUrl: body.api_url,
    apiKey: body.api_key,
    expectedEmail: String(body.chatgpt_email || "").toLowerCase(),
    workerId,
    pairedAt: Date.now()
  });
  await openWorkerWindow();
  return { ok: true, email: body.chatgpt_email };
}

chrome.runtime.onInstalled.addListener((details) => {
  if (details.reason === "install") openOnboarding().catch(() => {});
});

chrome.action.onClicked.addListener(() => {
  chrome.storage.local.get(["apiKey"]).then((stored) => {
    return stored.apiKey ? openWorkerWindow() : openOnboarding();
  }).catch(() => {});
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message && message.type === "KAVENO_PAIR") {
    redeemPairing(message).then(sendResponse).catch((error) => {
      sendResponse({ ok: false, error: error.message || String(error) });
    });
    return true;
  }
  if (message && message.type === "OPEN_ONBOARDING") {
    openOnboarding().then(() => sendResponse({ ok: true })).catch((error) => {
      sendResponse({ ok: false, error: error.message || String(error) });
    });
    return true;
  }
  return false;
});
