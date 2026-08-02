(async () => {
  const status = document.getElementById("kaveno-pair-status");
  const readMeta = (name) => {
    const element = document.querySelector(`meta[name="${name}"]`);
    return element ? element.content : "";
  };
  const ticket = readMeta("kaveno-chatgpt-ticket");
  const apiUrl = readMeta("kaveno-chatgpt-api");
  if (!ticket || !apiUrl) return;

  if (status) status.textContent = "Pairing the extension…";
  try {
    const result = await chrome.runtime.sendMessage({
      type: "KAVENO_PAIR",
      ticket,
      apiUrl
    });
    if (!result || !result.ok) throw new Error((result && result.error) || "Pairing failed.");
    if (status) status.textContent = `Connected to ${result.email}. The worker window is opening.`;
  } catch (error) {
    if (status) status.textContent = `Could not connect: ${error.message || error}`;
  }
})();

