// Minimal service worker. Sole purpose: make the site installable as a
// PWA so File System Access folder permissions become persistent across
// browser sessions (Chrome 121+ feature: Persisted Permissions for
// Installed Apps).
//
// Does NOT cache anything. Pass-through fetch handler so offline doesn't
// silently break the app.

self.addEventListener("install", (e) => {
  self.skipWaiting();
});

self.addEventListener("activate", (e) => {
  e.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", (e) => {
  // Network-only. Don't cache — backend changes shipping minute-by-minute.
});
