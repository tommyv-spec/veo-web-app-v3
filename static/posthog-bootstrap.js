// PostHog bootstrap (v773.11.0, 2026-05-29).
//
// Loaded as a deferred script in the <head> of every static HTML page.
// Responsibilities:
//   1. Fetch /api/posthog-config — server tells us project key + host.
//      If POSTHOG_KEY env is unset on the server, config returns
//      {enabled: false} and this whole script no-ops. Keeps local dev clean.
//   2. Load the official posthog-js snippet from the configured host.
//   3. Fetch /api/me — if the user is logged in, call posthog.identify(id)
//      with email as a person-property so events join to a real account.
//      Anonymous visitors are still tracked under an auto-generated distinct_id.
//   4. Expose window.track(event, props) for the rest of the app to call.
//      Wraps posthog.capture() so callers don't need to defensively check
//      whether PostHog finished loading or whether analytics is enabled.
//
// Privacy posture:
//   - autocapture: true            -> every click / form submit / pageview
//   - session_recording enabled    -> DOM-level replay
//   - mask_all_inputs: true        -> form field values redacted in replay
//   - capture_pageview: true       -> SPA-style pageviews when URL changes
//
// Diagnostic: prints one [posthog-bootstrap] line on boot so we can confirm
// from a browser console that the snippet loaded + identified correctly.
// Remove the console.log lines after the next production run confirms they
// fire (per code/CLAUDE.md "Production deploy discipline").

(function () {
    "use strict";

    var DIAG_PREFIX = "[posthog-bootstrap]";

    function diag() {
        try {
            var args = Array.prototype.slice.call(arguments);
            args.unshift(DIAG_PREFIX);
            console.log.apply(console, args);
        } catch (_) { /* noop */ }
    }

    // Stub window.track immediately so callers that fire early don't crash.
    // Replaced with the real posthog.capture wrapper once PostHog is ready.
    var pendingEvents = [];
    window.track = function (event, props) {
        pendingEvents.push({ event: event, props: props || {} });
    };

    function flushPending() {
        if (!window.posthog || typeof window.posthog.capture !== "function") return;
        while (pendingEvents.length) {
            var ev = pendingEvents.shift();
            try { window.posthog.capture(ev.event, ev.props); } catch (e) { diag("capture failed", e); }
        }
        // Replace the stub with the real thing so future calls go direct.
        window.track = function (event, props) {
            try { window.posthog.capture(event, props || {}); } catch (e) { diag("capture failed", e); }
        };
    }

    function loadPostHog(key, host) {
        // Official posthog-js snippet (array-loader). Source:
        // https://posthog.com/docs/libraries/js#option-1-snippet
        // Trimmed and reformatted; behavior identical.
        !function (t, e) {
            var o, n, p, r; e.__SV || (window.posthog = e, e._i = [], e.init = function (i, s, a) {
                function g(t, e) { var o = e.split("."); 2 == o.length && (t = t[o[0]], e = o[1]); t[e] = function () { t.push([e].concat(Array.prototype.slice.call(arguments, 0))) } }
                p = t.createElement("script"); p.type = "text/javascript"; p.crossOrigin = "anonymous"; p.async = !0;
                p.src = s.api_host.replace(".i.posthog.com", "-assets.i.posthog.com") + "/static/array.js";
                (r = t.getElementsByTagName("script")[0]).parentNode.insertBefore(p, r);
                var u = e; for (void 0 !== a ? u = e[a] = [] : a = "posthog", u.people = u.people || [], u.toString = function (t) { var e = "posthog"; return "posthog" !== a && (e += "." + a), t || (e += " (stub)"), e }, u.people.toString = function () { return u.toString(1) + ".people (stub)" }, o = "init capture register register_once register_for_session unregister unregister_for_session getFeatureFlag getFeatureFlagPayload isFeatureEnabled reloadFeatureFlags updateEarlyAccessFeatureEnrollment getEarlyAccessFeatures on onFeatureFlags onSessionId getSurveys getActiveMatchingSurveys renderSurvey canRenderSurvey getNextSurveyStep identify setPersonProperties group resetGroups setPersonPropertiesForFlags resetPersonPropertiesForFlags setGroupPropertiesForFlags resetGroupPropertiesForFlags reset get_distinct_id getGroups get_session_id get_session_replay_url alias set_config startSessionRecording stopSessionRecording sessionRecordingStarted captureException loaded".split(" "), n = 0; n < o.length; n++) g(u, o[n]);
                e._i.push([i, s, a]);
            }, e.__SV = 1);
        }(document, window.posthog || []);

        window.posthog.init(key, {
            api_host: host,
            autocapture: true,
            capture_pageview: true,
            capture_pageleave: true,
            session_recording: {
                maskAllInputs: true,
            },
            loaded: function (ph) {
                diag("loaded", { host: host, distinct_id: ph.get_distinct_id() });
                flushPending();
                identifyIfLoggedIn();
            },
        });
    }

    function identifyIfLoggedIn() {
        fetch("/api/me", { credentials: "same-origin" })
            .then(function (r) { return r.ok ? r.json() : { authenticated: false }; })
            .then(function (me) {
                if (!me || !me.authenticated) {
                    diag("anon user");
                    return;
                }
                try {
                    window.posthog.identify(String(me.id), { email: me.email });
                    diag("identified", { id: me.id, email: me.email });
                } catch (e) { diag("identify failed", e); }
            })
            .catch(function (e) { diag("/api/me failed", e); });
    }

    function boot() {
        fetch("/api/posthog-config", { credentials: "same-origin" })
            .then(function (r) { return r.ok ? r.json() : { enabled: false }; })
            .then(function (cfg) {
                if (!cfg || !cfg.enabled) {
                    diag("disabled (no POSTHOG_KEY on server)");
                    return;
                }
                loadPostHog(cfg.key, cfg.host || "https://us.i.posthog.com");
            })
            .catch(function (e) { diag("config fetch failed", e); });
    }

    // Defer boot until DOM is ready to avoid blocking page paint.
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
