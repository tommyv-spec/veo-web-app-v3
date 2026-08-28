// UI checks — run the SAME checks against local source or the DEPLOYED page.
//
// Why this exists: static/index.html holds ~21k lines of inline browser JS and
// the only thing guarding it was a `node --check` syntax pass. A feature could
// deploy and be broken, and nobody would know until the operator clicked it.
//
// Why it fetches the live page rather than trusting the local file: root
// CLAUDE.md §v938.1 — a stage log proves a stage RAN, only measuring the
// DELIVERED artifact proves it worked. `--source live` pulls the bytes Render
// is actually serving and runs the function out of those bytes.
//
// What it deliberately does NOT do: create anything. No job, no batch, no
// render. Per `feedback_no-test-jobs-on-production-platform`, a check may
// never leave a row behind on production. These run pure functions against
// fabricated state, so there is nothing to clean up.
//
// Usage:
//   node code/tests/ui/run_ui_checks.mjs --source local
//   node code/tests/ui/run_ui_checks.mjs --source live
//   node code/tests/ui/run_ui_checks.mjs --source live --url https://host/static/index.html
//
// Exit: 0 everything passed · 1 a check failed · 2 the source could not be read.

import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { dirname, join, resolve } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const LOCAL_HTML = resolve(HERE, '..', '..', 'static', 'index.html');
const LIVE_URL = 'https://veo-web-app-v3.onrender.com/static/index.html';

function parseArgs(argv) {
    const out = { source: 'local', url: LIVE_URL, only: null, file: null };
    for (let i = 0; i < argv.length; i++) {
        if (argv[i] === '--source') out.source = argv[++i];
        else if (argv[i] === '--url') out.url = argv[++i];
        else if (argv[i] === '--only') out.only = argv[++i];
        // --file points at any copy of the page. Its real job is falsification:
        // break a function in a scratch copy and confirm these checks go red.
        // A checker nobody has ever seen fail is not evidence of anything.
        else if (argv[i] === '--file') out.file = argv[++i];
    }
    return out;
}

async function loadHtml(args) {
    if (args.file) {
        return { html: readFileSync(args.file, 'utf8'), where: args.file };
    }
    if (args.source === 'local') {
        return { html: readFileSync(LOCAL_HTML, 'utf8'), where: LOCAL_HTML };
    }
    if (args.source !== 'live') throw new Error(`unknown --source ${args.source} (use local or live)`);
    const res = await fetch(args.url, { redirect: 'follow', signal: AbortSignal.timeout(60000) });
    if (!res.ok) throw new Error(`GET ${args.url} -> HTTP ${res.status}`);
    return { html: await res.text(), where: args.url };
}

// Pull one top-level function out of the inline script.
//
// This leans on the file's own shape rather than counting braces: every
// top-level function in that script block sits at exactly 8 spaces, so the
// function ends at the first following line that is 8 spaces and a closing
// brace. Brace counting would need a full JS lexer to survive the template
// literals and regex literals in there, and would fail SILENTLY when it got
// one wrong. This can only fail loudly, because whatever it slices out is
// then handed to the JS parser below — a bad slice does not parse.
function extractFunction(html, name) {
    const startRe = new RegExp(`\\n {8}(?:async )?function ${name}\\s*\\(`);
    const m = startRe.exec(html);
    if (!m) return { ok: false, reason: `no top-level "function ${name}(" found in the source` };
    const from = m.index + 1;
    const endRe = /\n {8}\}/g;
    endRe.lastIndex = from;
    const e = endRe.exec(html);
    if (!e) return { ok: false, reason: `found "function ${name}(" but no closing brace at its indentation` };
    const src = html.slice(from, e.index + e[0].length).trim();
    let fn;
    try {
        fn = new Function(`return (${src})`)();
    } catch (err) {
        return { ok: false, reason: `extracted "${name}" but it does not parse: ${err.message}` };
    }
    if (typeof fn !== 'function') return { ok: false, reason: `extracted "${name}" but it is not a function` };
    return { ok: true, fn, src };
}

// Minimal stand-ins for the browser globals a check declares it needs. A check
// supplies concrete values; anything it did not mention stays undefined, so a
// function reaching for state the check did not think about fails loudly
// instead of quietly reading a leftover from the previous case.
function applyState(state) {
    const applied = [];
    for (const [k, v] of Object.entries(state || {})) {
        globalThis[k] = v;
        applied.push(k);
    }
    return applied;
}
function clearState(keys) {
    for (const k of keys) { try { delete globalThis[k]; } catch (e) { globalThis[k] = undefined; } }
}
export function fakeDocument(byId) {
    return {
        getElementById: (id) => (byId && Object.prototype.hasOwnProperty.call(byId, id)) ? byId[id] : null,
        querySelector: () => null,
    };
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    let html, where;
    try {
        ({ html, where } = await loadHtml(args));
    } catch (e) {
        console.log(`SOURCE UNREADABLE: ${e.message}`);
        process.exit(2);
    }
    console.log(`source: ${args.source} (${where})`);
    console.log(`bytes:  ${html.length}`);

    const checkDir = join(HERE, 'checks');
    const files = readdirSync(checkDir).filter(f => f.endsWith('.mjs')).sort();
    let pass = 0, fail = 0;
    const failures = [];

    for (const file of files) {
        const mod = await import(pathToFileURL(join(checkDir, file)).href);
        const spec = mod.default;
        if (args.only && spec.feature !== args.only) continue;
        console.log(`\n== ${spec.feature} (${file})`);

        // Extract every function this feature declares it needs. A function
        // that cannot be extracted is a FAILURE, never a silent skip — a
        // checker that quietly covers nothing is worse than no checker.
        const fns = {};
        let missing = false;
        for (const name of spec.needs || []) {
            const got = extractFunction(html, name);
            if (!got.ok) {
                console.log(`  [FAIL] ${name}: ${got.reason}`);
                failures.push(`${spec.feature}/${name}: ${got.reason}`);
                fail++; missing = true;
                continue;
            }
            fns[name] = got.fn;
            console.log(`  [ok]   extracted ${name} (${got.src.length} chars)`);
        }
        if (missing) continue;

        for (const c of spec.cases || []) {
            const keys = applyState(c.state);
            let verdict;
            try {
                verdict = c.run(fns);
            } catch (e) {
                verdict = { ok: false, why: `threw ${e.name}: ${e.message}` };
            } finally {
                clearState(keys);
            }
            if (verdict && verdict.ok) {
                console.log(`  [PASS] ${c.name}`);
                pass++;
            } else {
                console.log(`  [FAIL] ${c.name} -- ${verdict ? verdict.why : 'check returned nothing'}`);
                failures.push(`${spec.feature}: ${c.name} -- ${verdict ? verdict.why : 'no verdict'}`);
                fail++;
            }
        }
    }

    console.log(`\n----------------------------------------`);
    console.log(`RESULT: ${fail === 0 ? 'PASS' : 'FAIL'}  (${pass} passed, ${fail} failed)`);
    // Report every fault at once — `feedback_checkers-report-all-faults-at-once`.
    if (fail) { console.log('failures:'); failures.forEach(f => console.log(`  - ${f}`)); }
    process.exit(fail === 0 ? 0 : 1);
}

main();
