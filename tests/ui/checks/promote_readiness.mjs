// Checks for the promote auto-generate feature (2026-08-28).
//
// imgPromoteReadiness decides whether a staged batch goes straight to
// createJob() or stops on the form. Getting it wrong in either direction is
// expensive: too strict and the feature never fires, too loose and a render
// starts from a broken storyboard. It is a pure function of staged state,
// which is exactly what can be checked without touching production.
//
// Not covered, and deliberately said out loud rather than implied: this does
// not prove the button is wired to it, does not prove the countdown fires,
// and does not prove createJob() builds a correct payload. Those need a real
// click, and a real click on production would create a real job.

import { fakeDocument } from '../run_ui_checks.mjs';

// A batch that is complete in every way — the case the feature exists for.
function goodState(extra = {}) {
    return {
        uploadedImages: [{ n: 0 }, { n: 1 }, { n: 2 }],
        uploadedFilesData: [{ index: 0 }, { index: 1 }, { index: 2 }],
        editorMode: 'storyboard',
        sceneBreaks: [
            { lineIndex: 0, imgIndex: 0 },
            { lineIndex: 2, imgIndex: 1 },
            { lineIndex: 4, imgIndex: 2 },
        ],
        document: fakeDocument({ dialogueInput: { value: 'one\ntwo\nthree\nfour\nfive\nsix' } }),
        ...extra,
    };
}
const goodData = { scene_assignments: [{}, {}, {}], uploaded: [{}, {}, {}] };

const is = (got, wantOk, mustSay) => {
    if (!got || typeof got.ok !== 'boolean') return { ok: false, why: `returned ${JSON.stringify(got)}` };
    if (got.ok !== wantOk) return { ok: false, why: `expected ok=${wantOk}, got ok=${got.ok} (${got.reason || 'no reason'})` };
    if (mustSay && !(got.reason || '').includes(mustSay)) {
        return { ok: false, why: `reason should mention "${mustSay}", got "${got.reason}"` };
    }
    return { ok: true };
};

export default {
    feature: 'promote-readiness',
    needs: ['imgPromoteReadiness', 'imgShowCountdownToast'],
    cases: [
        {
            name: 'a complete staged batch is ready',
            state: goodState(),
            run: (f) => is(f.imgPromoteReadiness(goodData), true),
        },
        {
            name: 'no images -> not ready',
            state: goodState({ uploadedImages: [] }),
            run: (f) => is(f.imgPromoteReadiness(goodData), false, 'images'),
        },
        {
            name: 'no dialogue lines -> not ready',
            state: goodState({ document: fakeDocument({ dialogueInput: { value: '   \n  ' } }) }),
            run: (f) => is(f.imgPromoteReadiness(goodData), false, 'dialogue'),
        },
        {
            name: 'auto-cycle mode -> not ready (it would throw away the md scene mapping)',
            state: goodState({ editorMode: 'auto' }),
            run: (f) => is(f.imgPromoteReadiness(goodData), false, 'storyboard'),
        },
        {
            name: 'staging dropped a scene -> not ready',
            state: goodState(),
            run: (f) => is(f.imgPromoteReadiness({ scene_assignments: [{}, {}] }), false, 'dropped a scene'),
        },
        {
            name: 'a scene pointing at no image -> not ready',
            state: goodState({
                sceneBreaks: [
                    { lineIndex: 0, imgIndex: 0 },
                    { lineIndex: 2, imgIndex: 9 },
                    { lineIndex: 4, imgIndex: 2 },
                ],
            }),
            run: (f) => is(f.imgPromoteReadiness(goodData), false, 'scene 2'),
        },
        {
            name: 'a text_card scene with no image is FINE (v682e) -- the carve-out must not false-alarm',
            state: goodState({
                sceneBreaks: [
                    { lineIndex: 0, imgIndex: 0 },
                    { lineIndex: 2, imgIndex: null, sceneType: 'text_card' },
                    { lineIndex: 4, imgIndex: 2 },
                ],
            }),
            run: (f) => is(f.imgPromoteReadiness(goodData), true),
        },
        {
            name: 'first scene not at line 1 -> not ready',
            state: goodState({
                sceneBreaks: [
                    { lineIndex: 1, imgIndex: 0 },
                    { lineIndex: 2, imgIndex: 1 },
                    { lineIndex: 4, imgIndex: 2 },
                ],
            }),
            run: (f) => is(f.imgPromoteReadiness(goodData), false, 'first scene'),
        },
        {
            name: 'missing scene_assignments does not crash it',
            state: goodState(),
            run: (f) => is(f.imgPromoteReadiness({}), false),
        },
        {
            name: 'the countdown toast takes (title, body, seconds, onFire, onCancel)',
            state: {},
            run: (f) => f.imgShowCountdownToast.length === 5
                ? { ok: true }
                : { ok: false, why: `expected 5 parameters, got ${f.imgShowCountdownToast.length}` },
        },
    ],
};
