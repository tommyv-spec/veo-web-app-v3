// Checks for the mobile tap decision (2026-08-29).
//
// On a desktop a MOUSE click on a variant tile IS the approval — that is the
// existing behaviour and it must not change. A finger activation asks to see
// the picture bigger. The decision follows the actual event, not a device
// label, because a hybrid laptop can have a mouse and a touchscreen together.
//
// imgVariantTapAction is the whole decision, kept pure so it can be checked
// here rather than only through a browser. What this does NOT prove: that
// the tile is wired to it, or that the viewer opens. Those need a real tap
// and live in tools/prove_feature.py.

const is = (got, want) => got === want
    ? { ok: true }
    : { ok: false, why: `expected "${want}", got "${got}"` };

export default {
    feature: 'mobile-tap',
    needs: ['imgVariantTapAction', 'imgIsTouchDevice'],
    cases: [
        {
            name: 'on a touch device a tap asks to see it bigger',
            state: {},
            run: (f) => is(f.imgVariantTapAction('touch', false), 'zoom'),
        },
        {
            name: 'on a mouse a click still picks — desktop behaviour is unchanged',
            state: {},
            run: (f) => is(f.imgVariantTapAction('mouse', true), 'pick'),
        },
        {
            name: 'an unknown event uses the coarse-pointer fallback safely',
            state: {},
            run: (f) => {
                if (f.imgVariantTapAction('', true) !== 'zoom') return { ok: false, why: 'coarse fallback did not preview' };
                if (f.imgVariantTapAction('', false) !== 'pick') return { ok: false, why: 'fine fallback did not preserve mouse behaviour' };
                return { ok: true };
            },
        },
        {
            name: 'pen and other non-mouse pointers take the safe preview path',
            state: {},
            run: (f) => is(f.imgVariantTapAction('pen', false), 'zoom'),
        },
        {
            name: 'imgIsTouchDevice never throws when matchMedia is missing',
            state: { matchMedia: undefined },
            run: (f) => {
                const got = f.imgIsTouchDevice();
                return got === false ? { ok: true } : { ok: false, why: `returned ${got}, expected false` };
            },
        },
    ],
};
