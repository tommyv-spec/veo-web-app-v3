const videos = [
    {
        video: 'alpha.mp4', total_views: 1000, comments: 14,
        instagram_url: 'https://instagram.com/reel/a',
        by_account: [{ account: 'nuri', platform: 'instagram', tracking_ids: ['kv1'] }],
        period: { clicks: { value: 10 }, items_ordered: { value: 2 } },
        posted_at: '2026-09-10T10:00:00Z',
    },
    {
        video: 'beta.mp4', total_views: 5000, comments: 6,
        facebook_url: 'https://facebook.com/reel/b',
        by_account: [{ account: 'martha-fb', platform: 'facebook', tracking_ids: ['kv2'] }],
        period: { clicks: { value: 20 }, items_ordered: { value: 0 } },
        posted_at: '2026-09-11T10:00:00Z',
    },
    {
        video: 'unknown.mp4', total_views: 'Unknown', comments: 'Unknown',
        by_account: [], period: { clicks: { value: null }, items_ordered: { value: null } },
        posted_at: '',
    },
];

export default {
    feature: 'performance-table',
    needs: ['perfSelectVideos', 'perfRender'],
    cases: [
        {
            name: 'platform and outcome filters isolate clicks with no sale',
            run: ({ perfSelectVideos }) => {
                const got = perfSelectVideos(videos, {
                    platform: 'facebook', outcome: 'clicked_no_sale', sort: 'views_desc',
                });
                return got.length === 1 && got[0].video === 'beta.mp4'
                    ? { ok: true }
                    : { ok: false, why: `expected beta.mp4, got ${got.map(v => v.video).join(', ')}` };
            },
        },
        {
            name: 'search covers account and tracking tag',
            run: ({ perfSelectVideos }) => {
                const account = perfSelectVideos(videos, { search: 'martha', sort: 'video_asc' });
                const tag = perfSelectVideos(videos, { search: 'kv1', sort: 'video_asc' });
                return account[0]?.video === 'beta.mp4' && tag[0]?.video === 'alpha.mp4'
                    ? { ok: true }
                    : { ok: false, why: 'account or tracking-tag search missed its video' };
            },
        },
        {
            name: 'view to click ordering uses the funnel ratio',
            run: ({ perfSelectVideos }) => {
                const got = perfSelectVideos(videos, { sort: 'click_rate_desc' });
                return got.map(v => v.video).join(',') === 'alpha.mp4,beta.mp4,unknown.mp4'
                    ? { ok: true }
                    : { ok: false, why: `wrong ratio order: ${got.map(v => v.video).join(', ')}` };
            },
        },
        {
            name: 'direct-sales filter and ordering use attributed items',
            run: ({ perfSelectVideos }) => {
                const got = perfSelectVideos(videos, { outcome: 'sales', sort: 'sales_desc' });
                return got.length === 1 && got[0].video === 'alpha.mp4'
                    ? { ok: true }
                    : { ok: false, why: 'direct-sales outcome did not isolate alpha.mp4' };
            },
        },
        {
            name: 'unknown measurements stay last even in ascending order',
            run: ({ perfSelectVideos }) => {
                const got = perfSelectVideos(videos, { sort: 'comments_asc' });
                return got.at(-1)?.video === 'unknown.mp4'
                    ? { ok: true }
                    : { ok: false, why: 'Unknown comments did not stay last' };
            },
        },
    ],
};
