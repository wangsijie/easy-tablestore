import EasyTableStore, { TS } from '../src/index';

const client = new EasyTableStore({
    accessKeyId: '',
    accessKeySecret: '',
    endpoint: 'https://platform.cn-shanghai.ots.aliyuncs.com',
    instancename: 'platform',
    prefix: 'dev_',
});

test('getRows', async () => {
    const rows = await client.getRows(
        'learn_event',
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210118' },
            { stamp: TS.INF_MIN },
        ],
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210118' },
            { stamp: TS.INF_MAX },
        ]
    );
    expect(rows.length).toBeGreaterThan(0);
});

test('getRow', async () => {
    const row: any = await client.getRow(
        'learn_event',
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210118' },
            { stamp: 1610941474218000 },
        ]
    );
    expect(row.event).toBe('step-run-code');
});

test('getRow not found', async () => {
    const row: any = await client.getRow(
        'learn_event',
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210117' },
            { stamp: 1610941474218000 },
        ]
    );
    expect(row).toBe(null);
});
