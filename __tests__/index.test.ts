import EasyTableStore, { TS } from '../src/index';

const client = new EasyTableStore({
    accessKeyId: process.env.AK,
    accessKeySecret: process.env.SK,
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
            { date: '20210125' },
            { stamp: TS.INF_MIN },
        ],
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210125' },
            { stamp: TS.INF_MAX },
        ]
    );
    expect(rows.length).toBeGreaterThan(0);
});

test('getRows onPage', async () => {
    const start = new Date().getTime();
    const rows = await client.getRows(
        'learn_event',
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210125' },
            { stamp: TS.INF_MIN },
        ],
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210125' },
            { stamp: TS.INF_MAX },
        ],
        undefined,
        async (rs, next) => {
            expect(rs.length).toBeGreaterThan(0);
            await new Promise(r => setTimeout(r, 1000));
        },
    );
    const end = new Date().getTime();
    expect(rows.length).toBeGreaterThan(0);
    expect(end - start).toBeGreaterThan(1000);
});

test('getRow', async () => {
    const row: any = await client.getRow(
        'learn_event',
        [
            { event: 'step-run-code' },
            { userId: '1' },
            { date: '20210125' },
            { stamp: 1611532940464000 },
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

test('putRow and getRow', async () => {
    const result = await client.putRow(
        'learn_event',
        {
            event: 'easy-tablestore-test',
            userId: '1',
            date: '20210417',
            stamp: TS.PK_AUTO_INCR,
        },
        [
            { foo: 'bar' },
        ],
    );
    const row: any = await client.getRow(
        'learn_event',
        Object.keys(result).map(key => ({ [key]: result[key] })),
    );
    expect(row.stamp).toBe(result.stamp);
});

test('putRow(object) and getRow', async () => {
    const result = await client.putRow(
        'learn_event',
        {
            event: 'easy-tablestore-test',
            userId: '1',
            date: '20210417',
            stamp: TS.PK_AUTO_INCR,
        },
        {
            foo: 'baz',
        }
    );
    const row: any = await client.getRow(
        'learn_event',
        Object.keys(result).map(key => ({ [key]: result[key] })),
    );
    expect(row.stamp).toBe(result.stamp);
    expect(row.foo).toBe('baz');
});
