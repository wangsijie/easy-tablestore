import EasyTableStore, { TS } from '../src/index';

const client = new EasyTableStore({
    accessKeyId: 'LTAI4GCbQbkWxzXbba3TEttJ',
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
