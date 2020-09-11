import * as TableStore from 'tablestore';
import { valueToOTSValue } from './to-ots-value';
import { tsRowToObject } from './to-object';

interface EasyTableStoreOptions {
    accessKeyId: string;
    accessKeySecret: string;
    endpoint: string;
    instancename: string;
    prefix?: string;
}

interface OTSColumn {
    key: string;
    value?: string | number;
}

interface OTSPkItem {
    [key: string]: any;
}

interface OTSNextPk {
    key: string;
    value: any;
}

export const TS = TableStore;

export default class EasyTableStore {
    client: any;
    prefix: string = '';
    constructor({ accessKeyId, accessKeySecret, endpoint, instancename, prefix }: EasyTableStoreOptions) {
        this.client = new TableStore.Client({
            accessKeyId,
            accessKeySecret,
            endpoint,
            instancename,
        });
        if (prefix) {
            this.prefix = prefix;
        }
    }
    putRow(tableName: string, pks: any, columns: OTSColumn[]) {
        Object.keys(pks).forEach(pk => {
            pks[pk] = valueToOTSValue(pks[pk]);
        });
        const params = {
            tableName: this.prefix + tableName,
            condition: new TableStore.Condition(
                TableStore.RowExistenceExpectation.IGNORE,
                null,
            ),
            primaryKey: [pks],
            attributeColumns: [
                ...columns.map(column => {
                    const item = { [column.key]: valueToOTSValue(column.value) };
                    return item;
                }),
            ],
            returnContent: { returnType: TableStore.ReturnType.Primarykey },
        };

        return new Promise((resolve, reject) => {
            this.client.putRow(params, function (err: any, data: any) {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(data);
            });
        });
    }

    async getRows(tableName: string, startPks: OTSPkItem[], endPks: OTSPkItem[], columnsToGet?: string[]) {
        startPks.forEach(pks =>
            Object.keys(pks).forEach(pk => (pks[pk] = valueToOTSValue(pks[pk])))
        );
        endPks.forEach(pks =>
            Object.keys(pks).forEach(pk => (pks[pk] = valueToOTSValue(pks[pk])))
        );
        // 得到的rows按照下标0-n是从旧到新，新的在最后
        let result: any[] = [];
        let nextPks = null;
        while (true) {
            const [rows, next] = await this.getRowsPage(
                tableName,
                nextPks || startPks,
                endPks,
                columnsToGet,
            );
            result = [...result, ...rows];
            if (!next) {
                return result;
            }
            nextPks = next.map((item: any) => ({ [item.name]: item.value }));
        }
    }

    getRowsPage(
        tableName: string,
        startPks: any,
        endPks: any,
        columnsToGet?: string[],
    ): Promise<[any[], OTSNextPk[]]> {
        const params = {
            tableName: this.prefix + tableName,
            direction: TableStore.Direction.FORWARD,
            inclusiveStartPrimaryKey: startPks,
            exclusiveEndPrimaryKey: endPks,
            limit: 500,
            columnsToGet,
        };
        return new Promise((resolve, reject) => {
            this.client.getRange(params, (err: any, data: any) => {
                if (err) {
                    return reject(err);
                }
                const rows = data.rows.map(tsRowToObject);
                resolve([rows, data.nextStartPrimaryKey]);
            });
        });
    }
}
