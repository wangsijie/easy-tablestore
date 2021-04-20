import * as TableStore from 'tablestore';
import { valueToOTSValue } from './to-ots-value';
import { tsRowToObject, OTSObject } from './to-object';

export * from './to-object';
export * from './to-ots-value';

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

function formatPksToArray(pks: OTSPkItem[] | OTSObject): OTSPkItem[] {
    const pkGroups = Array.isArray(pks) ? pks : Object.keys(pks).map(key => ({ [key]: pks[key] }));
    return pkGroups.map(pkGroup => {
        const key = Object.keys(pkGroup)[0];
        return { [key]: valueToOTSValue(pkGroup[key]) };
    });
}

export default class EasyTableStore {
    client: any;
    prefix: string = '';
    constructor({
        accessKeyId,
        accessKeySecret,
        endpoint,
        instancename,
        prefix,
    }: EasyTableStoreOptions) {
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
    putRow(
        tableName: string,
        pks: OTSObject,
        columnsOrDataObject: OTSColumn[] | OTSObject
    ): Promise<OTSObject | null> {
        Object.keys(pks).forEach(pk => {
            pks[pk] = valueToOTSValue(pks[pk]);
        });
        const attributeColumns = Array.isArray(columnsOrDataObject)
            ? [
                  ...columnsOrDataObject.map(column => {
                      const item = { [column.key]: valueToOTSValue(column.value) };
                      return item;
                  }),
              ]
            : Object.keys(columnsOrDataObject).map(key => {
                  const item = { [key]: valueToOTSValue(columnsOrDataObject[key]) };
                  return item;
              });
        const params = {
            tableName: this.prefix + tableName,
            condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
            primaryKey: [pks],
            attributeColumns,
            returnContent: { returnType: TableStore.ReturnType.Primarykey },
        };

        return new Promise((resolve, reject) => {
            this.client.putRow(params, function (err: any, data: any) {
                if (err) {
                    reject(err);
                    return;
                }
                if (data.row) {
                    resolve(tsRowToObject(data.row));
                } else {
                    resolve(null);
                }
            });
        });
    }

    async getRow(tableName: string, pks: OTSPkItem[] | OTSObject, columnsToGet?: string[]) {
        const params = {
            tableName: this.prefix + tableName,
            direction: TableStore.Direction.FORWARD,
            primaryKey: formatPksToArray(pks),
            limit: 500,
            columnsToGet,
        };
        return new Promise((resolve, reject) => {
            this.client.getRow(params, (err: any, data: any) => {
                if (err) {
                    return reject(err);
                }
                if (data.row && Object.keys(data.row).length) {
                    const row = tsRowToObject(data.row);
                    resolve(row);
                } else {
                    resolve(null);
                }
            });
        });
    }

    async getRows(
        tableName: string,
        startPks: OTSPkItem[] | OTSObject,
        endPks: OTSPkItem[] | OTSObject,
        columnsToGet?: string[],
        onPage?: (rows: any[], next: OTSNextPk[]) => Promise<void>
    ) {
        const formatedStartPks = formatPksToArray(startPks);
        const formatedEndPks = formatPksToArray(endPks);
        // 得到的rows按照下标0-n是从旧到新，新的在最后
        let result: any[] = [];
        let nextPks = null;
        while (true) {
            const [rows, next] = await this.getRowsPage(
                tableName,
                nextPks || formatedStartPks,
                formatedEndPks,
                columnsToGet
            );
            if (onPage) {
                await onPage(rows, next);
            }
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
        columnsToGet?: string[]
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

    async searchByTerms(
        tableName: string,
        indexName: string,
        conditions: any,
        columnsToGet?: string[]
    ) {
        const query = {
            queryType: TableStore.QueryType.BOOL_QUERY,
            query: {
                mustQueries: Object.keys(conditions).map(key => ({
                    queryType: TableStore.QueryType.TERM_QUERY,
                    query: {
                        fieldName: key,
                        term: valueToOTSValue(conditions[key]),
                    },
                })),
            },
        };
        return await this.search(tableName, indexName, query, columnsToGet);
    }

    // query: https://github.com/aliyun/aliyun-tablestore-nodejs-sdk/blob/master/samples/search.js
    async search(tableName: string, indexName: string, query: any, columnsToGet?: string[]) {
        let next = null;
        const result = [];
        do {
            const { rows, nextToken } = await this.searchPage(
                next,
                tableName,
                indexName,
                query,
                columnsToGet
            );
            result.push(...rows);
            next = nextToken;
        } while (next);
        return result.map(tsRowToObject);
    }

    async searchPage(
        nextToken: any,
        tableName: string,
        indexName: string,
        query: any,
        columnsToGet?: string[]
    ): Promise<{ rows: any[]; nextToken: any }> {
        const params = {
            tableName: this.prefix + tableName,
            indexName: this.prefix + indexName,
            searchQuery: {
                offset: 0,
                limit: 100,
                query,
                getTotalCount: true,
                token: nextToken,
            },
            columnToGet: !columnsToGet
                ? {
                      returnType: TableStore.ColumnReturnType.RETURN_ALL,
                  }
                : {
                      returnType: TableStore.ColumnReturnType.RETURN_SPECIFIED,
                      returnNames: columnsToGet,
                  },
        };
        return new Promise((resolve, reject) => {
            this.client.search(params, (err: any, data: any) => {
                if (err) {
                    return reject(err);
                }
                const nextToken = data.nextToken.toString('base64');
                resolve({
                    rows: data.rows,
                    nextToken,
                });
            });
        });
    }

    async deleteRow(tableName: string, pks: OTSPkItem[] | OTSObject) {
        const params = {
            tableName: this.prefix + tableName,
            condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
            primaryKey: formatPksToArray(pks),
        };
        return new Promise((resolve, reject) => {
            this.client.deleteRow(params, (err: any, data: any) => {
                if (err) {
                    return reject(err);
                }
                resolve(null);
            });
        });
    }

    async updateRow(
        tableName: string,
        pks: OTSPkItem[] | OTSObject,
        updateOfAttributeColumns: OTSObject[]
    ) {
        const params = {
            tableName: this.prefix + tableName,
            condition: new TableStore.Condition(TableStore.RowExistenceExpectation.IGNORE, null),
            primaryKey: formatPksToArray(pks),
            updateOfAttributeColumns,
        };
        return new Promise((resolve, reject) => {
            this.client.updateRow(params, (err: any, data: any) => {
                if (err) {
                    return reject(err);
                }
                if (data) {
                    resolve(data);
                } else {
                    resolve(null);
                }
            });
        });
    }
}
