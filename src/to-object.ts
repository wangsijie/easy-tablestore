export const tsRowToObject = (row: any) => {
    const result: { [key: string]: any } = {};
    row.primaryKey.forEach((item: any) => {
        if (item.value.toNumber) {
            result[item.name] = item.value.toNumber();
        } else {
            result[item.name] = item.value;
        }
    });
    row.attributes.forEach((item: any) => {
        if (item.columnValue.toNumber) {
            result[item.columnName] = item.columnValue.toNumber();
        } else if (typeof item.columnValue === 'boolean') {
            result[item.columnName] = item.columnValue;
        } else {
            result[item.columnName] = item.columnValue.toString();
        }
    });
    return result;
};