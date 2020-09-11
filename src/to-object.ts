export const tsRowToObject = (row: any) => {
    const result: { [key: string]: any } = {};
    row.primaryKey.forEach((item: any) => {
        result[item.name] = item.value;
    });
    row.attributes.forEach((item: any) => {
        if (item.columnValue.toNumber) {
            result[item.columnName] = item.columnValue.toNumber();
        } else {
            result[item.columnName] = item.columnValue.toString();
        }
    });
    return result;
};