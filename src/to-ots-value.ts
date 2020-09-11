import TableStore from 'tablestore';

export const valueToOTSValue = (value: unknown) => {
    if (typeof value === 'number') {
        return TableStore.Long.fromNumber(value);
    }
    if (typeof value === 'undefined') {
        return '';
    }
    return value;
};
