import { PlywoodRequester } from 'plywood-base-api';
// import  toArray from 'stream-to-array';
import { AttributeInfo, Attributes, PseudoDatum, PlyType, External, ExternalJS, ExternalValue, SQLExternal } from 'plywood';
// import { PseudoDatum } from 'plywood';
import { PrestoDialect } from './prestoDialect';
// import { PlyType } from 'plywood/build/types';
// import { External, ExternalJS, ExternalValue } from 'plywood/build/external/baseExternal';
// import { SQLExternal } from 'plywood/build/external/sqlExternal';

const toArray = require('stream-to-array');

export interface PrestoSQLDescribeRow {
    name: string;
    sqlType: string;
    arrayType?: string;
}

export class PrestoExternal extends SQLExternal {
    static engine = 'presto';
    static type = 'DATASET';

    static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): PrestoExternal {
        let value: ExternalValue = External.jsToValue(parameters, requester);
        return new PrestoExternal(value);
    }

    static postProcessIntrospect(columns: PrestoSQLDescribeRow[]): Attributes {
        return columns
          .map((column: PrestoSQLDescribeRow) => {
            let name = column.name;
            let type: PlyType;
            let nativeType = column.sqlType.toLowerCase();
            if (nativeType.indexOf('timestamp') !== -1 || nativeType.indexOf('date') !== -1) {
              type = 'TIME';
            } else if (nativeType.indexOf('varchar') !== -1 || nativeType.indexOf('char') !== -1){
              type = 'STRING';
            } else if (nativeType === 'integer' || nativeType === 'bigint') {
              // ToDo: make something special for integers
              type = 'NUMBER';
            } else if (nativeType === 'double' || nativeType === 'real') {
              type = 'NUMBER';
            } else if (nativeType === 'boolean') {
              type = 'BOOLEAN';
            } else if (nativeType === 'array') {
              nativeType = column.arrayType.toLowerCase();
              if (nativeType.indexOf('varchar') !== -1 || nativeType.indexOf('char') !== -1) {
                type = 'SET/STRING';
              } else if (nativeType === 'timestamp') {
                type = 'SET/TIME';
              } else if (
                nativeType === 'integer' ||
                nativeType === 'bigint' ||
                nativeType === 'double' ||
                nativeType === 'real'
              ) {
                type = 'SET/NUMBER';
              } else if (nativeType === 'boolean') {
                type = 'SET/BOOLEAN';
              } else {
                return null;
              }
            } else {
              return null;
            }
    
            return new AttributeInfo({
              name,
              type,
              nativeType,
            });
          })
          .filter(Boolean);
    }

    static getSourceList(requester: PlywoodRequester<any>): Promise<string[]> {
        return toArray(
          requester({
            query: `SHOW TABLES FROM turnilo`,
          }),
        ).then((sources: any) => {
          if (!sources.length) return sources;
          let key = Object.keys(sources[0])[0];
          return sources.map((s: PseudoDatum) => s[key]).sort();
        });
    }

    static getVersion(requester: PlywoodRequester<any>): Promise<string> {
        return toArray(requester({ query: `SELECT 'aws-emr-presto'` })).then((res: any) => {
          if (!Array.isArray(res) || res.length !== 1) throw new Error('invalid version response');
          let key = Object.keys(res[0])[0];
          if (!key) throw new Error('invalid version response (no key)');
          return res[0][key];
        });
    }

    constructor(parameters: ExternalValue) {
        super(parameters, new PrestoDialect());
        this._ensureEngine('postgres');
    }
    protected getIntrospectAttributes(): Promise<Attributes> {
        return toArray(
          this.requester({
            query: `SELECT c.column_name as "name", c.data_type as "sqlType", null AS "arrayType"
           FROM information_schema.columns c
           WHERE table_name = ${this.dialect.escapeLiteral(this.source as string)}`,
          }),
        ).then(PrestoExternal.postProcessIntrospect);
    }
}
