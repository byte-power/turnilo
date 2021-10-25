import { PlywoodRequester } from 'plywood-base-api';
// import  toArray from 'stream-to-array';
import { AttributeInfo, Attributes, PseudoDatum, PlyType, External, ExternalJS, ExternalValue, SQLExternal } from 'plywood';
// import { PseudoDatum } from 'plywood';
import { PrestoDialect } from './prestoDialect';
import { List } from 'immutable';
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

  // static queryListValueToPrestoSQLDescribeRow(listValue: any): any {
  //   if (!listValue.length) {
  //     return [];
  //   }
  //   let sub_list = listValue[0];
  //   return sub_list.map((subListValue: string[]) => {
  //     if (subListValue.length !== 3) {
  //       return null;
  //     }
  //     return {
  //       name: subListValue[0],
  //       sqlType: subListValue[1],
  //       arrayType: subListValue[2],
  //     }
  //   })
  // }

  static postProcessIntrospect(columns: PrestoSQLDescribeRow[]): Attributes {
    // let columnsValue = PrestoExternal.queryListValueToPrestoSQLDescribeRow(columns);
    return columns
      .map((column: PrestoSQLDescribeRow) => {
        let name = column.name;
        let type: PlyType;
        let nativeType = column.sqlType.toLowerCase();
        if (nativeType.indexOf('timestamp') !== -1 || nativeType.indexOf('date') !== -1) {
          type = 'TIME';
        } else if (nativeType.indexOf('varchar') !== -1 || nativeType.indexOf('char') !== -1) {
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
    ).then((res: any) => {
      if (!res.length) {
        return res;
      }
      return res.map((row_object: any) => row_object.Table).sort();
    });
  }

  static getVersion(requester: PlywoodRequester<any>): Promise<string> {
    return toArray(requester({ query: `SELECT '0.0.1-aws-emr-presto' as version` })).then((res: any) => {
      if (!Array.isArray(res) || res.length !== 1) throw new Error('invalid version response');
      let row_object = res[0];
      return row_object.version;
    });
  }

  constructor(parameters: ExternalValue) {
    super(parameters, new PrestoDialect());
    this._ensureEngine('presto');
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
