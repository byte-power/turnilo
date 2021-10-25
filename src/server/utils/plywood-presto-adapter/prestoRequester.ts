import { PlywoodLocator, basicLocator, PlywoodRequester } from 'plywood-base-api';
import { PassThrough, Readable } from 'stream';
import { Client } from 'presto-client-ts';
const toArray = require('stream-to-array');

export interface PrestoRequesterParameters {
    locator?: PlywoodLocator;
    host?: string;
    user?: string;
    source?: string;
    catalog: string;
}

interface RowObject {
    [key: string]: any
}

export function prestoRequesterFactory(parameters: PrestoRequesterParameters): PlywoodRequester<string> {
    let locator = parameters.locator;
    if (!locator) {
        let host = parameters.host;
        if (!host) throw new Error("must have a `host` or a `locator`");
        locator = basicLocator(host, 8889);
    }


    let user = !parameters.user ? 'default_user' : parameters.user;
    let source = !parameters.source ? 'default_source' : parameters.source;
    let catalog = !parameters.catalog ? 'hive' : parameters.catalog

    return (requester): Readable => {
        let query = requester.query;

        let stream = new PassThrough({
            objectMode: true
        });

        let column_stream = new PassThrough({
            objectMode: true
        })

        locator().then((location) => {
            let client = new Client({
                host: location.hostname,
                port: location.port || 8889,
                user: user,
                source: source,
                catalog: catalog,
                schema: 'turnilo',
            } as any);

            client.execute({
                query: query,
                columns: function (error, data) {
                    data.forEach((x) => {
                        column_stream.push(x.name);
                    })
                    column_stream.push(null);
                    column_stream.end();
                },
                data: function (error, data, columns, stats) {
                    toArray(column_stream).then((column_name_array: string[]) => {
                        if (!Array.isArray(column_name_array) || column_name_array.length < 1) {
                            throw new Error('invalid column response');
                        }
                        data.forEach((row) => {
                            if (row.length !== column_name_array.length) {
                                throw new Error('column length mismatch');
                            }
                            let row_object: RowObject = {};
                            for (var i = 0; i < column_name_array.length; i++){
                                row_object[column_name_array[i]] = row[i];
                            }
                            stream.push(row_object);
                        })
                    })
                },
                success: function (error, stats) {
                    stream.push(null);
                    stream.end();
                },
                error: function (error) {
                    stream.emit(error)
                    stream.end();
                }
            });
        })

        return stream;
    }
}
