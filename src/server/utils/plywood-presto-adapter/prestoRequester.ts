import { PlywoodLocator, basicLocator, PlywoodRequester } from 'plywood-base-api';
import { PassThrough, Readable } from 'stream';
import { Client } from 'presto-client-ts';


export interface PrestoRequesterParameters {
    locator?: PlywoodLocator;
    host?: string;
    user?: string;
    source?: string;
    catalog: string;
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

        locator().then((location) => {
            let client = new Client({
                host: location.hostname,
                port: location.port || 8889,
                user: user,
                source: source,
                catalog: catalog
            } as any);

            client.execute({
                query: query,
                data: function (error, data, columns, stats) {
                    stream.push(data);
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