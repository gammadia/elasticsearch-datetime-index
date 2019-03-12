/*jslint node: true, nomen: true, white: true */

var es = null,
    appName = null,
    indexPrefix = null,
    datadogApiKey = null,
    datadogOptions = {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        }
    },
    https = require('https');
    elasticsearch = require('elasticsearch');

/**
 * Initialize the connection to an elasticsearch server.
 * @param host The elasticsearch server IP/URL
 * @param app The name of the. Will be used to build a prefix for the index where the documents must be uploaded.
 * Every documents will be uploaded to <app>_YYYYMMDD (index change every day for expiration purposes)
 * @param template If provided, the template will be applied to the created indexes
 * (more information about templates can be found here: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html)
 * @param datadogKey If provided, will trigger an event on datadog in case of failure during log upload.
 * @returns {Promise<elasticsearch.Client>}
 */
exports.init = async function(host, app, template, datadogKey) {
    es = elasticsearch.Client({host: host});
    appName = app;
    indexPrefix = `${appName.toLowerCase()}_`;
    datadogApiKey = datadogKey;

    // Check if the template is already registered and create it if needed
    if (undefined !== template) {
        try {
            let exists = await es.indices.existsTemplate({name: indexPrefix});
            if (!exists) {
                if (undefined === template.index_patterns) {
                    template.index_patterns = [`${indexPrefix}*`];
                }
                await es.indices.putTemplate({name: indexPrefix, body: template});
            }
        } catch (err) {
            if (undefined !== datadogApiKey) {
                let req = https.request(`https://api.datadoghq.com/api/v1/events?api_key=${datadogApiKey}`, datadogOptions);
                req.write(JSON.stringify({
                    title: 'Elasticsearch problem',
                    text: `Unable to find or create template ${indexPrefix}`,
                    tags: [`application:${appName}`, 'application:elasticsearch'],
                    alert_type: 'error'
                }));
                req.end();
            } else{
                throw err;
            }
        }

    }

    return es;
};

/**
 * Insert a new document in an index defined by the prefix used during initialization
 * @param log The document to upload
 * @returns {Promise<void>}
 */
exports.log = async function (log) {

    let now = new Date().toISOString();
    if (log.timestamp === undefined) {
        log.timestamp = now;
    }

    try {
        await es.index({
            index: `${indexPrefix}${now.slice(0, 10).replace(/-/gi, '')}`,
            body: log,
            type: 'log'
        })
    } catch (err) {
        if (undefined !== datadogApiKey) {
            let req = https.request(`https://api.datadoghq.com/api/v1/events?api_key=${datadogApiKey}`, datadogOptions);
            req.write(JSON.stringify({
                title: 'Elasticsearch problem',
                text: `Unable to upload log to elasticsearch server`,
                tags: [`application:${appName}`, 'application:elasticsearch'],
                alert_type: 'error'
            }));
            req.end();
        } else{
            throw err;
        }
    }
};
