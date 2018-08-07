const R = require('ramda');
const request = require('request');
const moment = require('moment');
const archiver = require('archiver');
const csv = require('csv');
const events = require('events');
const reportsConfigs = require('require-all')(__dirname + '/reportsConfigs');
    
const downloadZip = ({connection, contactId, res, requester}) => {
    let datetime = new Date();
    let archive = archiver('zip');
    let eventEmitter = new events.EventEmitter();
    let pipeToReStarted = false;
    
    // name is custom specific value for private usecase
    // instead of query might be used the ready url
    let reports = [
        {
            name: 'cp-messages-download',
            fileName: `messages-${moment(datetime.getTime()).format('YYYY-MM-DD')}`,
            query: JSON.stringify({
                filters: [{accountId: requester.accountId}, {contactId}],
                orderBy: 'id',
                direction: 'desc'
            })
        },
        {
            name: 'cp-contacts-download',
            fileName: `contact-${moment(datetime.getTime()).format('YYYY-MM-DD')}`,
            query: JSON.stringify({
                filters: [{accountId: requester.accountId}, {id: contactId}],
                orderBy: 'id',
                direction: 'desc'
            })
        }
    ];

    // all headers might be provided from outside
    eventEmitter.on('start', function(stream, reportName) {
        if (!pipeToReStarted) {
            res.set('Expires', 'Tue, 03 Jul 2001 06:00:00 GMT');
            res.set('Cache-Control', 'max-age=0, no-cache, must-revalidate, proxy-revalidate');
            res.set('Last-Modified', datetime + 'GMT');
            res.set('Content-Type', 'application/force-download');
            res.set('Content-Type', 'application/octet-stream');
            res.set('Content-Type', 'application/download');
            res.set('Content-Disposition', `attachment; filename=\"report-${moment(datetime.getTime()).format('YYYY-MM-DD')}.zip\"`);
            res.set('Content-Transfer-Encoding', 'binary');
            archive.pipe(res);
            pipeToReStarted = true;
        }
        archive.append(stream, { name: `${reportName}.csv` });
    });

    let reportsCount = 0;
    eventEmitter.on('finish', stream => {
        reportsCount++;
        console.log('finish There will be no more data. Count: ' + reportsCount);
        stream.end();
        if (reportsCount === reports.length)
            archive.finalize();
    });

    let getReport = report => {
        let titles = reportsConfigs[report.name].titles;
        let stringifier = csv.stringify();
        let headersSent = false;

        request.get(`${connection.path}/${report.name}?query=${report.query}`, {
            headers: connection.headers,
        }).on('data', chunk => {

            // convert buffer to string
            let jsonStr = chunk.toString();

            if (! headersSent) {
                // private usecase when titles are getting from another place
                stringifier.write(titles);
                eventEmitter.emit('start', stringifier, report.fileName);
                headersSent = true;
            } else {
                let firstSymbol = jsonStr.substring(0, 1);

                if (firstSymbol === ',') {
                    // remove first ',' in order to get pure json string
                    jsonStr = jsonStr.substring(1);
                } else if (firstSymbol === ']') {
                    // last chunk, do nothing
                    return;
                }

                try {
                    let jsonRow = JSON.parse(jsonStr);
                    stringifier.write(R.values(jsonRow));
                } catch (e) {

                    console.log('-----------------------------');
                    console.dir({e, jsonStr}, {colors: true, depth: 5});
                    console.log('-----------------------------');
                }
            }
        }).on('end', () => {
            eventEmitter.emit('finish', stringifier);
        });
    };
    R.forEach(getReport, reports);
};

module.exports = {
    downloadZip
};
