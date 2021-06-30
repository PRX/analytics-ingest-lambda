'use strict';

const zlib = require('zlib');
const { promisify } = require('util');
const gzip = promisify(zlib.gzip);

const { buildEvent } = require('./support');
const { getRecordsFromEvent } = require('../lib/get-records');
const logger = require('../lib/logger');

describe('get-records', () => {
  it('decodes base64 kinesis records', async () => {
    const data1 = Buffer.from(JSON.stringify({ thing: 'one' }), 'utf-8').toString('base64');
    const data2 = Buffer.from(JSON.stringify({ thing: 'two' }), 'utf-8').toString('base64');
    const event = {
      Records: [{ kinesis: { data: data1 } }, { kinesis: { data: data2 } }],
    };

    const recs = await getRecordsFromEvent(event);
    expect(recs).to.eql([{ thing: 'one' }, { thing: 'two' }]);
  });

  it('decodes gzipped cloudwatch subscription filter events', async () => {
    const logEvents1 = [
      { message: JSON.stringify({ thing: 'one' }) },
      { message: JSON.stringify({ thing: 'two' }) },
      { message: JSON.stringify({ thing: 'three' }) },
    ];
    const logEvents2 = [{ message: JSON.stringify({ thing: 'four' }) }];

    const zipped1 = await gzip(JSON.stringify({ logEvents: logEvents1 }));
    const zipped2 = await gzip(JSON.stringify({ logEvents: logEvents2 }));

    const event = {
      Records: [
        { kinesis: { data: Buffer.from(zipped1).toString('base64') } },
        { kinesis: { data: Buffer.from(zipped2).toString('base64') } },
      ],
    };

    const recs = await getRecordsFromEvent(event);
    expect(recs).to.eql([
      { thing: 'one' },
      { thing: 'two' },
      { thing: 'three' },
      { thing: 'four' },
    ]);
  });
});
