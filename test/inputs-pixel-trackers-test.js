const support = require('./support')
const bigquery = require('../lib/bigquery')
const logger = require('../lib/logger')
const PixelTrackers = require('../lib/inputs/pixel-trackers')

describe('pixel-trackers', () => {

  let pixel = new PixelTrackers()
  const data = {
    type: 'pixel',
    timestamp: 1490827132999,
    key: 'the-key',
    canonical: 'the-canonical',
    remoteAgent: 'the-user-agent',
    remoteIp: '127.0.0.1',
    remoteReferrer: 'the-referer',
    userId: 'the-user-id',
  }

  it('recognizes pixel records', () => {
    expect(pixel.check({})).to.be.false
    expect(pixel.check({type: 'impression'})).to.be.false
    expect(pixel.check({type: 'pixel'})).to.be.true
  })

  it('knows the destinations of records', () => {
    expect(pixel.destination({destination: 'tablename'})).to.equal('foobar_dataset.tablename')
    expect(pixel.destination({destination: 'foo.bar'})).to.equal('foo.bar')
  })

  it('logs bad destinations', () => {
    sinon.stub(logger, 'error')

    expect(pixel.destination({destination: ''})).to.be.null
    expect(logger.error).to.have.callCount(1)
    expect(logger.error.args[0][0]).to.match(/No destination in record:/)

    expect(pixel.destination({destination: 'one.two.three'})).to.be.null
    expect(logger.error).to.have.callCount(2)
    expect(logger.error.args[1][0]).to.match(/No destination in record:/)
  })

  it('formats table inserts', async () => {
    const record = await pixel.format(data)
    expect(record).to.have.keys('insertId', 'json')
    expect(record.insertId.length).to.be.above(10)
    expect(record.json).to.eql({
      timestamp: 1490827132,
      user_id: 'the-user-id',
      key: 'the-key',
      canonical: 'the-canonical',
      remote_agent: 'the-user-agent',
      remote_referrer: 'the-referer',
      remote_ip: '127.0.0.0',
      city_geoname_id: null,
      country_geoname_id: null,
      postal_code: null,
      latitude: null,
      longitude: null,
    })
  })

  it('generates unique user ids when not set', async () => {
    const record1 = await pixel.format({...data, userId: undefined})
    const record2 = await pixel.format({...data, userId: undefined, key: 'other-key'})
    const record3 = await pixel.format({...data, userId: undefined, canonical: 'other-canonical'})
    const record4 = await pixel.format({...data, userId: undefined, remoteIp: '127.0.0.2'})
    expect(record1.json.user_id).to.equal(record2.json.user_id)
    expect(record1.json.user_id).to.equal(record3.json.user_id)
    expect(record1.json.user_id).not.to.equal(record4.json.user_id)
  })

  it('generates unique insert ids', async () => {
    const record1 = await pixel.format(data)
    const record2 = await pixel.format({...data})
    const record3 = await pixel.format({...data, key: 'other-key'})
    const record4 = await pixel.format({...data, canonical: 'other-canonical'})
    const record5 = await pixel.format({...data, timestamp: 123456789})
    expect(record1.insertId).to.equal(record2.insertId)
    expect(record1.insertId).not.to.equal(record3.insertId)
    expect(record1.insertId).not.to.equal(record4.insertId)
    expect(record1.insertId).not.to.equal(record5.insertId)
  })

  it('inserts nothing', async () => {
    const result = await pixel.insert()
    expect(result.length).to.equal(0)
  })

  it('inserts download records', async () => {
    let inserts = {}
    sinon.stub(bigquery, 'insert').callsFake(async (ds, tbl, rows) => {
      inserts[`${ds}.${tbl}`] = rows
      return rows.length
    })

    const pixel2 = new PixelTrackers([
      {type: 'download', key: '1'},
      {type: 'pixel', key: '2', destination: 'foo.bar'},
      {type: 'pixel', key: '3', destination: 'bar'},
      {type: 'pixel', key: '4', destination: 'foobar_dataset.bar'},
      {type: 'pixel', key: '5', destination: 'bar'},
      {type: 'pixel', key: '6', destination: 'foo.bar'},
    ])
    const results = await pixel2.insert()
    expect(results.length).to.equal(2)
    expect(results[0]).to.eql({count: 2, dest: 'foo.bar'})
    expect(results[1]).to.eql({count: 3, dest: 'foobar_dataset.bar'})
  })

  it('catches table 404 errors', async () => {
    sinon.stub(logger, 'error')
    sinon.stub(bigquery, 'insert').callsFake(async () => {
      const err = new Error('Table not found')
      err.code = 404
      throw err
    })

    const pixel2 = new PixelTrackers([{type: 'pixel', destination: 'foo.bar'}])
    const results = await pixel2.insert()
    expect(results.length).to.equal(1)
    expect(results[0]).to.eql({count: 0, dest: 'foo.bar'})

    expect(logger.error).to.have.callCount(1)
    expect(logger.error.args[0][0]).to.match(/Table not found: foo.bar/)
  })

  it('catches bigquery insert errors', async () => {
    sinon.stub(logger, 'error')
    sinon.stub(bigquery, 'insert').callsFake(async () => {
      const err = new Error('A failure occurred during this request.')
      err.name = 'PartialFailureError'
      err.response = {
        kind: 'bigquery#tableDataInsertAllResponse',
        insertErrors: [
          {errors: [{location: 'timestamp', message: 'no such field.'}]},
          {errors: [{location: 'other_thing', message: 'bad bad bad'}]},
          {errors: [{location: 'timestamp', message: 'no such field.'}]},
          {errors: [{message: 'just a message.'}]},
          {errors: [{reason: 'just a reason.'}]},
        ],
      }
      throw err
    })

    const pixel2 = new PixelTrackers([{type: 'pixel', destination: 'foo.bar'}])
    const results = await pixel2.insert()
    expect(results.length).to.equal(1)
    expect(results[0]).to.eql({count: 0, dest: 'foo.bar'})

    expect(logger.error).to.have.callCount(1)
    const msg = logger.error.args[0][0]
    expect(msg).to.match(/Insert errors on foo.bar:/)
    expect(msg).to.match(/timestamp: no such field, other_thing: bad bad bad, just a message, just a reason/)
  })

})
