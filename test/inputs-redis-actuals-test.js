'use strict'

const support = require('./support')
const RedisActuals = require('../lib/inputs/redis-actuals')

describe('redis-actuals', () => {

  beforeEach(() => process.env.REDIS_IMPRESSIONS_HOST = 'cluster://127.0.0.1:6379')

  it('recognizes impression records', () => {
    const act = new RedisActuals()
    expect(act.check({})).to.be.false
    expect(act.check({type: 'combined'})).to.be.false
    expect(act.check({type: 'combined', impressions: [{flightId: 1}]})).to.be.false
    expect(act.check({type: 'combined', impressions: [{flightId: 1, targetPath: ':'}]})).to.be.true
    expect(act.check({type: 'combined', impressions: [{flightId: 1, targetPath: ':', isDuplicate: true}]})).to.be.false
    expect(act.check({type: 'whatever', impressions: [{flightId: 1, targetPath: ':'}]})).to.be.false
    expect(act.check({type: 'postbytes', impressions: [{flightId: 1, targetPath: ':'}]})).to.be.true
  })

  it('inserts nothing for no records', async () => {
    const act = new RedisActuals()
    expect(await act.insert()).to.eql([])
  })

  it('inserts nothing for records without flight ids', async () => {
    const act = new RedisActuals([
      {type: 'combined', timestamp: 1490827132, impressions: [{targetPath: ':'}]},
    ])
    expect(act._records.length).to.equal(1)
    expect(await act.insert()).to.eql([])
  })

  it('inserts nothing for no redis host', async () => {
    process.env.REDIS_IMPRESSIONS_HOST = ''
    const act = new RedisActuals([
      {type: 'combined', timestamp: 1490827132, impressions: [{flightId: 1, targetPath: ':'}]}
    ])
    expect(act._records.length).to.equal(1)
    expect(await act.insert()).to.eql([])
  })

  it('does not increment duplicate records', async () => {
    const act = new RedisActuals([
      {type: 'combined', timestamp: 1490827132, remoteAgent: 'googlebot', impressions: [{flightId: 1, targetPath: ':'}]}
    ])
    expect(act._records.length).to.equal(1)
    expect(await act.insert()).to.eql([])
  })

  it('increments redis counts', async () => {
    const act = new RedisActuals([
      {type: 'combined', timestamp: 1490827132, impressions: [{flightId: 11, targetPath: ':'}]},
      {type: 'combined', timestamp: 1490827132, impressions: [{flightId: 11, targetPath: ':'}]},
      {type: 'combined', timestamp: 1490827132, impressions: [{flightId: 22, targetPath: ':'}]},
      {type: 'combined', timestamp: 1490927132, impressions: [{flightId: 11, targetPath: ':'}]},
      {type: 'combined', timestamp: 1491027132, impressions: [{flightId: 11, targetPath: ':'}]}
    ])

    expect(await act.insert()).to.eql([{count: 5, dest: 'cluster://127.0.0.1'}])

    const keys = await support.redisKeys('dovetail:impression:*')
    expect(keys.length).to.equal(3)
    expect(keys.sort()).to.eql([
      'dovetail:impression:2017-03-29:actuals',
      'dovetail:impression:2017-03-31:actuals',
      'dovetail:impression:2017-04-01:actuals'
    ])

    expect(await support.redisHgetAll('dovetail:impression:2017-03-29:actuals')).to.eql({
      '11': '2',
      '22': '1'
    })
    expect(await support.redisHgetAll('dovetail:impression:2017-03-31:actuals')).to.eql({
      '11': '1'
    })
    expect(await support.redisHgetAll('dovetail:impression:2017-04-01:actuals')).to.eql({
      '11': '1',
    })
  })

  it('expires redis keys', async () => {
    process.env.REDIS_IMPRESSIONS_TTL = 9876
    const act = new RedisActuals([
      {type: 'combined', timestamp: 1490827132, impressions: [{flightId: 11, targetPath: ':'}]}
    ])

    expect(await act.insert()).to.eql([{count: 1, dest: 'cluster://127.0.0.1'}])

    const keys = await support.redisKeys('dovetail:impression:*')
    expect(keys.length).to.equal(1)
    expect(keys.sort()).to.eql(['dovetail:impression:2017-03-29:actuals'])

    expect(await support.redisTTL('dovetail:impression:2017-03-29:actuals')).to.eql(9876)
  })

})
