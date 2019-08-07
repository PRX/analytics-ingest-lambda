'use strict'

const support = require('./support')
const assayer = require('../lib/assayer')

describe('assayer', () => {

  describe('test', () => {

    it('returns basic info', async () => {
      const info = await assayer.test({remoteAgent: 'something'})
      expect(info).to.eql({
        isDuplicate: false,
        cause: null,
        geo: null,
        agent: {name: null, type: null, os: null, bot: false}
      })
    })

    it('optionally geo locates', async () => {
      const info = await assayer.test({remoteAgent: 'Pocket Casts', remoteIp: '127.0.0.1'}, true)
      expect(info).to.eql({
        isDuplicate: false,
        cause: null,
        geo: {city: null, country: null, latitude: null, longitude: null, masked: '127.0.0.0', postal: null},
        agent: {name: 16, type: 36, os: null, bot: false}
      })
    })

    it('checks the download duplicate', async () => {
      const info = await assayer.test({download: {isDuplicate: true, cause: 'foo'}, remoteAgent: 'googlebot'})
      expect(info.isDuplicate).to.equal(true)
      expect(info.cause).to.equal('foo')
    })

    it('has a default cause', async () => {
      const info = await assayer.test({download: {isDuplicate: true}, remoteAgent: 'googlebot'})
      expect(info.isDuplicate).to.equal(true)
      expect(info.cause).to.equal('unknown')
    })

    it('checks for bots', async () => {
      const info = await assayer.test({remoteAgent: 'googlebot'})
      expect(info.isDuplicate).to.equal(false) // TODO: bot-filter preview
      expect(info.cause).to.equal('bot')
    })

  })

  describe('testImpression', async () => {

    it('returns basic info', async () => {
      const info = await assayer.testImpression({remoteAgent: 'something'}, {})
      expect(info).to.eql({
        isDuplicate: false,
        cause: null,
        geo: null,
        agent: {name: null, type: null, os: null, bot: false}
      })
    })

    it('optionally geo locates', async () => {
      const info = await assayer.testImpression({remoteAgent: 'Pocket Casts', remoteIp: '127.0.0.1'}, {}, true)
      expect(info).to.eql({
        isDuplicate: false,
        cause: null,
        geo: {city: null, country: null, latitude: null, longitude: null, masked: '127.0.0.0', postal: null},
        agent: {name: 16, type: 36, os: null, bot: false}
      })
    })

    it('checks the download duplicate', async () => {
      const info = await assayer.testImpression({remoteAgent: 'googlebot'}, {isDuplicate: true, cause: 'foo'})
      expect(info.isDuplicate).to.equal(true)
      expect(info.cause).to.equal('foo')
    })

    it('has a default cause', async () => {
      const info = await assayer.testImpression({remoteAgent: 'googlebot'}, {isDuplicate: true})
      expect(info.isDuplicate).to.equal(true)
      expect(info.cause).to.equal('unknown')
    })

    it('checks for bots', async () => {
      const info = await assayer.testImpression({remoteAgent: 'googlebot'}, {isDuplicate: false})
      expect(info.isDuplicate).to.equal(false) // TODO: bot-filter preview
      expect(info.cause).to.equal('bot')
    })

  })

})
