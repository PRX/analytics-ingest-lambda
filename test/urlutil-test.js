'use strict';

const support = require('./support');
const urlutil = require('../lib/urlutil');
const URI     = require('urijs');
const uuid    = require('uuid');

describe('urlutil', () => {

  const TEST_IMPRESSION = (key, val) => {
    let data = {
      url: '/99/the/path.mp3?foo=bar',
      feederPodcast: 1234,
      feederEpisode: 'episode-guid',
      remoteAgent: 'agent-string',
      remoteIp: '127.0.0.1, 127.0.0.2, 127.0.0.3',
      remoteReferrer: 'http://www.prx.org/',
      timestamp: 1507234920,
      listenerId: 'listener-id',
      listenerEpisode: 'listener-episode',
      listenerSession: 'listener-session',
      adId: 9,
      campaignId: 8,
      creativeId: 7,
      flightId: 6
    };
    if (key) { data[key] = val; }
    return data;
  };

  it('expands non-transformed params', () => {
    let nonTransforms = ['ad', 'agent', 'campaign', 'creative', 'episode',
      'flight', 'listener', 'listenerepisode', 'listenersession', 'podcast',
      'referer'];
    let url = urlutil.expand(`http://foo.bar/{?${nonTransforms.join(',')}}`, TEST_IMPRESSION());
    let params = URI(url).query(true);
    expect(url).to.match(/^http:\/\/foo\.bar\/\?/);
    expect(params.ad).to.equal('9');
    expect(params.agent).to.equal('agent-string');
    expect(params.campaign).to.equal('8');
    expect(params.creative).to.equal('7');
    expect(params.episode).to.equal('episode-guid');
    expect(params.flight).to.equal('6');
    expect(params.listener).to.equal('listener-id');
    expect(params.listenerepisode).to.equal('listener-episode');
    expect(params.listenersession).to.equal('listener-session');
    expect(params.podcast).to.equal('1234');
    expect(params.referer).to.equal('http://www.prx.org/');
  });

  it('expands non templates', () => {
    const url = 'https://some/host/i.png?foo=bar&anything=else';
    expect(urlutil.expand(url)).to.equal(url);
  });

  it('gets the md5 digest for the agent', () => {
    let url = urlutil.expand('http://foo.bar/?ua={agentmd5}', TEST_IMPRESSION());
    expect(url).to.equal('http://foo.bar/?ua=da08af6021d3ec8b8d27558ca92c314e');
  });

  it('cleans ip addresses', () => {
    let url1 = urlutil.expand('http://foo.bar/{?ip}', TEST_IMPRESSION());
    let url2 = urlutil.expand('http://foo.bar/{?ip}', TEST_IMPRESSION('remoteIp', '  what , 127.0.0.1'));
    let url3 = urlutil.expand('http://foo.bar/{?ip}', TEST_IMPRESSION('remoteIp', '  '));
    expect(url1).to.equal('http://foo.bar/?ip=127.0.0.1');
    expect(url2).to.equal('http://foo.bar/?ip=127.0.0.1');
    expect(url3).to.equal('http://foo.bar/');
  });

  it('returns timestamps in milliseconds', () => {
    let url1 = urlutil.expand('http://foo.bar/{?timestamp}', TEST_IMPRESSION());
    let url2 = urlutil.expand('http://foo.bar/{?timestamp}', TEST_IMPRESSION('timestamp', 1507234920010));
    expect(url1).to.equal('http://foo.bar/?timestamp=1507234920000');
    expect(url2).to.equal('http://foo.bar/?timestamp=1507234920010');
  });

  it('does not collide on 32 bit random ints very often', () => {
    let many = Array(1000).fill().map(() => {
      return urlutil.expand('{randomint}', TEST_IMPRESSION('requestUuid', uuid.v4()));
    });
    expect(new Set(many).size).to.equal(1000);
    many.forEach(url => {
      let num = parseInt(url);
      let bitCount = num.toString(2).match(/1/g).length;
      expect(num).to.be.above(0);
      expect(num).to.be.at.most(2147483647);
      expect(bitCount).to.be.at.most(32);
    });
  });

  it('returns random strings based on timestamp + listenerepisode + ad', () => {
    let url1 = urlutil.expand('http://foo.bar/{?randomstr}', TEST_IMPRESSION());
    let url2 = urlutil.expand('http://foo.bar/{?randomstr}', TEST_IMPRESSION());
    let url3 = urlutil.expand('http://foo.bar/{?randomstr}', TEST_IMPRESSION('timestamp', 9999999));
    let url4 = urlutil.expand('http://foo.bar/{?randomstr}', TEST_IMPRESSION('listenerEpisode', 'changed'));
    let url5 = urlutil.expand('http://foo.bar/{?randomstr}', TEST_IMPRESSION('adId', 8));
    expect(url1).to.equal(url2);
    expect(url1).not.to.equal(url3);
    expect(url1).not.to.equal(url4);
    expect(url1).not.to.equal(url5);
    expect(url3).not.to.equal(url4);
    expect(url4).not.to.equal(url5);
  });

  it('reassembles original request url', () => {
    let url = urlutil.expand('http://foo.bar/?ru={url}', TEST_IMPRESSION());
    expect(url).to.equal('http://foo.bar/?ru=dovetail.prxu.org%2F99%2Fthe%2Fpath.mp3%3Ffoo%3Dbar');
  });

  it('counts by hostname', () => {
    expect(urlutil.count({}, null)).to.eql({});
    expect(urlutil.count({start: 99}, undefined)).to.eql({start: 99});
    expect(urlutil.count({}, 'http://foo.gov/bar')).to.eql({'foo.gov': 1});
    expect(urlutil.count({'foo.gov': 10}, 'https://foo.gov/bar')).to.eql({'foo.gov': 11});
  });

});
