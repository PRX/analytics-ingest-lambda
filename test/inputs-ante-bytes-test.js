'use strict';

require('./support');
const dynamo = require('../lib/dynamo');
const AnteBytes = require('../lib/inputs/ante-bytes');

describe('ante-bytes', () => {

  it('recognizes ante-byte records', () => {
    const bytes = new AnteBytes();
    expect(bytes.check({})).to.be.false;
    expect(bytes.check({type: 'anything'})).to.be.false;
    expect(bytes.check({type: 'bytes'})).to.be.false;
    expect(bytes.check({type: 'antebytes'})).to.be.true;
    expect(bytes.check({type: 'antebytespreview'})).to.be.true;
  });

  it('recognizes and formats the antebytes record from dt-router', () => {

    const dtAntebytesRecord = {
      "time": "2020-01-23T12:10:43.177Z",
      "level": "info",
      "msg": "impression",
      "confirmed": true,
      "digest": "digest1",
      "download": {
        "adCount": 1,
        "cause": "something",
        "isDuplicate": false
      },
      "feederEpisode": "cc2db84c-a8f6-4146-a2d9-dec62ae54564",
      "feederPodcast": 213,
      "impressions": [
        {
          "isDuplicate": false,
          "placementsKey": "two",
          "segment": 2,
          "targetPath": ":Some-target",
          "zoneName": "test_feeder_pre_1"
        }
      ],
      "listenerEpisode": "le1",
      "type": "antebytes",
    }
    const bytes = new AnteBytes();
    expect(bytes.check(dtAntebytesRecord)).to.be.true;
    const formatted = bytes.format(dtAntebytesRecord);
    expect(formatted).to.eql({
      id: 'le1.digest1',
      feederEpisode: "cc2db84c-a8f6-4146-a2d9-dec62ae54564",
      feederPodcast: 213,
      confirmed: true,
      download: {adCount: 1},
      impressions: [{
          segment: 2,
          placementsKey: "two",
          targetPath: ":Some-target",
          zoneName: "test_feeder_pre_1"}
      ],
      level: "info",
      msg: "impression",
      time: "2020-01-23T12:10:43.177Z",
      type: "antebytes",
    });
  });

  it('formats dynamodb records', () => {
    const bytes = new AnteBytes();
    const formatted = bytes.format({
      listenerEpisode: 'le1',
      digest: 'digest1',
      something: 'else',
    });
    expect(formatted).to.eql({id: 'le1.digest1', something: 'else'});
  });

  it('removes duplicate flags', () => {
    const bytes = new AnteBytes();
    const formatted = bytes.format({
      listenerEpisode: 'le1',
      digest: 'digest1',
      download: {isDuplicate: true, cause: 'whatever', something: 'else'},
      impressions: [
        {segment: 0, isDuplicate: true, cause: 'anything'},
        {segment: 2, isDuplicate: true},
        {segment: 4, isDuplicate: false, cause: null},
      ]
    });
    expect(formatted).to.eql({
      id: 'le1.digest1',
      download: {something: 'else'},
      impressions: [{segment: 0}, {segment: 2}, {segment: 4}]
    });
  });

  it('inserts nothing', () => {
    return new AnteBytes().insert().then(result => {
      expect(result.length).to.equal(0);
    });
  });

  it('inserts ddb records', async () => {
    sinon.stub(dynamo, 'write').callsFake(recs => Promise.resolve(recs.length));

    const bytes = new AnteBytes([
      {type: 'combined', listenerEpisode: 'le1', digest: 'd1', test: 'one'},
      {type: 'antebytes', listenerEpisode: 'le2', digest: 'd2', test: 'two'},
      {type: 'antebytespreview', listenerEpisode: 'le3', digest: 'd3', test: 'three'},
      {type: 'postbytes', listenerEpisode: 'le4', digest: 'd4', test: 'four'},
      {type: 'postbytespreview', listenerEpisode: 'le5', digest: 'd5', test: 'five'},
      {type: 'antebytes', listenerEpisode: 'le2', digest: 'd2', test: 'duplicate'},
    ]);
    expect(await bytes.insert()).to.eql([{dest: 'dynamodb', count: 3}]);
    expect(dynamo.write).to.have.been.calledOnce;

    const recs = dynamo.write.args[0][0];
    expect(recs.length).to.equal(3);
    expect(recs[0]).to.eql({id: 'le2.d2', type: 'antebytes', test: 'two'});
    expect(recs[1]).to.eql({id: 'le3.d3', type: 'antebytespreview', test: 'three'});
    expect(recs[2]).to.eql({id: 'le2.d2', type: 'antebytes', test: 'duplicate'});
  });

});
