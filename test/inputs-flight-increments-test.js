'use strict';

const support = require('./support');
const logger = require('../lib/logger');
const FlightIncrements = require('../lib/inputs/flight-increments');

describe('flight-increments', () => {
  beforeEach(() => {
    process.env.DOVETAIL_ROUTER_HOSTS = 'host1.dt.test,host2.dt.test';
  });

  afterEach(() => {
    process.env.DOVETAIL_ROUTER_HOSTS = '';
  });

  it('recognizes impression records', () => {
    const incrs = new FlightIncrements();

    expect(incrs.check({})).to.be.false;
    expect(incrs.check({ type: 'postbytes', impressions: [] })).to.be.false;
    expect(incrs.check({ type: 'postbytes', impressions: [{ isDuplicate: true }] })).to.be.false;

    expect(incrs.check({ type: 'postbytes', impressions: [{}] })).to.be.true;
    expect(incrs.check({ type: 'postbytes', impressions: [{ isDuplicate: true }, {}] })).to.be.true;
  });

  it('inserts nothing', async () => {
    const incrs = new FlightIncrements([]);
    expect(await incrs.insert()).to.eql([]);
  });

  it('posts increments to dovetail routers', async () => {
    const day1 = '/api/v1/flight_increments/1970-01-01';
    const day2 = '/api/v1/flight_increments/1970-01-02';
    nock('https://host1.dt.test').post(day1).reply(202);
    nock('https://host2.dt.test').post(day1).reply(202);
    nock('https://host1.dt.test').post(day2).reply(202);
    nock('https://host2.dt.test').post(day2).reply(202);

    const incrs = new FlightIncrements([
      { type: 'postbytes', timestamp: 0, impressions: [{ flightId: 1 }] },
      {
        type: 'postbytes',
        timestamp: 0,
        impressions: [{ flightId: 2, isDuplicate: true }],
      },
      {
        type: 'postbytes',
        timestamp: 0,
        impressions: [{ flightId: 3 }, { flightId: 1 }, { flightId: 1 }],
      },
      {
        type: 'postbytes',
        timestamp: 24 * 60 * 60,
        impressions: [{ flightId: 4 }],
      },
    ]);

    const result = await incrs.insert();
    expect(result.length).to.equal(2);
    expect(result.map(r => r.dest).sort()).to.eql(['host1.dt.test', 'host2.dt.test']);
    expect(result.map(r => r.count)).to.eql([5, 5]);
  });

  it('complains about failed posts', async () => {
    const warns = [];
    sinon.stub(logger, 'warn').callsFake(msg => warns.push(msg));

    const day1 = '/api/v1/flight_increments/1970-01-01';
    nock('https://host1.dt.test').post(day1).times(3).reply(502);
    nock('https://host2.dt.test').post(day1).reply(404);

    const recs = [
      { type: 'postbytes', impressions: [{ flightId: 1 }] },
      { type: 'postbytes', impressions: [{ flightId: 2 }] },
    ];
    const incrs = new FlightIncrements(recs, 1000, 0);

    const result = await incrs.insert();
    expect(result.length).to.equal(2);
    expect(result.map(r => r.dest).sort()).to.eql(['host1.dt.test', 'host2.dt.test']);
    expect(result.map(r => r.count)).to.eql([2, 2]);

    expect(warns.length).to.equal(4);
    expect(warns.sort()[0]).to.include('PINGFAIL Error: HTTP 404 from https://host2.dt.test');
    expect(warns.sort()[1]).to.include('PINGFAIL Error: HTTP 502 from https://host1.dt.test');
    expect(warns.sort()[2]).to.include('PINGRETRY 502 https://host1.dt.test');
    expect(warns.sort()[3]).to.include('PINGRETRY 502 https://host1.dt.test');
  });

  it('does not ping duplicate records', async () => {
    const recs = [{ type: 'postbytes', remoteAgent: 'googlebot', impressions: [{ flightId: 1 }] }];
    const incrs = new FlightIncrements(recs);
    expect(incrs._records.length).to.equal(1);

    const result = await incrs.insert();
    expect(result.length).to.equal(2);
    expect(result.map(r => r.count)).to.eql([0, 0]);
  });
});
