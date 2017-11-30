'use strict';

const support   = require('./support');
const timestamp = require('../lib/timestamp');

describe('timestamp', () => {

  it('converts milliseconds timestamps, when after the year 2000', () => {
    expect(timestamp.toEpochSeconds(1490827132000)).to.equal(1490827132);
    expect(timestamp.toEpochSeconds(1490827132999)).to.equal(1490827132);
    expect(timestamp.toEpochSeconds(946684800001)).to.equal(946684800);
    expect(timestamp.toEpochSeconds(946684700000)).to.equal(946684700000);
  });

  it('converts seconds timestamps, when before the year 2000', () => {
    expect(timestamp.toEpochMilliseconds(1490827132000)).to.equal(1490827132000);
    expect(timestamp.toEpochMilliseconds(1490827132999)).to.equal(1490827132999);
    expect(timestamp.toEpochMilliseconds(946684800001)).to.equal(946684800001);
    expect(timestamp.toEpochMilliseconds(946684700000)).to.equal(946684700000000);
  });

  it('leaves seconds alone', () => {
    expect(timestamp.toEpochSeconds(1490827132)).to.equal(1490827132);
    expect(timestamp.toEpochSeconds(0)).to.equal(0);
    expect(timestamp.toEpochSeconds(4102444800)).to.equal(4102444800);
  });

  it('gets a date string for milliseconds timestamps', () => {
    expect(timestamp.toDateString(1490827132000)).to.equal('20170329');
    expect(timestamp.toDateString(1490827132999)).to.equal('20170329');
    expect(timestamp.toDateString(1490831999999)).to.equal('20170329');
    expect(timestamp.toDateString(1490832000000)).to.equal('20170330');
    expect(timestamp.toDateString(946684800001)).to.equal('20000101');
    expect(timestamp.toDateString(946684700000)).to.equal('+0319690330');
  });

  it('gets a date string for second timestamps', () => {
    expect(timestamp.toDateString(1490827132)).to.equal('20170329');
    expect(timestamp.toDateString(0)).to.equal('19700101');
    expect(timestamp.toDateString(4102444800)).to.equal('21000101');
  });

  it('gets a date string without the Z', () => {
    expect(timestamp.toISOExtendedZ(1490827132)).to.equal('2017-03-29T22:38:52Z');
    expect(timestamp.toISOExtendedZ(4102444800)).to.equal('2100-01-01T00:00:00Z');
    expect(timestamp.toISOExtendedZ(1490827132010)).to.equal('2017-03-29T22:38:52Z');
  });

});
