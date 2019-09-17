'use strict';

const url = require('url');
const logger = require('../logger');

const domains = {};
try {
  const list = require('../../db/domainthreats.json');
  list.forEach(d => domains[d] = true);
} catch (e) {
  logger.warn('Could not load domainthreats.json - using empty domain database');
}

/**
 * Check if a referer comes from a threatening domain
 */
exports.look = async (referrerString) => {
  const parsed = url.parse(referrerString || '');
  if (parsed.host && domains[parsed.host]) {
    return true;
  } else {
    return false;
  }
};
