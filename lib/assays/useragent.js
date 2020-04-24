'use strict';

const podagent = require('prx-podagent');
const logger = require('../logger');

/**
 * Lookup the agentid of the user agent string
 */
exports.look = (agentString) => {
  return new Promise((resolve, reject) => {
    try {
      podagent.parse(agentString, (err, agent) => {
        if (err) {
          console.error(`ERROR parsing podagent: ${err}`);
          reject(err);
        } else {
          if (agent) {
            resolve({name: agent.nameId, type: agent.typeId, os: agent.osId, bot: agent.bot});
          } else {
            resolve({name: null, type: null, os: null, bot: false});
          }
        }
      });
    } catch (err) {
      logger.warn(`Invalid agent string: ${agentString}`);
      resolve({name: null, type: null, os: null, bot: false});
    }
  });
};
