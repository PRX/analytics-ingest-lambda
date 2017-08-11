'use strict';
const podagent = require('prx-podagent');

/**
 * Lookup the agentid of the user agent string
 */
exports.look = (agentString) => {
  return new Promise((resolve, reject) => {
    podagent.parse(agentString, (err, agent) => {
      if (err) {
        console.error(`ERROR parsing podagent: ${err}`);
        reject(err);
      } else {
        if (agent) {
          resolve({name: agent.nameId, type: agent.typeId, os: agent.osId});
        } else {
          resolve({name: null, type: null, os: null});
        }
      }
    });
  });
};
