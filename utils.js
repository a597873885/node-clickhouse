const uuid = require('node-uuid')
const Utils = {
  getUuid() {
    return uuid.v1()
  }
}

module.exports = Utils