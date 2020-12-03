const {BigQuery} = require('@google-cloud/bigquery')
const decrypt = require('./decrypt')

/**
 * Streaming inserts
 */
exports.insert = async (dataset, table, rows, retries = 2) => {
  if (rows.length === 0) {
    return 0
  }
  try {
    const client = await exports.client()
    const result = await client.dataset(dataset).table(table).insert(rows, {raw: true})
    return rows.length
  } catch (err) {
    if (err.message.match(/client_email/)) {
      throw new Error('You forgot to set BQ_CLIENT_EMAIL')
    } else if (err.message.match(/private_key/)) {
      throw new Error('You forgot to set BQ_PRIVATE_KEY')
    } else if (err.message.match(/PEM_read_bio/)) {
      throw new Error('You have a poorly formatted BQ_PRIVATE_KEY')
    } else if (err.message.match(/invalid_client/)) {
      throw new Error('Invalid BQ_CLIENT_EMAIL and/or BQ_PRIVATE_KEY')
    } else if (err.message.match(/without a project/)) {
      throw new Error('You forgot to set BQ_PROJECT_ID')
    } else if (err.message.match(/Not Found/)) {
      throw new Error(`Could not find table: ${err.response.req.path}`)
    } else if (retries > 0) {
      return exports.insert(dataset, table, rows, retries - 1)
    } else {
      throw err
    }
  }
}

/**
 * Insert with retries
 */
exports.client = async () => {
  const projectId = process.env.BQ_PROJECT_ID
  const client_email = process.env.BQ_CLIENT_EMAIL
  const private_key = await exports.key()
  return new BigQuery({projectId, credentials: {client_email, private_key}})
}

/**
 * Optionally decrypt the key, and cache the result
 */
let _key = null
exports.key = async (force = false) => {
  if (_key && !force) {
    return _key
  } else {
    const key = process.env.BQ_PRIVATE_KEY || ''
    if (key.match(/^"{0,1}-----/)) {
      return _key = key.replace(/\\n/g, '\n').replace(/"/, '')
    } else {
      const decrypted = await decrypt.decryptAws(key)
      return _key = decrypted.replace(/\\n/g, '\n')
    }
  }
}
