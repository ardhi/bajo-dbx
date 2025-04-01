import path from 'path'
import format from '../../lib/ndjson-csv-xlsx.js'
import { createGunzip } from 'zlib'
import supportedExt from '../../lib/io-exts.js'
import scramjet from 'scramjet'

const { DataStream } = scramjet
const { json, ndjson, csv, xlsx } = format

async function importFrom (source, dest, { trashOld = true, batch = 1, progressFn, converterFn, useHeader = true, fileType, createOpts = {} } = {}, opts = {}) {
  const { getPluginDataDir } = this.app.bajo
  const { recordCreate } = this.app.bajoDb
  const { merge } = this.lib._
  const { fs } = this.lib

  if (dest !== false) this.app.bajoDb.getInfo(dest) // make sure dest coll is valid
  let file
  if (path.isAbsolute(source)) file = source
  else {
    file = `${getPluginDataDir(this.name)}/import/${source}`
    fs.ensureDirSync(path.dirname(file))
  }
  if (!fs.existsSync(file)) throw this.error('Source file \'%s\' doesn\'t exist', file)
  let ext = fileType ? `.${fileType}` : path.extname(file)
  let decompress = false
  if (ext === '.gz') {
    ext = path.extname(path.basename(file, '.gz'))
    decompress = true
  }
  if (!supportedExt.includes(ext)) throw this.error('Unsupported format \'%s\'', ext.slice(1))
  if (trashOld && dest !== false) await this.app.bajoDb.collClear(dest)
  const reader = fs.createReadStream(file)
  batch = parseInt(batch) || 100
  if (batch > this.config.import.maxBatch) batch = this.config.import.maxBatch
  if (batch < 0) batch = 1
  let count = 0
  const pipes = [reader]
  if (decompress) pipes.push(createGunzip())
  if (ext === '.json') pipes.push(json.parse(opts))
  else if (['.ndjson', '.jsonl'].includes(ext)) pipes.push(ndjson.parse(opts))
  else if (ext === '.csv') pipes.push(csv.parse(merge({}, { headers: useHeader }, opts)))
  else if (ext === '.tsv') pipes.push(csv.parse(merge({}, { headers: useHeader }, merge({}, opts, { delimiter: '\t' }))))
  else if (ext === '.xlsx') pipes.push(xlsx.parse(merge({}, { header: useHeader }, opts)))

  const stream = DataStream.pipeline(...pipes)
  let batchNo = 1
  const data = []
  await stream
    .batch(batch)
    .map(async items => {
      if (items.length === 0) return null
      const batchStart = new Date()
      for (let item of items) {
        count++
        item = converterFn ? await converterFn.call(this, item) : item
        if (dest !== false) await recordCreate(dest, item, createOpts)
        else data.push(item)
      }
      if (progressFn) await progressFn.call(this, { batchNo, data: items, batchStart, batchEnd: new Date() })
      batchNo++
    })
    .run()

  return dest === false ? data : { file, count }
}

export default importFrom
