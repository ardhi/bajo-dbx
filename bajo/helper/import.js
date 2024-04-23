import path from 'path'
import format from '../../lib/ndjson-csv-xlsx.js'
import { createGunzip } from 'zlib'
import supportedExt from '../../lib/io-exts.js'
import scramjet from 'scramjet'

const { DataStream } = scramjet
const { json, ndjson, csv, xlsx } = format

async function importFrom (source, dest, { trashOld = true, batch = 1, progressFn, converterFn, useHeader = true, fileType, createOpts = {} } = {}, opts = {}) {
  const { fs, error, getConfig, getPluginDataDir } = this.bajo.helper
  if (dest !== false) {
    if (!this.bajoDb) throw error('Bajo DB isn\'t loaded')
    await this.bajoDb.helper.getInfo(dest)
  }
  const { merge } = this.bajo.helper._
  const cfg = getConfig('bajoDbx')

  let file
  if (path.isAbsolute(source)) file = source
  else {
    file = `${getPluginDataDir('bajoDbx')}/import/${source}`
    fs.ensureDirSync(path.dirname(file))
  }
  if (!fs.existsSync(file)) throw error('Source file \'%s\' doesn\'t exist', file)
  let ext = fileType ? `.${fileType}` : path.extname(file)
  let decompress = false
  if (ext === '.gz') {
    ext = path.extname(path.basename(file, '.gz'))
    decompress = true
  }
  if (!supportedExt.includes(ext)) throw error('Unsupported format \'%s\'', ext.slice(1))
  if (trashOld && dest !== false) await this.bajoDb.helper.collClear(dest)
  const reader = fs.createReadStream(file)
  batch = parseInt(batch) || 100
  if (batch > cfg.import.maxBatch) batch = cfg.import.maxBatch
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
        if (dest !== false) await this.bajoDb.helper.recordCreate(dest, item, createOpts)
        else data.push(item)
      }
      if (progressFn) await progressFn.call(this, { batchNo, data: items, batchStart, batchEnd: new Date() })
      batchNo++
    })
    .run()

  return dest === false ? data : { file, count }
}

export default importFrom
