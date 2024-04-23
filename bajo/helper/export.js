import path from 'path'
import format from '../../lib/ndjson-csv-xlsx.js'
import { createGzip } from 'node:zlib'
import scramjet from 'scramjet'
import supportedExt from '../../lib/io-exts.js'

const { DataStream } = scramjet
const { json, ndjson, csv, xlsx } = format

async function getFile (dest, ensureDir) {
  const { fs, importPkg, error, getPluginDataDir } = this.bajo.helper
  const increment = await importPkg('add-filename-increment')
  let file
  if (path.isAbsolute(dest)) file = dest
  else {
    file = `${getPluginDataDir('bajoDbx')}/export/${dest}`
    fs.ensureDirSync(path.dirname(file))
  }
  file = increment(file, { fs: true })
  const dir = path.dirname(file)
  if (!fs.existsSync(dir)) {
    if (ensureDir) fs.ensureDirSync(dir)
    else throw error('Directory \'%s\' doesn\'t exist', dir)
  }
  let compress = false
  let ext = path.extname(file)
  if (ext === '.gz') {
    compress = true
    ext = path.extname(path.basename(file).replace('.gz', ''))
    // file = file.slice(0, file.length - 3)
  }
  if (!supportedExt.includes(ext)) throw error('Unsupported format \'%s\'', ext.slice(1))
  return { file, ext, compress }
}

async function getData ({ source, filter, count, stream, progressFn }) {
  let cnt = count ?? 0
  const { recordFind } = this.bajoDb.helper
  for (;;) {
    const batchStart = new Date()
    const { data, page } = await recordFind(source, filter, { dataOnly: false })
    if (data.length === 0) break
    cnt += data.length
    await stream.pull(data)
    if (progressFn) await progressFn.call(this, { batchNo: page, data, batchStart, batchEnd: new Date() })
    filter.page++
  }
  await stream.end()
  return cnt
}

function exportTo (source, dest, { filter = {}, ensureDir, useHeader = true, batch = 500, progressFn } = {}, opts = {}) {
  const { fs, error, getConfig } = this.bajo.helper
  const cfg = getConfig('bajoDbx')
  if (!this.bajoDb) throw error('Bajo DB isn\'t loaded')
  filter.page = 1
  batch = parseInt(batch) ?? 500
  if (batch > cfg.export.maxBatch) batch = cfg.export.maxBatch
  if (batch < 0) batch = 1
  filter.limit = batch
  const { merge } = this.bajo.helper._

  return new Promise((resolve, reject) => {
    const { getInfo } = this.bajoDb.helper
    let count = 0
    let file
    let ext
    let stream
    let compress
    let writer
    getInfo(source)
      .then(res => {
        return getFile.call(this, dest, ensureDir)
      })
      .then(res => {
        file = res.file
        ext = res.ext
        compress = res.compress
        writer = fs.createWriteStream(file)
        writer.on('error', err => {
          reject(err)
        })
        writer.on('finish', () => {
          resolve({ file, count })
        })
        stream = new DataStream()
        stream = stream.flatMap(items => (items))
        const pipes = []
        if (ext === '.json') pipes.push(json.stringify(opts))
        else if (['.ndjson', '.jsonl'].includes(ext)) pipes.push(ndjson.stringify(opts))
        else if (ext === '.csv') pipes.push(csv.stringify(merge({}, { headers: useHeader }, opts)))
        else if (ext === '.tsv') pipes.push(csv.stringify(merge({}, { headers: useHeader }, merge({}, opts, { delimiter: '\t' }))))
        else if (ext === '.xlsx') pipes.push(xlsx.stringify(merge({}, { header: useHeader }, opts)))
        if (compress) pipes.push(createGzip())
        DataStream.pipeline(stream, ...pipes).pipe(writer)
        return getData.call(this, { source, filter, count, stream, progressFn })
      })
      .then(cnt => {
        count = cnt
      })
      .catch(reject)
  })
}

export default exportTo
