import _path from 'path'

const batch = 100

function makeProgress (spin) {
  const { secToHms } = this.app.bajo
  return async function ({ batchNo, data, batchStart, batchEnd } = {}) {
    if (data.length === 0) return
    spin.setText('Batch #%d (%s)', batchNo, secToHms(batchEnd.toTime() - batchStart.toTime(), true))
  }
}

async function exportTo (...args) {
  const { importPkg } = this.app.bajo
  const { dayjs } = this.lib
  const { isEmpty, map } = this.lib._
  const { getInfo, start } = this.app.bajoDb

  const [input, select] = await importPkg('bajoCli:@inquirer/input',
    'bajoCli:@inquirer/select')
  const schemas = map(this.app.bajoDb.schemas, 'name')
  if (isEmpty(schemas)) return this.print.fatal('No schema found!')
  let [coll, dest, query] = args
  if (isEmpty(coll)) {
    coll = await select({
      message: this.print.write('Please choose collection:'),
      choices: map(schemas, s => ({ value: s }))
    })
  }
  if (isEmpty(dest)) {
    dest = await input({
      message: this.print.write('Please enter destination file:'),
      default: `${coll}-${dayjs().format('YYYYMMDD')}.ndjson`,
      validate: (item) => !isEmpty(item)
    })
  }
  if (isEmpty(query)) {
    query = await input({
      message: this.print.write('Please enter a query (if any):')
    })
  }
  const spin = this.print.spinner().start('Exporting...')
  const progressFn = makeProgress.call(this, spin)
  const { connection } = getInfo(coll)
  await start(connection.name)
  try {
    const filter = { query }
    const result = await this.exportTo(coll, dest, { filter, batch, progressFn })
    spin.succeed('%d records successfully exported to \'%s\'', result.count, _path.resolve(result.file))
  } catch (err) {
    console.log(err)
    spin.fatal('Error: %s', err.message)
  }
}

export default exportTo
