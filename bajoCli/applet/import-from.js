import _path from 'path'

const batch = 100

function makeProgress (spin) {
  const { secToHms } = this.app.bajo
  return async function ({ batchNo, data, batchStart, batchEnd } = {}) {
    spin.setText('Batch #%d (%s)', batchNo, secToHms(batchEnd.toTime() - batchStart.toTime(), true))
  }
}

async function importFrom (...args) {
  const { importPkg } = this.app.bajo
  const { isEmpty, map } = this.app.bajo.lib._
  const { getInfo, start } = this.app.bajoDb

  const [input, select, confirm] = await importPkg('bajoCli:@inquirer/input',
    'bajoCli:@inquirer/select', 'bajoCli:@inquirer/confirm')
  const schemas = map(this.app.bajoDb.schemas, 'name')
  if (isEmpty(schemas)) return this.print.fatal('No schema found!')
  let [dest, coll] = args
  if (isEmpty(dest)) {
    dest = await input({
      message: this.print.write('Please enter source file:'),
      validate: (item) => !isEmpty(item)
    })
  }
  if (isEmpty(coll)) {
    coll = await select({
      message: this.print.write('Please choose collection:'),
      choices: map(schemas, s => ({ value: s }))
    })
  }
  const answer = await confirm({
    message: this.print.write('You\'re about to replace ALL records with the new ones. Are you really sure?'),
    default: false
  })
  if (!answer) return this.print.fatal('Aborted!')
  const spin = this.print.spinner({ showCounter: true }).start('Importing...')
  const progressFn = makeProgress.call(this, spin)
  const { connection } = getInfo(coll)
  await start(connection.name)
  try {
    const result = await importFrom(dest, coll, { batch, progressFn })
    spin.succeed('%d records successfully imported from \'%s\'', result.count, _path.resolve(result.file))
  } catch (err) {
    spin.fatal('Error: %s', err.message)
  }
}

export default importFrom
