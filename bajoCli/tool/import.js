import Path from 'path'

const batch = 100

function makeProgress (spin) {
  const { secToHms } = this.bajo.helper
  return async function ({ batchNo, data, batchStart, batchEnd } = {}) {
    spin.setText('Batch #%d (%s)', batchNo, secToHms(batchEnd.toTime() - batchStart.toTime(), true))
  }
}

async function importFrom ({ path, args }) {
  const { importPkg, print, getConfig, spinner, startPlugin } = this.bajo.helper
  const { isEmpty, map } = this.bajo.helper._
  const [input, select, confirm] = await importPkg('bajoCli:@inquirer/input',
    'bajoCli:@inquirer/select', 'bajoCli:@inquirer/confirm')
  const config = getConfig()
  if (!this.bajoDb) return print.fail('Bajo DB isn\'t loaded', { exit: config.tool })
  const schemas = map(this.bajoDb.schemas, 'name')
  if (isEmpty(schemas)) return print.fail('No schema found!', { exit: config.tool })
  let [dest, coll] = args
  if (isEmpty(dest)) {
    dest = await input({
      message: print.__('Please enter source file:'),
      validate: (item) => !isEmpty(item)
    })
  }
  if (isEmpty(coll)) {
    coll = await select({
      message: print.__('Please choose collection:'),
      choices: map(schemas, s => ({ value: s }))
    })
  }
  const answer = await confirm({
    message: print.__('You\'re about to replace ALL records with the new ones. Are you really sure?'),
    default: false
  })
  if (!answer) return print.fail('Aborted!', { exit: config.tool })
  const spin = spinner({ showCounter: true }).start('Importing...')
  const progressFn = makeProgress.call(this, spin)
  const { connection } = await this.bajoDb.helper.getInfo(coll)
  await startPlugin('bajoDb', connection.name)
  try {
    const result = await this.bajoDbx.helper.import(dest, coll, { batch, progressFn })
    spin.succeed('%d records successfully imported from \'%s\'', result.count, Path.resolve(result.file))
  } catch (err) {
    spin.fail('Error: %s', err.message, { exit: config.tool })
  }
}

export default importFrom
