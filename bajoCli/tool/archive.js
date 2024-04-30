async function move (task) {
  const { dayjs, print, spinner, importPkg, getConfig } = this.bajo.helper
  const { set } = this.bajo.helper._
  const { formatInteger } = this.bajoExtra.helper
  const { recordFind, recordCreate, recordRemove, statAggregate } = this.bajoDb.helper
  const prompts = await importPkg('bajoCli:@inquirer/prompts')
  const { confirm } = prompts
  // get relevant record
  print.info('Copying %s -> %s...', task.source, task.destination)
  const mark = dayjs().subtract(task.maxAge, 'day').toDate()
  const query = set({}, task.sourceField, { $lt: mark })
  let count = await statAggregate(task.source, { query }, { aggregate: 'count' })
  count = count[0].count
  if (count === 0) {
    print.warn('No record found, skipped')
    return
  }
  const config = getConfig()
  if (config.prompt !== false) {
    const answer = await confirm({
      message: print.__('%d records will be archived. Continue?', count),
      default: true
    })
    if (!answer) {
      print.warn('Task %s -> %s cancelled', task.source, task.destination)
      return
    }
  } else print.info('Archiving %d records will be archived', count)
  let page = 1
  let total = 0
  let affected = 0
  let error = 0
  let isMax = false
  const ids = []
  const spin = spinner({ showCounter: true }).start()
  for (;;) {
    const results = await recordFind(task.source, { query, page, limit: 100 }, { noCache: true, noHook: true })
    if (results.length === 0) break
    if (this.bajo.config.tool && total % 1000 === 0 && total !== 0) print.succeed('Milestone #%s records copied', formatInteger(total))
    for (const r of results) {
      if (task.maxRecords && task.maxRecords < total) {
        isMax = true
        break
      }
      total++
      try {
        await recordCreate(task.destination, r, { noSanitize: true, noHook: true, noResult: true, noCheckUnique: true })
        ids.push(r.id)
        spin.setText('ID #%s', r.id)
      } catch (err) {
        error++
      }
    }
    if (isMax) break
    affected = total - error
    page++
  }
  if (isMax) print.warn('Max of %d records reached', task.maxRecords)
  print.info('Removing %s...', task.source)
  for (const idx in ids) {
    const id = ids[idx]
    try {
      await recordRemove(task.source, id, { noHook: true, noResult: true })
      spin.setText('ID #%s', id)
      if (this.bajo.config.tool && idx % 1000 === 0 && idx !== '0') print.succeed('Milestone #%s records removed', formatInteger(idx))
    } catch (err) {}
  }
  spin.stop()
  print.info('Archiving %s -> %s ended, moved: %s, error: %s', task.source, task.destination, formatInteger(affected), formatInteger(error))
}

async function archive ({ path, args }) {
  const { importPkg, print, startPlugin, getConfig } = this.bajo.helper
  const prompts = await importPkg('bajoCli:@inquirer/prompts')
  const { confirm } = prompts
  const config = getConfig()
  if (config.prompt !== false) {
    const answer = await confirm({
      message: print.__('You\'re about to manually run archive tasks. Continue?'),
      default: false
    })

    if (!answer) print.fatal('Aborted!')
  }
  await startPlugin('bajoDb')
  const cfg = getConfig('bajoDbx')
  if (cfg.archive.tasks.length === 0) print.fatal('Nothing to archive')
  for (const t of cfg.archive.tasks) {
    if (t.maxAge < 1) {
      print.warn('Archive %s -> %s is disabled', t.source, t.destination)
      continue
    }
    await move.call(this, t)
  }
}

export default archive
