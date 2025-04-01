async function move (task) {
  const { importPkg } = this.app.bajo
  const { dayjs } = this.lib
  const { set } = this.lib._
  const { formatInteger } = this.app.bajoExtra
  const { recordFind, recordCreate, recordRemove, statAggregate } = this.app.bajoDb
  const prompts = await importPkg('bajoCli:@inquirer/prompts')
  const { confirm } = prompts
  // get relevant record
  this.print.info('Copying %s -> %s...', task.source, task.destination)
  const mark = dayjs().subtract(task.maxAge, 'day').toDate()
  const query = set({}, task.sourceField, { $lt: mark })
  let count = await statAggregate(task.source, { query }, { aggregate: 'count' })
  count = count[0].count
  if (count === 0) {
    this.print.warn('No record found, skipped')
    return
  }
  if (this.config.prompt !== false) {
    const answer = await confirm({
      message: this.print.write('%d records will be archived. Continue?', count),
      default: true
    })
    if (!answer) {
      this.print.warn('Task %s -> %s cancelled', task.source, task.destination)
      return
    }
  } else this.print.info('%d records will be archived', count)
  let page = 1
  let total = 0
  let affected = 0
  let error = 0
  let isMax = false
  const ids = []
  const spin = this.print.spinner({ showCounter: true }).start()
  for (;;) {
    const results = await recordFind(task.source, { query, page, limit: 100 }, { noCache: true, noHook: true })
    if (results.length === 0) break
    if (this.bajo.config.tool && total % 1000 === 0 && total !== 0) this.print.succeed('Milestone #%s records copied', formatInteger(total))
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
  if (isMax) this.print.warn('Max of %d records reached', task.maxRecords)
  this.print.info('Removing %s...', task.source)
  for (const idx in ids) {
    const id = ids[idx]
    try {
      await recordRemove(task.source, id, { noHook: true, noResult: true })
      spin.setText('ID #%s', id)
      if (this.bajo.config.tool && idx % 1000 === 0 && idx !== '0') this.print.succeed('Milestone #%s records removed', formatInteger(idx))
    } catch (err) {}
  }
  spin.stop()
  this.print.info('Archiving %s -> %s ended, moved: %s, error: %s', task.source, task.destination, formatInteger(affected), formatInteger(error))
}

async function archive (...args) {
  const { importPkg } = this.app.bajo
  const prompts = await importPkg('bajoCli:@inquirer/prompts')
  const { confirm } = prompts
  if (this.config.prompt !== false) {
    const answer = await confirm({
      message: this.print.write('You\'re about to manually run task archiver. Continue?'),
      default: false
    })

    if (!answer) this.print.fatal('Aborted!')
  }
  await this.app.bajoDb.start()
  if (this.archivers.length === 0) this.print.fatal('Nothing to archive')
  for (const t of this.archivers.tasks) {
    if (t.maxAge < 1) {
      this.print.warn('Archive %s -> %s is disabled', t.source, t.destination)
      continue
    }
    await move.call(this, t)
  }
}

export default archive
