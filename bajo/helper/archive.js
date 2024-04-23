async function move (task) {
  const { dayjs, print, spinner } = this.bajo.helper
  const { set } = this.bajo.helper._
  const { formatInteger } = this.bajoExtra.helper
  const { recordFind, recordCreate, recordRemove } = this.bajoDb.helper
  // get relevant record
  const mark = dayjs().subtract(task.maxAge, 'day').toDate()
  const query = set({}, task.sourceField, { $lt: mark })
  let page = 1
  let total = 0
  let affected = 0
  let error = 0
  const ids = []
  const spin = spinner({ showCounter: true }).start()
  print.info('Copying %s -> %s...', task.source, task.destination)
  for (;;) {
    const results = await recordFind(task.source, { query, page, limit: 100 }, { noCache: true, noHook: true })
    if (results.length === 0) break
    if (this.bajo.config.tool && total % 1000 === 0 && total !== 0) print.succeed('Milestone #%s records copied', formatInteger(total))
    for (const r of results) {
      try {
        await recordCreate(task.destination, r, { noSanitize: true, noHook: true, noResult: true, noCheckUnique: true })
        ids.push(r.id)
        spin.setText('ID #%s', r.id)
      } catch (err) {
        error++
      }
    }
    total = total + results.length
    affected = total - error
    page++
  }
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

async function archive () {
  const { getConfig, print, log } = this.bajo.helper
  const cfg = getConfig('bajoDbx')
  if (cfg.archive.tasks.length === 0) print.warn('Nothing to archive')
  for (const t of cfg.archive.tasks) {
    if (t.maxAge < 1) {
      log.trace('Archive %s -> %s is disabled', t.source, t.destination)
      continue
    }
    await move.call(this, t)
  }
}

export default archive
