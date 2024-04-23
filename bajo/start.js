async function start () {
  const { getConfig, log } = this.bajo.helper
  const { archive } = this.bajoDbx.helper
  const cfg = getConfig('bajoDbx')
  if (cfg.archive.checkInterval === false || cfg.archive.checkInterval <= 0) {
    log.warn('Automatic archive is disabled')
    return
  }
  if (cfg.archive.runEarly) await archive()
  this.bajoDbx.archiveIntv = setInterval(() => {
    archive().then().catch(err => {
      log.error('Archive error: %s', err.message)
    })
  }, cfg.archive.checkInterval * 60 * 1000)
}

export default start
