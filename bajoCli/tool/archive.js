async function archive ({ path, args }) {
  const { archive } = this.bajoDbx.helper
  const { importPkg, print, startPlugin } = this.bajo.helper
  const prompts = await importPkg('bajoCli:@inquirer/prompts')
  const { confirm } = prompts
  const answer = await confirm({
    message: print.__('You\'re about to manually run archive tasks. Continue?'),
    default: false
  })
  if (!answer) {
    print.fail('Aborted!')
    process.kill(process.pid, 'SIGINT')
    return
  }
  await startPlugin('bajoDb')
  await archive()
}

export default archive
