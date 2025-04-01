const types = ['datetime', 'date', 'timestamp']

async function handler ({ item }) {
  const { join } = this.app.bajo
  const { getSchema } = this.app.bajoDb
  const { has } = this.lib._
  for (const f of ['source', 'destination']) {
    if (!has(item, f)) throw this.error('Task must have %s collection', f)
    const key = `${f}Field`
    item[key] = item[key] ?? 'createdAt'
    const schema = getSchema(item[f])
    const prop = schema.properties.find(p => p.name === item[key])
    if (!prop) throw this.error('Unknown field \'%s@%s\'', item[key], item[f])
    if (!types.includes(prop.type)) throw this.error('\'%s@%s (%s)\' is not supported (must be one of %s)', item[key], item[f], prop.type, join(types))
  }
  if (item.source === item.destination) throw this.error('Source & destination must be different')
  item.maxAge = item.maxAge ?? 1 // in days, less then 1 is ignored
}

async function init () {
  const { buildCollections } = this.app.bajo
  this.archivers = await buildCollections({ ns: this.name, handler, container: 'archive.tasks', dupChecks: ['source'], useDefaultName: false })
}

export default init
