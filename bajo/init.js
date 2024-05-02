const types = ['datetime', 'date', 'timestamp']

async function handler ({ item }) {
  const { error, join } = this.bajo.helper
  const { getSchema } = this.bajoDb.helper
  const { has } = this.bajo.helper._
  for (const f of ['source', 'destination']) {
    if (!has(item, f)) throw error('Task must have %s collection', f)
    const key = `${f}Field`
    item[key] = item[key] ?? 'createdAt'
    const schema = getSchema(item[f])
    const prop = schema.properties.find(p => p.name === item[key])
    if (!prop) throw error('Unknown field \'%s@%s\'', item[key], item[f])
    if (!types.includes(prop.type)) throw error('\'%s@%s (%s)\' is not supported (must be one of %s)', item[key], item[f], prop.type, join(types))
  }
  if (item.source === item.destination) throw error('Source & destination must be different')
  item.maxAge = item.maxAge ?? 1 // in days, less then 1 is ignored
}

async function init () {
  const { buildCollections } = this.bajo.helper
  await buildCollections({ handler, container: 'archive.tasks', dupChecks: ['source'], useDefaultName: false })
}

export default init
