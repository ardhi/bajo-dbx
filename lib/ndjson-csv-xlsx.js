// Borrowed from: https://github.com/fanlia/ndjson-csv-xlsx/blob/main/index.js

import ndjson from 'ndjson'
import csv from 'fast-csv'
import xlsxparse from 'xlsx-parse-stream'
import XLSXWriteStream from '@atomictech/xlsx-write-stream'
import StreamArray from 'stream-json/streamers/StreamArray.js'
import stringer from 'stream-json/Stringer.js'
import disassembler from 'stream-json/Disassembler.js'
import chain from 'stream-chain'

export default {
  ndjson: {
    parse: (...args) => ndjson.parse(...args),
    stringify: (...args) => ndjson.stringify(...args)
  },
  csv: {
    parse: (...args) => csv.parse(...args),
    stringify: (...args) => csv.format(...args)
  },
  xlsx: {
    parse: (...args) => xlsxparse(...args),
    stringify: (...args) => new XLSXWriteStream(...args)
  },
  json: {
    parse: (...args) => chain([
      StreamArray.withParser(...args),
      data => data.value
    ]),
    stringify: (options, ...args) => chain([
      disassembler(),
      stringer({ ...options, makeArray: true })
    ])
  }
}
