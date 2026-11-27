import fs from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const root = path.dirname(fileURLToPath(import.meta.url))
const queue = path.join(root, 'pending')
const trace = path.join(root, 'trace.jsonl')
let bytes = Buffer.alloc(0)
let expected
let active = false

function emit(type, data, id) {
  const frame = Buffer.from(JSON.stringify({ version: 1, type, data, id }))
  const size = Buffer.alloc(4)
  size.writeUInt32BE(frame.length)
  process.stdout.write(Buffer.concat([size, frame]))
}

function log(type, data) {
  fs.appendFileSync(trace, JSON.stringify({ type, data }) + '\n')
}

function ready(account) {
  emit('ready', { account, device: 'fixture-device', auth: { key: 'test-fixture' } })
}

function command({ type, id, data }) {
  switch (type) {
    case 'start':
      expected = data.expected
      log('start', { resumed: Boolean(data.auth) })
      if (data.auth) ready(expected)
      else emit('qr', { payload: 'test-fixture-qr' })
      break
    case 'activate': active = true; break
    case 'send':
      log('send', data)
      emit('response', { outcome: 'ok', id: data.id, chat: data.chat }, id)
      if (!fs.existsSync(path.join(root, 'silence'))) {
        emit('receipt', { id: data.id, chat: data.chat, timestamp: Math.floor(Date.now() / 1000), status: 'accepted' })
      }
      break
    case 'membership':
      emit('response', { outcome: 'ok', chat: data.chat, actor: data.actor, member: true }, id)
      break
    case 'media':
      log('media', { requested: data.locator.key.id })
      emit('response', { outcome: 'ok', bytes: data.locator.fixture_bytes ?? Buffer.from('fixture file').toString('base64') }, id)
      break
    default: log(type, data)
  }
}

process.stdin.on('data', chunk => {
  bytes = Buffer.concat([bytes, chunk])
  while (bytes.length >= 4) {
    const length = bytes.readUInt32BE()
    if (bytes.length < length + 4) return
    command(JSON.parse(bytes.subarray(4, length + 4)))
    bytes = bytes.subarray(length + 4)
  }
})
process.stdin.on('end', () => process.exit(0))

setInterval(() => {
  if (!expected) return
  for (const file of fs.readdirSync(queue).filter(file => file.endsWith('.json')).sort()) {
    const target = path.join(queue, file)
    const frame = JSON.parse(fs.readFileSync(target, 'utf8'))
    if (!active && frame.type !== 'pair') continue
    fs.unlinkSync(target)
    if (frame.type === 'pair') ready(frame.data.account ?? expected)
    else emit(frame.type, frame.data)
  }
}, 15)
