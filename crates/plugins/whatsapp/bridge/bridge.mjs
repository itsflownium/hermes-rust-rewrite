import { randomUUID } from 'node:crypto'

const output = process.stdout
const input = process.stdin
const VERSION = 1
const FRAME_LIMIT = 8 * 1024 * 1024
const MEDIA_LIMIT = 4 * 1024 * 1024
const HANDOFF_LIMIT = 128
const silent = () => {}
for (const level of ['log', 'debug', 'info', 'warn', 'error', 'trace']) console[level] = silent
const {
  default: makeSocket, BufferJSON, DisconnectReason, downloadMediaMessage, getAggregateVotesInPollMessage,
  initAuthCreds, jidNormalizedUser, normalizeMessageContent, proto,
} = await import('@whiskeysockets/baileys')
const logger = { level: 'silent', trace: silent, debug: silent, info: silent,
  warn: silent, error: silent, fatal: silent, child: () => logger }

let socket, expected, credentials, keys
let socketEpoch = 0
let opened = 0
let activated = false
let generation = 0
let burstMs = 0
let inputBuffer = Buffer.alloc(0)
let outputChain = Promise.resolve()
let inputEvents = Promise.resolve()
let backlog = 0
let transferring = 0
const handoffs = new Map()
const authenticating = new Map()
const messages = new Map()
const polls = new Map()
const burst = new Map()
const reserved = new Set()

function outputFrame(type, data, id) {
  const payload = Buffer.from(JSON.stringify({ version: VERSION, type, data, id }, BufferJSON.replacer))
  if (payload.length === 0 || payload.length > FRAME_LIMIT || transferring + payload.length > 24 * 1024 * 1024) {
    process.exit(2)
  }
  const header = Buffer.alloc(4)
  header.writeUInt32BE(payload.length)
  const packet = Buffer.concat([header, payload])
  transferring += packet.length
  outputChain = outputChain.then(() => new Promise((resolve, reject) => {
    output.write(packet, error => {
      transferring -= packet.length
      if (error) reject(error)
      else resolve()
    })
  }))
  return outputChain
}

const reply = (id, outcome, body = {}) => outputFrame('response', { outcome, ...body }, id)
const jid = value => typeof value === 'string' ? jidNormalizedUser(value) : ''
const group = value => value.endsWith('@g.us')
const phoneOrLid = value => /^\d{5,24}@(s\.whatsapp\.net|lid)$/.test(value)
const now = () => Math.floor(Date.now() / 1000)
const epoch = () => socketEpoch

function remember(raw) {
  const key = raw?.key
  if (!key?.id || !key?.remoteJid || !raw.message) return
  if (messages.size >= 600) messages.delete(messages.keys().next().value)
  messages.set(`${jid(key.remoteJid)}:${key.id}`, raw)
}

function authentication() {
  return { credentials, keys, polls: Array.from(polls) }
}

async function persistAuthentication() {
  const revision = ++generation
  if (!activated) {
    await outputFrame('credentials', { auth: authentication() })
    return
  }
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      authenticating.delete(revision)
      reject(new Error('Credential commit timed out'))
    }, 10000)
    authenticating.set(revision, () => { clearTimeout(timer); resolve() })
    outputFrame('credentials', { auth: authentication(), revision }).catch(reject)
  })
}

function signalKeys() {
  return {
    get: async (kind, ids) => Object.fromEntries(ids.flatMap(id => {
      const value = keys[kind]?.[id]
      if (!value) return []
      return [[id, kind === 'app-state-sync-key' ? proto.Message.AppStateSyncKeyData.fromObject(value) : value]]
    })),
    set: async changes => {
      for (const [kind, entries] of Object.entries(changes)) {
        const values = keys[kind] ??= Object.create(null)
        for (const [id, value] of Object.entries(entries)) {
          if (value == null) delete values[id]
          else values[id] = value
        }
      }
      await persistAuthentication()
    },
  }
}

function isSelf(value) {
  const who = jid(value)
  return Boolean(who) && [expected, jid(socket?.user?.id), jid(socket?.user?.lid)].includes(who)
}

async function isMember(chat, actor) {
  if (!group(chat)) return true
  const result = await socket.groupMetadata(chat)
  if (result.id !== chat) return false
  const found = person => result.participants.some(member => [member.id, member.lid, member.phoneNumber].some(id => jid(id) === person))
  return result.participants.some(member => [member.id, member.lid, member.phoneNumber].some(isSelf))
    && (!actor || found(actor))
}

const mediaType = [
  ['imageMessage', 'image'], ['videoMessage', 'video'], ['audioMessage', 'audio'], ['documentMessage', 'document'],
]

function permittedLocator(media) {
  if (media?.directPath && (!media.directPath.startsWith('/') || media.directPath.startsWith('//')
    || media.directPath.includes('..') || media.directPath.includes('\\'))) return false
  if (!media?.url) return typeof media?.directPath === 'string'
  try {
    const url = new URL(media.url)
    return url.protocol === 'https:' && !url.username && !url.password && !url.port
      && (url.hostname === 'whatsapp.net' || url.hostname.endsWith('.whatsapp.net'))
  } catch { return false }
}

async function received(raw, type, requestId) {
  const key = raw.key
  const chat = jid(key?.remoteJid)
  if (!key?.id || !chat || reserved.has(key.id) || chat === 'status@broadcast') return null
  const fromMe = key.fromMe === true
  const authored = fromMe && (type === 'notify' || (type === 'append' && !requestId
    && Number(raw.messageTimestamp) >= opened
    && raw.status === proto.WebMessageInfo.Status.SERVER_ACK))
  if ((!fromMe && type !== 'notify') || requestId) return null
  const actor = fromMe ? expected : jid(key.participant ?? key.remoteJid)
  if (!phoneOrLid(actor)) return null
  const message = normalizeMessageContent(raw.message)
  if (!message) return null
  remember(raw)
  const member = group(chat) ? undefined : true
  const context = message.extendedTextMessage?.contextInfo ?? mediaType.map(([tag]) => message[tag]?.contextInfo).find(Boolean)
  const media = mediaType.flatMap(([tag, kind]) => {
    const file = message[tag]
    const size = Number(file?.fileLength)
    if (!file || !Number.isSafeInteger(size) || size < 1 || size > MEDIA_LIMIT || !permittedLocator(file)) return []
    const minimal = { ...file, jpegThumbnail: null, contextInfo: null }
    const protobuf = Buffer.from(proto.Message.encode({ [tag]: minimal }).finish()).toString('base64')
    return [{ id: key.id, kind, voice: kind === 'audio' && file.ptt === true, name: file.fileName,
      locator: { protobuf, size, key: { id: key.id, remoteJid: chat, participant: key.participant, fromMe } } }]
  })
  const location = message.liveLocationMessage ?? message.locationMessage
  return {
    id: key.id, chat, actor, member, live: type === 'notify' || authored,
    origin: fromMe ? (authored ? 'owner' : 'ambiguous') : 'peer',
    actor_alias: jid(key.participantAlt ?? (!group(chat) ? key.remoteJidAlt : undefined)),
    chat_alias: jid(key.remoteJidAlt),
    text: message.conversation ?? message.extendedTextMessage?.text ?? message.imageMessage?.caption ?? message.videoMessage?.caption ?? message.documentMessage?.caption ?? '',
    quote: context?.stanzaId, media,
    mentions: context?.mentionedJid?.map(value => isSelf(value) ? expected : jid(value)),
    location: location ? { latitude: location.degreesLatitude, longitude: location.degreesLongitude,
      live: Boolean(message.liveLocationMessage) } : undefined,
  }
}

async function handoff(items) {
  if (!items.length) return
  if (handoffs.size >= HANDOFF_LIMIT) {
    await outputFrame('gap', { reason: 'handoff_backpressure' })
    process.exit(2)
  }
  const id = randomUUID()
  const envelope = { id, items }
  handoffs.set(id, envelope)
  if (activated) await outputFrame('handoff', envelope)
}

async function enqueue(item) {
  if (!burstMs || item.quote || item.choice || item.text?.startsWith('/')) return handoff([item])
  const key = `${item.chat}:${item.actor}`
  let current = burst.get(key)
  if (!current) {
    if (burst.size >= 32) return handoff([item])
    current = []
    burst.set(key, current)
    setTimeout(() => {
      if (burst.get(key) !== current) return
      burst.delete(key)
      void handoff(current).catch(() => process.exit(2))
    }, burstMs).unref()
  }
  current.push(item)
  if (current.length >= 16) {
    burst.delete(key)
    return handoff(current)
  }
}

const providerState = new Map([
  [proto.WebMessageInfo.Status.SERVER_ACK, 'accepted'], [proto.WebMessageInfo.Status.DELIVERY_ACK, 'delivered'],
  [proto.WebMessageInfo.Status.READ, 'read'], [proto.WebMessageInfo.Status.PLAYED, 'played'],
  [proto.WebMessageInfo.Status.ERROR, 'failed'],
])

function emitReceipt(key, status, participant, timestamp = now()) {
  if (!key?.fromMe || !key?.id || !key?.remoteJid || !status) return
  return outputFrame('receipt', { id: key.id, chat: jid(key.remoteJid), participant: jid(participant), timestamp, status })
}

async function votes(key, pollUpdates) {
  const item = polls.get(key.id)
  if (!item || jid(key.remoteJid) !== jid(item.message.key.remoteJid)) return
  for (const update of pollUpdates) {
    if (!update.pollUpdateMessageKey?.id || update.pollUpdateMessageKey.fromMe) continue
    const voter = jid(update.pollUpdateMessageKey.participant ?? update.pollUpdateMessageKey.remoteJid)
    const chat = jid(key.remoteJid)
    if (!phoneOrLid(voter) || !await isMember(chat, voter)) continue
    const options = getAggregateVotesInPollMessage({ message: item.message.message, pollUpdates: [update] }, expected)
    const selected = options.filter(option => option.voters.some(person => jid(person) === voter))
    if (selected.length !== 1) continue
    const choice = item.options.find(option => option.label === selected[0].name)
    if (!choice) continue
    await handoff([{ id: update.pollUpdateMessageKey.id, chat, actor: voter, quote: key.id, choice: choice.id,
      member: true, live: true, origin: 'peer', mentions: [expected] }])
  }
}

function startSocket() {
  const current = ++socketEpoch
  socket = makeSocket({
    auth: { creds: credentials, keys: signalKeys() }, logger, markOnlineOnConnect: false,
    syncFullHistory: false, shouldSyncHistoryMessage: () => false,
    getMessage: async key => (messages.get(`${jid(key.remoteJid)}:${key.id}`) ?? polls.get(key.id)?.message)?.message,
  })
  const listen = (event, handler) => socket.ev.on(event, data => {
    if (current !== epoch()) return
    Promise.resolve().then(() => handler(data)).catch(() => process.exit(2))
  })
  listen('creds.update', async update => {
    Object.assign(credentials, update)
    await persistAuthentication()
  })
  listen('connection.update', async update => {
    if (update.qr) await outputFrame('qr', { payload: update.qr })
    if (update.connection === 'open') {
      if (!isSelf(socket.user?.id) || ![jid(socket.user?.id), jid(socket.user?.lid)].includes(expected)) {
        await outputFrame('closed', { revoked: true })
        process.exit(3)
      }
      opened = now()
      const deviceJid = socket.user?.id ?? credentials.me?.id
      const device = String(deviceJid).split('@')[0].replace(':', '-')
      await outputFrame('ready', { account: expected, device, auth: authentication() })
    }
    if (update.connection === 'close') {
      const status = update.lastDisconnect?.error?.output?.statusCode
      if (status === DisconnectReason.restartRequired && !activated) startSocket()
      else {
        await outputFrame('closed', { revoked: status === DisconnectReason.loggedOut })
        process.exit(0)
      }
    }
  })
  listen('messages.upsert', ({ messages, type, requestId }) => {
    if (++backlog > 128) process.exit(2)
    inputEvents = inputEvents.then(async () => {
      try {
        if (current !== epoch() || !opened) return
        for (const message of messages) {
          const item = await received(message, type, requestId)
          if (item) await enqueue(item)
        }
      } finally { backlog-- }
    })
    return inputEvents
  })
  listen('messaging-history.set', () => outputFrame('gap', { reason: 'historical_sync' }))
  listen('messages.update', async updates => {
    for (const { key, update } of updates) {
      await emitReceipt(key, providerState.get(update.status))
      if (update.pollUpdates?.length) await votes(key, update.pollUpdates)
    }
  })
  listen('message-receipt.update', async updates => {
    for (const { key, receipt } of updates) {
      const time = Number(receipt.playedTimestamp ?? receipt.readTimestamp ?? receipt.receiptTimestamp)
      if (!receipt.userJid || !Number.isSafeInteger(time) || time <= 0) continue
      const state = receipt.playedTimestamp ? 'played' : receipt.readTimestamp ? 'read' : 'delivered'
      await emitReceipt(key, state, receipt.userJid, time)
    }
  })
  listen('group-participants.update', async update => {
    if (update.action !== 'remove') return
    for (const actor of update.participants) await outputFrame('member_left', { chat: jid(update.id), actor: jid(actor.id ?? actor) })
  })
}

function quote(info) {
  if (!info) return undefined
  return messages.get(`${jid(info.chat)}:${info.id}`) ?? {
    key: { remoteJid: info.chat, participant: info.participant ?? undefined, id: info.id, fromMe: info.from_me },
    message: { conversation: info.text ?? '' },
  }
}

async function send(id, data) {
  if (!activated || !opened) return reply(id, 'unavailable')
  let content
  let options
  if (data.content.poll) {
    const names = data.content.poll.choices.map(choice => choice.label)
    if (new Set(names).size !== names.length || names.length < 2 || names.length > 10) return reply(id, 'rejected')
    content = { poll: { name: data.content.poll.name, values: names, selectableCount: 1 } }
    options = data.content.poll.choices
  } else if (data.content.media) {
    const media = data.content.media
    if (!['image', 'video', 'audio', 'document'].includes(media.kind) || typeof media.bytes !== 'string'
      || media.bytes.length > Math.ceil(MEDIA_LIMIT / 3) * 4) return reply(id, 'rejected')
    content = { [media.kind]: Buffer.from(media.bytes, 'base64'), mimetype: media.mime }
    if (media.kind === 'audio') content.ptt = media.voice === true
    if (media.kind === 'document') content.fileName = media.name
  } else if (typeof data.content.text === 'string' && data.content.text.length > 0) {
    content = { text: data.content.text }
  } else return reply(id, 'rejected')
  if (reserved.size >= 2048) reserved.delete(reserved.values().next().value)
  reserved.add(data.id)
  let result
  try { result = await socket.sendMessage(data.chat, content, { messageId: data.id, quoted: quote(data.content.quote) }) }
  catch { return reply(id, 'unknown') }
  if (!result || result.key.id !== data.id || jid(result.key.remoteJid) !== data.chat) return reply(id, 'unknown')
  remember(result)
  if (options) {
    if (polls.size >= 128) polls.delete(polls.keys().next().value)
    polls.set(data.id, { message: result, options })
    await persistAuthentication()
  }
  return reply(id, 'ok', { chat: data.chat, id: data.id })
}

async function getMedia(id, { locator, limit }) {
  if (!activated || !opened || !Number.isSafeInteger(limit) || limit < 1 || limit > MEDIA_LIMIT
    || locator.size > limit || !locator.protobuf || !locator.key) return reply(id, 'rejected')
  const message = proto.Message.decode(Buffer.from(locator.protobuf, 'base64'))
  if (!mediaType.some(([tag]) => permittedLocator(message[tag]))) return reply(id, 'rejected')
  try {
    const stream = await downloadMediaMessage({ key: locator.key, message }, 'stream', {}, { logger,
      reuploadRequest: value => socket.updateMediaMessage(value) })
    const chunks = []
    let size = 0
    for await (const chunk of stream) {
      size += chunk.length
      if (size > limit) { stream.destroy(); return reply(id, 'rejected') }
      chunks.push(chunk)
    }
    return reply(id, 'ok', { bytes: Buffer.concat(chunks).toString('base64') })
  } catch { return reply(id, 'unknown') }
}

async function command({ type, id, data }) {
  switch (type) {
    case 'start':
      if (socket || !phoneOrLid(data.expected)) return process.exit(2)
      expected = data.expected
      burstMs = Number.isInteger(data.burst_ms) ? Math.min(1200, Math.max(0, data.burst_ms)) : 0
      credentials = data.auth?.credentials ?? initAuthCreds()
      keys = Object.assign(Object.create(null), data.auth?.keys)
      for (const [key, value] of data.auth?.polls ?? []) polls.set(key, value)
      return startSocket()
    case 'activate':
      activated = true
      for (const envelope of handoffs.values()) await outputFrame('handoff', envelope)
      return
    case 'persisted':
      authenticating.get(data.revision)?.()
      authenticating.delete(data.revision)
      return
    case 'commit':
      handoffs.delete(data.id)
      return
    case 'membership':
      try {
        return reply(id, 'ok', { chat: data.chat, actor: data.actor, member: await isMember(data.chat, data.actor) })
      } catch { return reply(id, 'unavailable') }
    case 'reading':
      if (data.read) await socket.readMessages([{ remoteJid: data.chat, participant: data.participant, id: data.id, fromMe: false }])
      if (data.typing) await socket.sendPresenceUpdate('composing', data.chat)
      return
    case 'send': return send(id, data)
    case 'media': return getMedia(id, data)
    default: return id == null ? undefined : reply(id, 'rejected')
  }
}

input.on('data', chunk => {
  inputBuffer = Buffer.concat([inputBuffer, chunk])
  while (inputBuffer.length >= 4) {
    const length = inputBuffer.readUInt32BE()
    if (length === 0 || length > FRAME_LIMIT) process.exit(2)
    if (inputBuffer.length < length + 4) break
    let frame
    try { frame = JSON.parse(inputBuffer.subarray(4, length + 4), BufferJSON.reviver) }
    catch { process.exit(2) }
    inputBuffer = inputBuffer.subarray(length + 4)
    if (frame.version !== VERSION) process.exit(2)
    Promise.resolve(command(frame)).catch(error => {
      if (frame.id != null) void reply(frame.id, 'unknown').catch(() => process.exit(2))
      else if (frame.type !== 'reading') process.exit(2)
    })
  }
})
input.on('end', () => process.exit(0))
output.on('error', () => process.exit(2))
