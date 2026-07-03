// kps demo: chat (with E2EE DMs) + eth-rpc explorer over kps streams.

import { dial, parseAddress } from '@kpstreams/webrtc-client'

const CHAT_STREAM = 'chat'
const RPC_STREAM = 'eth-rpc'
const BULLETIN = '__bulletin__'
const RPC_TIMEOUT_MS = 20_000

// If you run a public demo server, hardcode its address here. The same server
// is reachable over IPv4 and IPv6 (one dual-stack socket, same certhash);
// DEMO_ADDR (IPv4) is the default offered to first-time visitors.
const DEMO_ADDR = '170.64.236.147:60949:uEiCkUUDgDV1i0X-h7AS3yMiv_aVZV2C0vige93oBFa2l6Q'
const DEMO_ADDR_V6 = '[2400:6180:10:200::cca4:4000]:60949:uEiCkUUDgDV1i0X-h7AS3yMiv_aVZV2C0vige93oBFa2l6Q'
const DEMO_ADDRS = [
  { addr: DEMO_ADDR, label: 'Demo server (IPv4)' },
  { addr: DEMO_ADDR_V6, label: 'Demo server (IPv6)' },
]
const SAVED_ADDRS_KEY = 'kps-demo:saved-addresses'
const PERSISTENT_IDENTITY_KEY = 'kps-demo:persistent-identity'
const IDENTITY_KEY_STORAGE_KEY = 'kps-demo:identity-key'

const NETWORKS = [
  { key: 'ethereum', name: 'Ethereum', chainId: 1 },
  { key: 'base',     name: 'Base',     chainId: 8453 },
  { key: 'arbitrum', name: 'Arbitrum', chainId: 42161 },
  { key: 'optimism', name: 'Optimism', chainId: 10 },
  { key: 'polygon',  name: 'Polygon',  chainId: 137 }
]

const $ = (id) => document.getElementById(id)
const persistentIdentityInput = $('persistent-identity')
const addrInput = $('addr')
const addrSelect = $('addr-quick')
const connectBtn = $('connect')
const checkBtn = $('check')
const reachResultEl = $('reach-result')
const disconnectBtn = $('disconnect')
const statusEl = $('status')
const statusDot = $('status-dot')
const statusText = $('status-text')
const identityNameEl = $('identity-name')
const identityPeerEl = $('identity-peer')
const identityRefreshBtn = $('identity-refresh')
const rosterEl = $('roster')
const logEl = $('log')
const msgInput = $('msg')
const sendBtn = $('send')
const composeForm = $('compose')
const chatCardEl = $('chat-card')
const tabMessagesEl = $('tab-messages')
const tabExplorerEl = $('tab-explorer')
const chatPanelEl = $('chat-panel')
const explorerPanelEl = $('explorer-panel')
const explorerTitleEl = $('explorer-title')
const explorerSubEl = $('explorer-sub')
const explorerBodyEl = $('explorer-body')
const explorerSearchForm = $('explorer-search')
const explorerQueryInput = $('explorer-query')
const explorerRefreshBtn = $('explorer-refresh')
const landingView = $('landing')
const appView = $('app')
const tryItBtn = $('try-it')
const titleEl = $('title')

let conn = null
let chatStream = null
let myPeerId = null
let myName = ''

let rpcStream = null
let nextRpcId = 1
const pendingRpc = new Map()

let identityKey = null   // CryptoKeyPair (Ed25519) — { privateKey, publicKey }
let myIdPublicKeyB64 = null
let dmKeyPair = null
let dmPublicKeyB64 = null
let dmSignatureB64 = null
const sharedKeyCache = new Map()

const DM_SIG_DOMAIN = 'kps-webrtc-dm-key-v1:'
function dmSignPayload(rawDmPubKey) {
  const prefix = new TextEncoder().encode(DM_SIG_DOMAIN)
  const out = new Uint8Array(prefix.length + rawDmPubKey.length)
  out.set(prefix, 0)
  out.set(rawDmPubKey, prefix.length)
  return out
}

/** @type {Map<string, { peerId: string, idPublicKey: string, dmPublicKey: string, name: string }>} */
const roster = new Map()

/** @type {Map<string, { messages: Array<any>, unread: number }>} */
const conversations = new Map()

let selectedConvo = BULLETIN
let activeTab = 'messages'
let nextOutgoingId = 1

const ADJECTIVES = ['quiet', 'bold', 'clever', 'noble', 'wild', 'swift', 'shy', 'merry', 'bright', 'grumpy', 'keen', 'vivid', 'sleek', 'dusty', 'bouncy', 'crispy', 'gentle', 'fierce', 'tipsy', 'fancy', 'sneaky', 'cozy', 'sturdy', 'breezy', 'tiny', 'jolly', 'spry', 'mellow', 'zesty', 'plucky', 'hazy', 'nimble']
const NOUNS = ['raven', 'panda', 'fox', 'otter', 'moth', 'ox', 'lark', 'newt', 'wren', 'owl', 'quokka', 'yak', 'badger', 'crow', 'moose', 'heron', 'stoat', 'toad', 'seal', 'gecko', 'orca', 'hare', 'lynx', 'bison', 'snail', 'elk', 'squid', 'goose', 'axolotl', 'pangolin', 'walrus', 'narwhal']

function nameFromPeerId(peerId) {
  let hash = 2166136261 >>> 0
  for (let i = 0; i < peerId.length; i++) {
    hash = Math.imul(hash ^ peerId.charCodeAt(i), 16777619) >>> 0
  }
  const adj = ADJECTIVES[hash % ADJECTIVES.length]
  const noun = NOUNS[Math.floor(hash / ADJECTIVES.length) % NOUNS.length]
  return `${adj}-${noun}`
}

const b64enc = (u8) => btoa(String.fromCharCode(...u8))
const b64dec = (s) => Uint8Array.from(atob(s), c => c.charCodeAt(0))

function shortId(id) {
  if (typeof id !== 'string') return '?'
  return id.length > 10 ? `…${id.slice(-8)}` : id
}

function peerLabel(peerId) {
  if (peerId === myPeerId) return myName || 'you'
  return roster.get(peerId)?.name ?? shortId(peerId)
}

function setStatus(kind, text) {
  statusDot.className = `dot${kind ? ` ${kind}` : ''}`
  statusText.textContent = text
}

function getConvo(key) {
  let c = conversations.get(key)
  if (!c) {
    c = { messages: [], unread: 0 }
    conversations.set(key, c)
  }
  return c
}

function setConnected(connected) {
  connectBtn.disabled = connected
  disconnectBtn.disabled = !connected
  addrInput.disabled = connected
  chatCardEl.hidden = !connected
  if (!connected) {
    activeTab = 'messages'
    tabMessagesEl.classList.add('active')
    tabExplorerEl.classList.remove('active')
    chatPanelEl.hidden = false
    explorerPanelEl.hidden = true
    selectedConvo = BULLETIN
    msgInput.disabled = true
    sendBtn.disabled = true
    msgInput.placeholder = 'connect to start chatting'
  }
}

// ------- identity (Ed25519 via WebCrypto) -------

async function generateIdentityKeyPair() {
  return crypto.subtle.generateKey({ name: 'Ed25519' }, true, ['sign', 'verify'])
}

async function exportIdentityToJwk(pair) {
  const priv = await crypto.subtle.exportKey('jwk', pair.privateKey)
  const pub = await crypto.subtle.exportKey('jwk', pair.publicKey)
  return { priv, pub }
}

async function importIdentityFromJwk(jwk) {
  const privateKey = await crypto.subtle.importKey('jwk', jwk.priv, { name: 'Ed25519' }, true, ['sign'])
  const publicKey = await crypto.subtle.importKey('jwk', jwk.pub, { name: 'Ed25519' }, true, ['verify'])
  return { privateKey, publicKey }
}

async function rawIdPublicKey(pair) {
  return new Uint8Array(await crypto.subtle.exportKey('raw', pair.publicKey))
}

async function importPeerIdPubKey(rawB64) {
  return crypto.subtle.importKey('raw', b64dec(rawB64), { name: 'Ed25519' }, false, ['verify'])
}

async function generateDmKeyPair() {
  dmKeyPair = await crypto.subtle.generateKey(
    { name: 'ECDH', namedCurve: 'P-256' },
    true,
    ['deriveKey']
  )
  const rawPub = new Uint8Array(await crypto.subtle.exportKey('raw', dmKeyPair.publicKey))
  dmPublicKeyB64 = b64enc(rawPub)
  const sig = new Uint8Array(await crypto.subtle.sign(
    { name: 'Ed25519' }, identityKey.privateKey, dmSignPayload(rawPub)
  ))
  dmSignatureB64 = b64enc(sig)
}

async function importPeerDmPubKey(b64) {
  return crypto.subtle.importKey(
    'raw', b64dec(b64),
    { name: 'ECDH', namedCurve: 'P-256' }, false, []
  )
}

function sharedKeyFor(peerId) {
  let promise = sharedKeyCache.get(peerId)
  if (promise) return promise
  const entry = roster.get(peerId)
  if (!entry) return Promise.reject(new Error(`unknown peer ${peerId}`))
  promise = (async () => {
    const peerPub = await importPeerDmPubKey(entry.dmPublicKey)
    return crypto.subtle.deriveKey(
      { name: 'ECDH', public: peerPub },
      dmKeyPair.privateKey,
      { name: 'AES-GCM', length: 256 },
      false, ['encrypt', 'decrypt']
    )
  })()
  sharedKeyCache.set(peerId, promise)
  return promise
}

async function encryptFor(peerId, text) {
  const key = await sharedKeyFor(peerId)
  const iv = crypto.getRandomValues(new Uint8Array(12))
  const cipher = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv }, key, new TextEncoder().encode(text)
  )
  return { iv: b64enc(iv), ciphertext: b64enc(new Uint8Array(cipher)) }
}

async function decryptFrom(peerId, ivB64, ciphertextB64) {
  const key = await sharedKeyFor(peerId)
  const plain = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv: b64dec(ivB64) }, key, b64dec(ciphertextB64)
  )
  return new TextDecoder().decode(plain)
}

// ------- saved addresses -------

let savedAddrs = loadSavedAddrs()

function loadSavedAddrs() {
  try {
    const raw = localStorage.getItem(SAVED_ADDRS_KEY)
    if (raw) {
      const parsed = JSON.parse(raw)
      if (Array.isArray(parsed)) return parsed.filter(s => typeof s === 'string')
    }
  } catch {}
  return []
}

function rememberAddr(addr) {
  savedAddrs = [addr, ...savedAddrs.filter(s => s !== addr)].slice(0, 10)
  localStorage.setItem(SAVED_ADDRS_KEY, JSON.stringify(savedAddrs))
  renderAddrDropdown()
}

function shortAddrLabel(addr) {
  const m4 = addr.match(/^(\d+\.\d+\.\d+\.\d+):(\d+):/)
  if (m4) return `${m4[1]}:${m4[2]}`
  const m6 = addr.match(/^(\[[0-9a-fA-F:]+\]):(\d+):/)
  if (m6) return `${m6[1]}:${m6[2]}`
  return addr.length > 48 ? addr.slice(0, 45) + '…' : addr
}

function renderAddrDropdown() {
  const current = addrInput.value.trim()
  addrSelect.innerHTML = ''
  const placeholder = document.createElement('option')
  placeholder.value = ''
  placeholder.textContent = '— pick or paste below —'
  addrSelect.appendChild(placeholder)

  for (const d of DEMO_ADDRS) {
    const opt = document.createElement('option')
    opt.value = d.addr
    opt.textContent = `${d.label} · ${shortAddrLabel(d.addr)}`
    addrSelect.appendChild(opt)
  }

  const demoSet = new Set(DEMO_ADDRS.map(d => d.addr))
  const others = savedAddrs.filter(s => !demoSet.has(s))
  if (others.length > 0) {
    const group = document.createElement('optgroup')
    group.label = 'recent'
    for (const a of others) {
      const opt = document.createElement('option')
      opt.value = a
      opt.textContent = shortAddrLabel(a)
      group.appendChild(opt)
    }
    addrSelect.appendChild(group)
  }

  if (current && [...addrSelect.options].some(o => o.value === current)) {
    addrSelect.value = current
  }
}

addrSelect.addEventListener('change', () => {
  if (addrSelect.value) {
    addrInput.value = addrSelect.value
    setReachState('', '')
  }
})

addrInput.addEventListener('input', () => {
  if ([...addrSelect.options].some(o => o.value === addrInput.value)) {
    addrSelect.value = addrInput.value
  } else {
    addrSelect.value = ''
  }
})

renderAddrDropdown()
// Default to the most recent saved address, else the IPv4 demo server (not empty).
const defaultAddr = savedAddrs[0] || DEMO_ADDR
if (defaultAddr) {
  addrInput.value = defaultAddr
  if ([...addrSelect.options].some(o => o.value === defaultAddr)) {
    addrSelect.value = defaultAddr
  }
}

// ------- rpc -------

async function openRpcStream() {
  rpcStream = await openLineStream(conn, RPC_STREAM, (line) => {
    let resp
    try { resp = JSON.parse(line) } catch { return }
    const idNum = typeof resp.id === 'number' ? resp.id : null
    const pending = idNum != null ? pendingRpc.get(idNum) : null
    if (!pending) return
    pendingRpc.delete(idNum)
    clearTimeout(pending.timer)
    if (resp.error) pending.reject(new Error(resp.error.message || 'rpc error'))
    else pending.resolve(resp.result)
  })
  rpcStream.stream.closed.then(() => {
    for (const p of pendingRpc.values()) {
      clearTimeout(p.timer)
      p.reject(new Error('rpc stream closed'))
    }
    pendingRpc.clear()
    rpcStream = null
  })
}

async function rpc(network, method, params = []) {
  if (!rpcStream) throw new Error('rpc not connected')
  const id = nextRpcId++
  const envelope = { network, req: { jsonrpc: '2.0', id, method, params } }
  await rpcStream.send(envelope)
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      if (pendingRpc.has(id)) {
        pendingRpc.delete(id)
        reject(new Error(`rpc timeout: ${method} on ${network}`))
      }
    }, RPC_TIMEOUT_MS)
    pendingRpc.set(id, { resolve, reject, timer })
  })
}

// ------- UI -------

function renderSelfChip() {
  if (!myPeerId) return
  const me = document.createElement('div')
  me.className = 'sidebar-self'
  const name = document.createElement('div')
  name.className = 'name'
  name.textContent = myName || 'you'
  const sub = document.createElement('div')
  sub.className = 'sub'
  sub.textContent = shortId(myPeerId)
  me.appendChild(name)
  me.appendChild(sub)
  rosterEl.appendChild(me)
}

function renderRoster() {
  rosterEl.innerHTML = ''
  if (activeTab === 'explorer') {
    renderExplorerSidebar()
  } else {
    renderMessagesSidebar()
  }
}

function renderExplorerSidebar() {
  renderSelfChip()
  for (const n of NETWORKS) {
    const btn = document.createElement('button')
    btn.type = 'button'
    btn.className = `convo-item network${n.key === explorerNetwork ? ' active' : ''}`
    btn.dataset.key = n.key
    const text = document.createElement('div')
    text.className = 'convo-text'
    const nameEl = document.createElement('div')
    nameEl.className = 'name'
    nameEl.textContent = n.name
    text.appendChild(nameEl)
    const subEl = document.createElement('div')
    subEl.className = 'sub'
    subEl.textContent = `chain ${n.chainId}`
    text.appendChild(subEl)
    btn.appendChild(text)
    btn.addEventListener('click', () => openExplorer(n.key))
    rosterEl.appendChild(btn)
  }
}

function renderMessagesSidebar() {
  renderSelfChip()
  const entries = [
    { key: BULLETIN, label: 'bulletin', sub: 'public · not encrypted', kind: 'bulletin' },
    ...[...roster.values()]
      .filter(p => p.peerId !== myPeerId)
      .sort((a, b) => a.name.localeCompare(b.name))
      .map(p => ({ key: p.peerId, label: p.name, sub: shortId(p.peerId), kind: 'dm' }))
  ]
  for (const e of entries) {
    const btn = document.createElement('button')
    btn.type = 'button'
    btn.className = `convo-item${e.kind === 'bulletin' ? ' bulletin' : ''}${e.key === selectedConvo ? ' active' : ''}`
    btn.dataset.key = e.key
    const text = document.createElement('div')
    text.className = 'convo-text'
    const nameEl = document.createElement('div')
    nameEl.className = 'name'
    nameEl.textContent = e.label
    text.appendChild(nameEl)
    if (e.sub) {
      const subEl = document.createElement('div')
      subEl.className = 'sub'
      subEl.textContent = e.sub
      text.appendChild(subEl)
    }
    btn.appendChild(text)
    const convo = getConvo(e.key)
    if (convo.unread > 0 && e.key !== selectedConvo) {
      const badge = document.createElement('span')
      badge.className = 'unread'
      badge.textContent = String(convo.unread)
      btn.appendChild(badge)
    }
    btn.addEventListener('click', () => selectConvo(e.key))
    rosterEl.appendChild(btn)
  }

  if (myPeerId && roster.size <= 1) {
    const hint = document.createElement('div')
    hint.className = 'sidebar-hint'
    hint.textContent = 'no other peers yet — wait for someone else to connect'
    rosterEl.appendChild(hint)
  }
}

function setActiveTab(tab) {
  if (tab !== 'messages' && tab !== 'explorer') return
  activeTab = tab
  tabMessagesEl.classList.toggle('active', tab === 'messages')
  tabExplorerEl.classList.toggle('active', tab === 'explorer')
  if (tab === 'messages') {
    chatPanelEl.hidden = false
    explorerPanelEl.hidden = true
    renderRoster()
    renderLog()
  } else {
    chatPanelEl.hidden = true
    explorerPanelEl.hidden = false
    renderRoster()
    openExplorer(explorerNetwork ?? NETWORKS[0].key)
  }
}

function renderLog() {
  logEl.innerHTML = ''
  if (!chatStream) {
    const hint = document.createElement('div')
    hint.className = 'sys'
    hint.textContent = 'not connected'
    logEl.appendChild(hint)
    return
  }
  if (selectedConvo === BULLETIN) {
    const banner = document.createElement('div')
    banner.className = 'sys'
    banner.textContent = 'public bulletin — every message is visible to the server and all connected peers'
    logEl.appendChild(banner)
  } else if (!roster.has(selectedConvo)) {
    const banner = document.createElement('div')
    banner.className = 'sys'
    banner.textContent = `${peerLabel(selectedConvo)} has left`
    logEl.appendChild(banner)
  }
  const c = getConvo(selectedConvo)
  for (const m of c.messages) appendMessageEl(m)
  logEl.scrollTop = logEl.scrollHeight
}

function appendMessageEl(m) {
  if (m.kind === 'sys') {
    const el = document.createElement('div')
    el.className = 'sys'
    el.textContent = m.text
    logEl.appendChild(el)
    m.bubbleEl = el
    return
  }
  const row = document.createElement('div')
  row.className = `msg ${m.kind === 'out' ? 'me' : 'peer'}`
  if (m.kind === 'in' && m.label) {
    const label = document.createElement('div')
    label.className = 'label'
    label.textContent = m.label
    row.appendChild(label)
  }
  const bubble = document.createElement('div')
  let cls = 'bubble'
  if (m.status === 'pending') cls += ' pending'
  if (m.status === 'failed') cls += ' failed'
  bubble.className = cls
  bubble.textContent = m.text
  row.appendChild(bubble)
  logEl.appendChild(row)
  m.bubbleEl = bubble
}

function appendToOpenLog(m) {
  appendMessageEl(m)
  logEl.scrollTop = logEl.scrollHeight
}

// ------- block explorer -------

let explorerNetwork = null
let explorerView = null

function networkMeta(key) { return NETWORKS.find(n => n.key === key) }

function hexToInt(hex) {
  if (typeof hex !== 'string' || !hex.startsWith('0x')) return 0
  return Number.parseInt(hex, 16)
}

function formatTimeAgo(secUnix) {
  const diff = Math.floor(Date.now() / 1000) - secUnix
  if (diff < 0) return 'just now'
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

function shortHex(s, keepStart = 8, keepEnd = 6) {
  if (typeof s !== 'string' || s.length <= keepStart + keepEnd + 1) return s
  return `${s.slice(0, keepStart)}…${s.slice(-keepEnd)}`
}

function setExplorerBody(...nodes) {
  explorerBodyEl.innerHTML = ''
  for (const n of nodes) explorerBodyEl.appendChild(n)
}

function explorerLoading(text = 'loading…') {
  const el = document.createElement('div')
  el.className = 'explorer-loading'
  el.textContent = text
  return el
}

function explorerError(text) {
  const el = document.createElement('div')
  el.className = 'explorer-error'
  el.textContent = text
  return el
}

async function openExplorer(network) {
  explorerNetwork = network
  const meta = networkMeta(network)
  explorerTitleEl.textContent = meta?.name ?? network
  explorerSubEl.textContent = meta ? `chain ${meta.chainId}` : ''
  explorerQueryInput.value = ''
  if (activeTab === 'explorer') renderRoster()
  await refreshExplorerOverview()
}

async function refreshExplorerOverview() {
  const network = explorerNetwork
  if (!network) return
  explorerView = { kind: 'overview' }
  setExplorerBody(explorerLoading('loading latest blocks…'))
  let latestHex
  try {
    latestHex = await rpc(network, 'eth_blockNumber')
  } catch (err) {
    setExplorerBody(explorerError(`rpc error: ${err.message}`))
    return
  }
  const latest = hexToInt(latestHex)
  const blockCount = 10
  const reqs = []
  for (let i = 0; i < blockCount; i++) {
    const n = latest - i
    if (n < 0) break
    reqs.push(
      rpc(network, 'eth_getBlockByNumber', ['0x' + n.toString(16), false])
        .catch(err => ({ __err: err.message, number: '0x' + n.toString(16) }))
    )
  }
  const blocks = await Promise.all(reqs)
  if (explorerNetwork !== network || activeTab !== 'explorer') return
  renderExplorerOverview(latest, blocks)
}

function renderExplorerOverview(latest, blocks) {
  const frag = document.createDocumentFragment()
  const stats = document.createElement('div')
  stats.className = 'explorer-stats'
  const stat1 = document.createElement('div')
  stat1.className = 'explorer-stat'
  stat1.innerHTML = `<div class="stat-label">latest block</div><div class="stat-value">${latest.toLocaleString()}</div>`
  stats.appendChild(stat1)
  frag.appendChild(stats)
  const h = document.createElement('h4')
  h.className = 'explorer-section-title'
  h.textContent = 'recent blocks'
  frag.appendChild(h)
  const list = document.createElement('div')
  list.className = 'explorer-list'
  for (const b of blocks) {
    const row = document.createElement('button')
    row.type = 'button'
    row.className = 'explorer-row'
    if (b.__err) {
      row.textContent = `#${hexToInt(b.number).toLocaleString()} · error: ${b.__err}`
      row.disabled = true
    } else {
      const n = hexToInt(b.number)
      const ts = hexToInt(b.timestamp)
      const txs = Array.isArray(b.transactions) ? b.transactions.length : 0
      row.innerHTML = `
        <div class="row-primary">
          <span class="row-num">#${n.toLocaleString()}</span>
          <span class="row-hash">${shortHex(b.hash)}</span>
        </div>
        <div class="row-secondary">
          <span>${txs} tx</span>
          <span>${formatTimeAgo(ts)}</span>
        </div>
      `
      row.addEventListener('click', () => openBlock(n))
    }
    list.appendChild(row)
  }
  frag.appendChild(list)
  setExplorerBody(frag)
}

async function openBlock(number) {
  const network = explorerNetwork
  if (!network) return
  explorerView = { kind: 'block', number }
  setExplorerBody(explorerLoading(`loading block #${number.toLocaleString()}…`))
  let block
  try {
    block = await rpc(network, 'eth_getBlockByNumber', ['0x' + number.toString(16), true])
  } catch (err) {
    setExplorerBody(explorerError(`rpc error: ${err.message}`))
    return
  }
  if (!block) { setExplorerBody(explorerError('block not found')); return }
  renderBlockDetail(block)
}

function renderBlockDetail(block) {
  const frag = document.createDocumentFragment()
  const back = document.createElement('button')
  back.type = 'button'
  back.className = 'ghost explorer-back'
  back.textContent = '← back'
  back.addEventListener('click', () => openExplorer(explorerNetwork))
  frag.appendChild(back)
  const n = hexToInt(block.number)
  const ts = hexToInt(block.timestamp)
  const gasUsed = hexToInt(block.gasUsed)
  const gasLimit = hexToInt(block.gasLimit)
  const txs = Array.isArray(block.transactions) ? block.transactions : []
  const summary = document.createElement('div')
  summary.className = 'detail-card'
  summary.innerHTML = `
    <h3>Block #${n.toLocaleString()}</h3>
    <dl class="kv">
      <dt>hash</dt><dd class="mono">${block.hash}</dd>
      <dt>parent</dt><dd class="mono">${block.parentHash}</dd>
      <dt>miner</dt><dd class="mono">${block.miner || '—'}</dd>
      <dt>timestamp</dt><dd>${new Date(ts * 1000).toISOString()} (${formatTimeAgo(ts)})</dd>
      <dt>gas</dt><dd>${gasUsed.toLocaleString()} / ${gasLimit.toLocaleString()}</dd>
      <dt>transactions</dt><dd>${txs.length}</dd>
    </dl>
  `
  frag.appendChild(summary)
  if (txs.length > 0) {
    const h = document.createElement('h4')
    h.className = 'explorer-section-title'
    h.textContent = 'transactions'
    frag.appendChild(h)
    const list = document.createElement('div')
    list.className = 'explorer-list'
    for (const tx of txs) {
      const row = document.createElement('button')
      row.type = 'button'
      row.className = 'explorer-row'
      const valueWei = BigInt(tx.value || '0x0')
      const valueEth = Number(valueWei) / 1e18
      row.innerHTML = `
        <div class="row-primary">
          <span class="row-hash">${shortHex(tx.hash)}</span>
          <span class="row-value">${valueEth.toLocaleString(undefined, { maximumSignificantDigits: 4 })} Ξ</span>
        </div>
        <div class="row-secondary">
          <span class="mono">${shortHex(tx.from)}</span>
          <span>→</span>
          <span class="mono">${tx.to ? shortHex(tx.to) : '(contract creation)'}</span>
        </div>
      `
      row.addEventListener('click', () => openTx(tx.hash))
      list.appendChild(row)
    }
    frag.appendChild(list)
  }
  setExplorerBody(frag)
}

async function openTx(hash) {
  const network = explorerNetwork
  if (!network) return
  explorerView = { kind: 'tx', hash }
  setExplorerBody(explorerLoading(`loading tx ${shortHex(hash)}…`))
  let tx, receipt
  try {
    [tx, receipt] = await Promise.all([
      rpc(network, 'eth_getTransactionByHash', [hash]),
      rpc(network, 'eth_getTransactionReceipt', [hash])
    ])
  } catch (err) {
    setExplorerBody(explorerError(`rpc error: ${err.message}`))
    return
  }
  if (!tx) { setExplorerBody(explorerError('transaction not found')); return }
  renderTxDetail(tx, receipt)
}

function renderTxDetail(tx, receipt) {
  const frag = document.createDocumentFragment()
  const back = document.createElement('button')
  back.type = 'button'
  back.className = 'ghost explorer-back'
  back.textContent = '← back'
  back.addEventListener('click', () => {
    if (tx.blockNumber) openBlock(hexToInt(tx.blockNumber))
    else openExplorer(explorerNetwork)
  })
  frag.appendChild(back)
  const valueWei = BigInt(tx.value || '0x0')
  const valueEth = Number(valueWei) / 1e18
  const gas = hexToInt(tx.gas)
  const status = receipt ? (receipt.status === '0x1' ? 'success' : 'failed') : 'pending'
  const card = document.createElement('div')
  card.className = 'detail-card'
  card.innerHTML = `
    <h3>Transaction</h3>
    <dl class="kv">
      <dt>hash</dt><dd class="mono">${tx.hash}</dd>
      <dt>status</dt><dd>${status}</dd>
      <dt>block</dt><dd>${tx.blockNumber ? '#' + hexToInt(tx.blockNumber).toLocaleString() : '(pending)'}</dd>
      <dt>from</dt><dd class="mono">${tx.from}</dd>
      <dt>to</dt><dd class="mono">${tx.to ?? '(contract creation)'}</dd>
      <dt>value</dt><dd>${valueEth.toLocaleString(undefined, { maximumSignificantDigits: 6 })} Ξ</dd>
      <dt>gas limit</dt><dd>${gas.toLocaleString()}</dd>
      ${receipt ? `<dt>gas used</dt><dd>${hexToInt(receipt.gasUsed).toLocaleString()}</dd>` : ''}
      <dt>nonce</dt><dd>${hexToInt(tx.nonce)}</dd>
      ${tx.input && tx.input !== '0x' ? `<dt>input</dt><dd class="mono wrap">${tx.input.length > 260 ? tx.input.slice(0, 260) + '…' : tx.input}</dd>` : ''}
    </dl>
  `
  frag.appendChild(card)
  setExplorerBody(frag)
}

async function openAddress(addr) {
  const network = explorerNetwork
  if (!network) return
  explorerView = { kind: 'address', addr }
  setExplorerBody(explorerLoading(`loading ${shortHex(addr)}…`))
  let balance, code, nonce
  try {
    [balance, code, nonce] = await Promise.all([
      rpc(network, 'eth_getBalance', [addr, 'latest']),
      rpc(network, 'eth_getCode', [addr, 'latest']),
      rpc(network, 'eth_getTransactionCount', [addr, 'latest'])
    ])
  } catch (err) {
    setExplorerBody(explorerError(`rpc error: ${err.message}`))
    return
  }
  const frag = document.createDocumentFragment()
  const back = document.createElement('button')
  back.type = 'button'
  back.className = 'ghost explorer-back'
  back.textContent = '← back'
  back.addEventListener('click', () => openExplorer(explorerNetwork))
  frag.appendChild(back)
  const weiBig = BigInt(balance || '0x0')
  const eth = Number(weiBig) / 1e18
  const isContract = typeof code === 'string' && code !== '0x'
  const card = document.createElement('div')
  card.className = 'detail-card'
  card.innerHTML = `
    <h3>Address</h3>
    <dl class="kv">
      <dt>address</dt><dd class="mono">${addr}</dd>
      <dt>type</dt><dd>${isContract ? 'contract' : 'EOA'}</dd>
      <dt>balance</dt><dd>${eth.toLocaleString(undefined, { maximumSignificantDigits: 6 })} Ξ</dd>
      <dt>tx count</dt><dd>${hexToInt(nonce).toLocaleString()}</dd>
      ${isContract ? `<dt>code size</dt><dd>${((code.length - 2) / 2).toLocaleString()} bytes</dd>` : ''}
    </dl>
  `
  frag.appendChild(card)
  setExplorerBody(frag)
}

explorerRefreshBtn.addEventListener('click', () => {
  if (!explorerNetwork || !explorerView) return
  const v = explorerView
  let p
  if (v.kind === 'block') p = openBlock(v.number)
  else if (v.kind === 'tx') p = openTx(v.hash)
  else if (v.kind === 'address') p = openAddress(v.addr)
  else p = refreshExplorerOverview()
  p.catch((err) => console.warn(`refresh: ${err.message}`))
})

explorerSearchForm.addEventListener('submit', (e) => {
  e.preventDefault()
  const q = explorerQueryInput.value.trim()
  if (!q) return
  if (/^0x[0-9a-fA-F]{64}$/.test(q)) return openTx(q)
  if (/^0x[0-9a-fA-F]{40}$/.test(q)) return openAddress(q)
  if (/^\d+$/.test(q)) return openBlock(parseInt(q, 10))
  setExplorerBody(explorerError(`couldn't parse "${q}" — expected block #, 0x-tx hash, or 0x-address`))
})

function selectConvo(key) {
  selectedConvo = key
  const c = getConvo(key)
  c.unread = 0
  const composable = key === BULLETIN || roster.has(key)
  msgInput.disabled = !composable
  sendBtn.disabled = !composable
  msgInput.placeholder = composable
    ? key === BULLETIN ? 'post to the bulletin' : `message ${peerLabel(key)}`
    : `${peerLabel(key)} is offline`
  renderRoster()
  renderLog()
  if (!msgInput.disabled) msgInput.focus()
}

// ------- transport -------

// KPS streams are unnamed; the demo routes by writing a one-line protocol
// selector first ("chat" / "eth-rpc"), then newline-delimited JSON. Returns a
// small wrapper: send(obj) writes one JSON line; the read loop calls onLine.
async function openLineStream(conn, selector, onLine) {
  const stream = await conn.openStream()
  const writer = stream.writable.getWriter()
  const enc = new TextEncoder()
  await writer.write(enc.encode(selector + '\n'))
  ;(async () => {
    const reader = stream.readable.getReader()
    const dec = new TextDecoder()
    let buf = ''
    try {
      for (;;) {
        const { value, done } = await reader.read()
        if (done) break
        buf += dec.decode(value, { stream: true })
        let idx
        while ((idx = buf.indexOf('\n')) !== -1) {
          const line = buf.slice(0, idx)
          buf = buf.slice(idx + 1)
          if (line) onLine(line)
        }
      }
    } catch { /* stream ended or reset */ }
  })()
  return {
    stream,
    async send(obj) { await writer.write(enc.encode(JSON.stringify(obj) + '\n')) },
    async close() {
      try { await writer.close() } catch {}
      try { await stream.close() } catch {}
    }
  }
}

async function sendServer(obj) {
  if (!chatStream) throw new Error('not connected')
  await chatStream.send(obj)
}

async function handleServerMessage(obj) {
  if (obj.type === 'roster' && Array.isArray(obj.peers)) {
    const next = new Map()
    for (const p of obj.peers) {
      if (
        typeof p?.peerId !== 'string' ||
        typeof p?.idPublicKey !== 'string' ||
        typeof p?.dmPublicKey !== 'string' ||
        typeof p?.dmSignature !== 'string'
      ) continue
      if (p.peerId === myPeerId) {
        next.set(p.peerId, { peerId: p.peerId, idPublicKey: p.idPublicKey, dmPublicKey: p.dmPublicKey, name: typeof p.name === 'string' && p.name ? p.name : shortId(p.peerId) })
        continue
      }
      // Verify dmPublicKey is signed by idPublicKey, and that idPublicKey
      // is in fact the peerId (so the server can't route under a key it
      // doesn't have the private half of).
      let ok = false
      try {
        if (p.peerId !== p.idPublicKey) throw new Error('peerId/idPublicKey mismatch')
        const pubKey = await importPeerIdPubKey(p.idPublicKey)
        const rawDmPub = b64dec(p.dmPublicKey)
        const sig = b64dec(p.dmSignature)
        ok = await crypto.subtle.verify({ name: 'Ed25519' }, pubKey, sig, dmSignPayload(rawDmPub))
      } catch (err) {
        console.warn(`rejecting peer ${p.peerId}: ${err.message}`)
        continue
      }
      if (!ok) {
        console.warn(`rejecting peer ${p.peerId}: DM-key signature did not match identity key`)
        continue
      }
      const prev = roster.get(p.peerId)
      if (prev && prev.dmPublicKey !== p.dmPublicKey) sharedKeyCache.delete(p.peerId)
      next.set(p.peerId, { peerId: p.peerId, idPublicKey: p.idPublicKey, dmPublicKey: p.dmPublicKey, name: typeof p.name === 'string' && p.name ? p.name : shortId(p.peerId) })
    }
    roster.clear()
    for (const [k, v] of next) roster.set(k, v)
    if (selectedConvo !== BULLETIN) {
      const online = roster.has(selectedConvo)
      msgInput.disabled = !online
      sendBtn.disabled = !online
      msgInput.placeholder = online ? `message ${peerLabel(selectedConvo)}` : `${peerLabel(selectedConvo)} is offline`
    }
    renderRoster()
    renderLog()
  } else if (obj.type === 'bulletin' && typeof obj.from === 'string' && typeof obj.text === 'string') {
    const c = getConvo(BULLETIN)
    const msg = { kind: 'in', text: obj.text, label: peerLabel(obj.from) }
    c.messages.push(msg)
    if (selectedConvo === BULLETIN) appendToOpenLog(msg)
    else { c.unread += 1; renderRoster() }
  } else if (obj.type === 'dm' && typeof obj.from === 'string') {
    try {
      const text = await decryptFrom(obj.from, obj.iv, obj.ciphertext)
      const c = getConvo(obj.from)
      const msg = { kind: 'in', text, label: peerLabel(obj.from) }
      c.messages.push(msg)
      if (selectedConvo === obj.from) appendToOpenLog(msg)
      else { c.unread += 1; renderRoster() }
    } catch (err) {
      const c = getConvo(obj.from)
      const msg = { kind: 'sys', text: `[decrypt failed from ${peerLabel(obj.from)}: ${err.message}]` }
      c.messages.push(msg)
      if (selectedConvo === obj.from) appendToOpenLog(msg)
      else { c.unread += 1; renderRoster() }
    }
  } else if (obj.type === 'ack' && typeof obj.id === 'number') {
    markDeliveryStatus(obj.id, 'acked')
  } else if (obj.type === 'dm-fail' && typeof obj.id === 'number') {
    markDeliveryStatus(obj.id, 'failed')
  }
}

function markDeliveryStatus(id, status) {
  for (const c of conversations.values()) {
    const msg = c.messages.find(m => m.id === id)
    if (!msg) continue
    msg.status = status
    if (msg.bubbleEl) {
      msg.bubbleEl.classList.remove('pending')
      if (status === 'failed') msg.bubbleEl.classList.add('failed')
    }
    return
  }
}

// Chat reads are handled by openLineStream's onLine callback (see doConnect).

// ------- lifecycle -------

function isPersistentPrefEnabled() {
  return localStorage.getItem(PERSISTENT_IDENTITY_KEY) === '1'
}

async function initializeIdentity() {
  const persistent = isPersistentPrefEnabled()
  persistentIdentityInput.checked = persistent
  if (persistent) {
    const saved = localStorage.getItem(IDENTITY_KEY_STORAGE_KEY)
    if (saved) {
      try { identityKey = await importIdentityFromJwk(JSON.parse(saved)) } catch (err) {
        console.warn(`saved identity unreadable (${err.message}); using ephemeral this session — click refresh to overwrite`)
      }
    }
    if (!identityKey) {
      identityKey = await generateIdentityKeyPair()
      const jwk = await exportIdentityToJwk(identityKey)
      localStorage.setItem(IDENTITY_KEY_STORAGE_KEY, JSON.stringify(jwk))
    }
  } else {
    identityKey = await generateIdentityKeyPair()
  }
  await updateIdentityDisplay()
}

async function regenerateIdentity() {
  if (persistentIdentityInput.checked) {
    const ok = confirm(
      'Replace your persistent identity?\n\n' +
      'Your current peer ID and display name will be overwritten with a ' +
      'fresh one, and the new key will be saved to this browser. This ' +
      'cannot be undone.'
    )
    if (!ok) return
  }
  identityKey = await generateIdentityKeyPair()
  if (persistentIdentityInput.checked) {
    const jwk = await exportIdentityToJwk(identityKey)
    localStorage.setItem(IDENTITY_KEY_STORAGE_KEY, JSON.stringify(jwk))
  }
  await updateIdentityDisplay()
}

async function updateIdentityDisplay() {
  if (!identityKey) {
    identityNameEl.textContent = '—'
    identityPeerEl.textContent = '…'
    return
  }
  const raw = await rawIdPublicKey(identityKey)
  myIdPublicKeyB64 = b64enc(raw)
  myPeerId = myIdPublicKeyB64
  myName = nameFromPeerId(myPeerId)
  identityNameEl.textContent = myName
  identityPeerEl.textContent = shortId(myPeerId)
}

persistentIdentityInput.checked = isPersistentPrefEnabled()
window.addEventListener('pageshow', () => {
  persistentIdentityInput.checked = isPersistentPrefEnabled()
})
persistentIdentityInput.addEventListener('change', async () => {
  localStorage.setItem(PERSISTENT_IDENTITY_KEY, persistentIdentityInput.checked ? '1' : '0')
  if (persistentIdentityInput.checked) {
    const saved = localStorage.getItem(IDENTITY_KEY_STORAGE_KEY)
    if (saved) {
      try { identityKey = await importIdentityFromJwk(JSON.parse(saved)) } catch (err) {
        console.warn(`saved identity unreadable (${err.message}); keeping current in-memory key`)
      }
    } else {
      identityKey = await generateIdentityKeyPair()
      const jwk = await exportIdentityToJwk(identityKey)
      localStorage.setItem(IDENTITY_KEY_STORAGE_KEY, JSON.stringify(jwk))
    }
  } else {
    identityKey = await generateIdentityKeyPair()
  }
  await updateIdentityDisplay()
})

initializeIdentity().catch(err => console.error('identity init:', err))

identityRefreshBtn.addEventListener('click', () => {
  regenerateIdentity().catch(err => console.error('identity refresh:', err))
})

function setReachState(kind, text) {
  reachResultEl.hidden = !text
  reachResultEl.className = `reach-result${kind ? ` ${kind}` : ''}`
  reachResultEl.textContent = text
}

async function checkReachable() {
  const value = addrInput.value.trim()
  if (!value) { setReachState('err', 'enter an address first'); return }
  try { parseAddress(value) } catch (err) {
    setReachState('err', `bad address: ${err.message}`)
    return
  }
  checkBtn.disabled = true
  setReachState('working', 'probing…')
  const started = Date.now()
  let probeConn = null
  try {
    probeConn = await dial(value, { timeoutMs: 5000 })
    const elapsed = Date.now() - started
    setReachState('ok', `reachable · ${elapsed} ms (DTLS OK, certhash matched)`)
  } catch (err) {
    const elapsed = Date.now() - started
    setReachState('err', `unreachable after ${elapsed} ms: ${err.message}`)
  } finally {
    if (probeConn) try { await probeConn.close() } catch {}
    checkBtn.disabled = false
  }
}

async function doConnect() {
  const value = addrInput.value.trim()
  if (!value) return
  try { parseAddress(value) } catch (err) {
    setStatus('err', `bad address: ${err.message}`)
    return
  }
  setStatus('working', 'dialing...')
  try {
    if (!identityKey) await initializeIdentity()
    await generateDmKeyPair()

    conn = await dial(value, { timeoutMs: 15_000 })
    conn.closed.then(() => {
      setStatus('', 'disconnected')
      cleanup()
    })

    chatStream = await openLineStream(conn, CHAT_STREAM, (line) => {
      let obj
      try { obj = JSON.parse(line) } catch { return }
      handleServerMessage(obj).catch(err => console.error('server msg:', err))
    })
    chatStream.stream.closed.then(() => { chatStream = null })

    try { await openRpcStream() } catch (err) {
      console.warn(`rpc stream failed to open: ${err.message}`)
    }

    setStatus('ok', `connected · ${shortAddrLabel(value)}`)
    setConnected(true)
    setActiveTab('messages')
    selectConvo(BULLETIN)

    await sendServer({
      type: 'hello',
      idPublicKey: myIdPublicKeyB64,
      dmPublicKey: dmPublicKeyB64,
      dmSignature: dmSignatureB64,
      name: myName
    })

    rememberAddr(value)
  } catch (err) {
    setStatus('err', `connect failed: ${err.message}`)
    await cleanup()
  }
}

async function disconnect() {
  await cleanup()
  setStatus('', 'idle')
}

async function cleanup() {
  explorerNetwork = null
  explorerView = null
  for (const p of pendingRpc.values()) {
    clearTimeout(p.timer)
    p.reject(new Error('disconnected'))
  }
  pendingRpc.clear()
  if (rpcStream) { try { await rpcStream.close() } catch {} rpcStream = null }
  if (chatStream) { try { await chatStream.close() } catch {} chatStream = null }
  if (conn) { try { await conn.close() } catch {} conn = null }
  roster.clear()
  conversations.clear()
  sharedKeyCache.clear()
  dmKeyPair = null
  dmPublicKeyB64 = null
  dmSignatureB64 = null
  setConnected(false)
  renderRoster()
  renderLog()
}

async function sendMsg() {
  const text = msgInput.value.trim()
  if (!text || !chatStream) return
  msgInput.value = ''

  if (selectedConvo === BULLETIN) {
    const id = nextOutgoingId++
    const c = getConvo(BULLETIN)
    const msg = { kind: 'out', text, status: 'pending', id }
    c.messages.push(msg)
    appendToOpenLog(msg)
    try { await sendServer({ type: 'bulletin', text, id }) } catch {
      msg.status = 'failed'
      if (msg.bubbleEl) { msg.bubbleEl.classList.remove('pending'); msg.bubbleEl.classList.add('failed') }
    }
    return
  }

  if (!roster.has(selectedConvo)) return
  const id = nextOutgoingId++
  const c = getConvo(selectedConvo)
  const msg = { kind: 'out', text, status: 'pending', id }
  c.messages.push(msg)
  appendToOpenLog(msg)
  try {
    const { iv, ciphertext } = await encryptFor(selectedConvo, text)
    await sendServer({ type: 'dm', to: selectedConvo, iv, ciphertext, id })
  } catch {
    msg.status = 'failed'
    if (msg.bubbleEl) { msg.bubbleEl.classList.remove('pending'); msg.bubbleEl.classList.add('failed') }
  }
}

// ------- routing -------

function showApp() {
  landingView.hidden = true
  appView.hidden = false
  statusEl.hidden = false
  tryItBtn.hidden = true
  titleEl.classList.add('clickable')
  addrInput.focus()
  renderRoster()
  renderLog()
  if (window.location.hash !== '#demo') {
    history.replaceState(null, '', '#demo')
  }
}

async function showLanding() {
  await cleanup()
  appView.hidden = true
  landingView.hidden = false
  statusEl.hidden = true
  tryItBtn.hidden = false
  titleEl.classList.remove('clickable')
  setStatus('', 'idle')
  if (window.location.hash === '#demo') {
    history.replaceState(null, '', window.location.pathname + window.location.search)
  }
}

if (window.location.hash === '#demo') showApp()
window.addEventListener('hashchange', () => {
  if (window.location.hash === '#demo' && appView.hidden) showApp()
  else if (window.location.hash !== '#demo' && !appView.hidden) {
    showLanding().catch(err => console.error('showLanding:', err))
  }
})

tabMessagesEl.addEventListener('click', () => setActiveTab('messages'))
tabExplorerEl.addEventListener('click', () => setActiveTab('explorer'))
tryItBtn.addEventListener('click', showApp)
titleEl.addEventListener('click', () => {
  if (titleEl.classList.contains('clickable')) showLanding()
})
checkBtn.addEventListener('click', checkReachable)
connectBtn.addEventListener('click', doConnect)
disconnectBtn.addEventListener('click', disconnect)
composeForm.addEventListener('submit', (e) => { e.preventDefault(); sendMsg() })
addrInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !connectBtn.disabled) doConnect()
})

setStatus('', 'idle')
renderRoster()
renderLog()
