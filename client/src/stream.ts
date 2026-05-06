// Stream — a single logical message stream over a kps connection.
// Wraps an RTCDataChannel. Message-oriented: every `send(data)` produces
// exactly one `'message'` event on the peer side.

const BUFFERED_AMOUNT_LOW_THRESHOLD = 1 << 20 // 1 MiB

export interface CloseInfo {
  reason: 'local' | 'remote' | 'error'
  error?: Error
}

export class Stream extends EventTarget {
  readonly name: string
  readonly closed: Promise<CloseInfo>

  #channel: RTCDataChannel
  #closeResolve!: (info: CloseInfo) => void
  #closeFired = false
  #queue: Uint8Array[] = []
  #waiters: Array<(v: IteratorResult<Uint8Array>) => void> = []
  #queueClosed = false

  constructor(channel: RTCDataChannel) {
    super()
    this.#channel = channel
    channel.binaryType = 'arraybuffer'
    channel.bufferedAmountLowThreshold = BUFFERED_AMOUNT_LOW_THRESHOLD
    this.name = channel.label

    this.closed = new Promise<CloseInfo>(res => { this.#closeResolve = res })

    channel.addEventListener('message', e => {
      const data = e.data as ArrayBuffer | string
      const buf = typeof data === 'string'
        ? new TextEncoder().encode(data)
        : new Uint8Array(data)
      this.#enqueue(buf)
      this.dispatchEvent(new MessageEvent('message', { data: buf }))
    })
    channel.addEventListener('close', () => {
      this.#fireClose({ reason: 'remote' })
    })
    channel.addEventListener('error', (e: Event) => {
      const err = (e as RTCErrorEvent).error ?? new Error('kps stream error')
      this.dispatchEvent(new ErrorEvent('error', { error: err, message: err.message }))
      this.#fireClose({ reason: 'error', error: err })
    })
  }

  send(data: Uint8Array | string): boolean {
    if (this.#channel.readyState !== 'open') {
      throw new Error(`kps: cannot send on stream '${this.name}' in state ${this.#channel.readyState}`)
    }
    if (typeof data === 'string') this.#channel.send(data)
    else this.#channel.send(data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer)
    return this.#channel.bufferedAmount < BUFFERED_AMOUNT_LOW_THRESHOLD
  }

  drain(): Promise<void> {
    if (this.#channel.bufferedAmount < BUFFERED_AMOUNT_LOW_THRESHOLD) return Promise.resolve()
    return new Promise(resolve => {
      const onLow = () => {
        this.#channel.removeEventListener('bufferedamountlow', onLow)
        resolve()
      }
      this.#channel.addEventListener('bufferedamountlow', onLow)
    })
  }

  async close(): Promise<void> {
    if (this.#channel.readyState === 'closed' || this.#channel.readyState === 'closing') {
      await this.closed
      return
    }
    this.#channel.close()
    this.#fireClose({ reason: 'local' })
    await this.closed
  }

  [Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
    return {
      next: (): Promise<IteratorResult<Uint8Array>> => {
        if (this.#queue.length) {
          return Promise.resolve({ value: this.#queue.shift()!, done: false })
        }
        if (this.#queueClosed) {
          return Promise.resolve({ value: undefined as never, done: true })
        }
        return new Promise(res => this.#waiters.push(res))
      },
      return: (): Promise<IteratorResult<Uint8Array>> => {
        this.#queueClosed = true
        return Promise.resolve({ value: undefined as never, done: true })
      }
    }
  }

  #enqueue(buf: Uint8Array): void {
    const w = this.#waiters.shift()
    if (w) w({ value: buf, done: false })
    else this.#queue.push(buf)
  }

  #fireClose(info: CloseInfo): void {
    if (this.#closeFired) return
    this.#closeFired = true
    this.#queueClosed = true
    for (const w of this.#waiters) w({ value: undefined as never, done: true })
    this.#waiters = []
    this.#closeResolve(info)
    this.dispatchEvent(new Event('close'))
  }
}
