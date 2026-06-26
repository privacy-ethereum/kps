// Wire format: "<ip>:<udp-port>:<certhash>" where certhash is the
// multibase-encoded sha-256 multihash (e.g. uEiD...). IPv6 hosts are bracketed:
// "[<ipv6>]:<port>:<certhash>" (the literal itself contains colons).

export interface Address {
  ip: string
  port: number
  certhash: string
}

export function parseAddress(s: string): Address {
  const malformed = () =>
    new Error(`address: malformed (expected <ip>:<port>:<certhash> or [ipv6]:<port>:<certhash>): ${s}`)

  let ip: string
  let rest: string
  if (s.startsWith('[')) {
    const end = s.indexOf(']')
    if (end < 0 || s[end + 1] !== ':') throw malformed()
    ip = s.slice(1, end)
    rest = s.slice(end + 2)
  } else {
    const i = s.indexOf(':')
    if (i < 0) throw malformed()
    ip = s.slice(0, i)
    rest = s.slice(i + 1)
  }

  // rest is "<port>:<certhash>"; the certhash never contains ':'.
  const j = rest.indexOf(':')
  if (j < 0) throw malformed()
  const port = Number(rest.slice(0, j))
  const certhash = rest.slice(j + 1)
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('address: port out of range')
  if (!ip || !certhash) throw malformed()
  return { ip, port, certhash }
}

export function formatAddress(addr: Address): string {
  const host = addr.ip.includes(':') ? `[${addr.ip}]` : addr.ip
  return `${host}:${addr.port}:${addr.certhash}`
}
