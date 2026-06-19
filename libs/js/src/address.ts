// Wire format: "<ip>:<udp-port>:<certhash>" where certhash is the
// multibase-encoded sha-256 multihash (e.g. uEiD...). IPv4 only for v0.

export interface Address {
  ip: string
  port: number
  certhash: string
}

export function parseAddress(s: string): Address {
  const m = s.match(/^(\d{1,3}(?:\.\d{1,3}){3}):(\d+):([A-Za-z0-9_-]+)$/)
  if (!m) throw new Error(`address: malformed (expected <ip>:<port>:<certhash>): ${s}`)
  const [, ip, portStr, certhash] = m
  for (const oct of ip.split('.')) {
    const n = Number(oct)
    if (!Number.isInteger(n) || n < 0 || n > 255) throw new Error(`address: bad IPv4 octet '${oct}'`)
  }
  const port = Number(portStr)
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error(`address: port out of range`)
  return { ip, port, certhash }
}

export function formatAddress(addr: Address): string {
  return `${addr.ip}:${addr.port}:${addr.certhash}`
}
