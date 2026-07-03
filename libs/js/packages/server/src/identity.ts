// Server identity: a persistent self-signed ECDSA P-256 certificate. node-
// datachannel pins DTLS to this cert via file paths; the certhash advertised in
// the address is sha256(cert DER) encoded as a multibase multihash (SPEC §3),
// so it must persist across restarts for the address to stay stable.

import 'reflect-metadata' // required by @peculiar/x509's tsyringe dependency
import * as x509 from '@peculiar/x509'
import { webcrypto, createHash, randomBytes } from 'node:crypto'
import { readFileSync, writeFileSync, existsSync } from 'node:fs'
import { encodeCerthash } from '@kpstreams/core'

const crypto = webcrypto as unknown as Crypto
x509.cryptoProvider.set(crypto)

export interface Identity {
  certPath: string
  keyPath: string
  /** sha256 of the certificate DER — the certhash digest (also the DTLS fingerprint). */
  digest: Uint8Array
  /** the `uEi...` multibase multihash advertised in the address. */
  certhash: string
}

function certhashOf(certPem: string): { digest: Uint8Array; certhash: string } {
  const der = new Uint8Array(new x509.X509Certificate(certPem).rawData)
  const digest = new Uint8Array(createHash('sha256').update(der).digest())
  return { digest, certhash: encodeCerthash(digest) }
}

// Load the cert/key from disk, or generate and persist a fresh self-signed pair.
export async function loadOrCreateIdentity(
  certPath = 'kps-cert.pem',
  keyPath = 'kps-key.pem'
): Promise<Identity> {
  if (existsSync(certPath) && existsSync(keyPath)) {
    const { digest, certhash } = certhashOf(readFileSync(certPath, 'utf8'))
    return { certPath, keyPath, digest, certhash }
  }

  const alg: EcKeyGenParams & { hash: string } = { name: 'ECDSA', namedCurve: 'P-256', hash: 'SHA-256' }
  const keys = await crypto.subtle.generateKey(alg, true, ['sign', 'verify'])
  const cert = await x509.X509CertificateGenerator.createSelfSigned({
    serialNumber: randomBytes(16).toString('hex'),
    name: 'CN=kps',
    notBefore: new Date(Date.now() - 60_000),
    notAfter: new Date(Date.now() + 10 * 365 * 24 * 60 * 60 * 1000),
    keys,
    signingAlgorithm: alg,
  })
  const certPem = cert.toString('pem')
  const pkcs8 = await crypto.subtle.exportKey('pkcs8', keys.privateKey)
  const keyPem = x509.PemConverter.encode(pkcs8, 'PRIVATE KEY')

  writeFileSync(certPath, certPem)
  writeFileSync(keyPath, keyPem, { mode: 0o600 })
  const { digest, certhash } = certhashOf(certPem)
  return { certPath, keyPath, digest, certhash }
}
