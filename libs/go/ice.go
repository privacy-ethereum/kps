package kps

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
)

// deriveICEPwd computes the ICE password from the pinned certhash digest and
// the connection's ufrag (SPEC §5.2). Client and server compute it identically;
// it is only ever a MESSAGE-INTEGRITY key and is never transmitted. This
// replaces the libp2p `ufrag == pwd` convention — removing the recomputable
// fingerprint and gating DTLS behind certhash possession (probe resistance).
func deriveICEPwd(certhashDigest []byte, ufrag string) string {
	mac := hmac.New(sha256.New, certhashDigest)
	mac.Write([]byte("kps-ice-pwd-v1:"))
	mac.Write([]byte(ufrag))
	return base64.StdEncoding.WithPadding(base64.NoPadding).EncodeToString(mac.Sum(nil))
}
