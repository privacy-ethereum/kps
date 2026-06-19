package kps

import (
	"bytes"
	"testing"
)

// These pin the §6.2 wire format so the Go and JS implementations stay
// byte-compatible. The JS side (libs/js/src/framing.ts) must produce the same
// bytes.

func TestEncodeData(t *testing.T) {
	got := encodeData([]byte("hi"))
	want := []byte{0x00, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Fatalf("encodeData = %v, want %v", got, want)
	}
	// Empty payload is a bare DATA tag.
	if got := encodeData(nil); !bytes.Equal(got, []byte{0x00}) {
		t.Fatalf("encodeData(nil) = %v, want [0]", got)
	}
}

func TestEncodeFin(t *testing.T) {
	if got := encodeFin(); !bytes.Equal(got, []byte{0x01}) {
		t.Fatalf("encodeFin = %v, want [1]", got)
	}
}

func TestEncodeCode_BigEndian(t *testing.T) {
	// RESET with code 3 (= "reset"): type 0x02, then uint32 BE 0x00000003.
	got := encodeCode(frameReset, CodeReset)
	want := []byte{0x02, 0x00, 0x00, 0x00, 0x03}
	if !bytes.Equal(got, want) {
		t.Fatalf("encodeCode(RESET, 3) = %v, want %v", got, want)
	}
	// STOP_SENDING with code 1 (= "cancelled").
	got = encodeCode(frameStopSending, CodeCancelled)
	want = []byte{0x03, 0x00, 0x00, 0x00, 0x01}
	if !bytes.Equal(got, want) {
		t.Fatalf("encodeCode(STOP_SENDING, 1) = %v, want %v", got, want)
	}
}

func TestDecodeCode(t *testing.T) {
	if c := decodeCode([]byte{0x00, 0x00, 0x00, 0x0b}); c != CodeInternalError {
		t.Fatalf("decodeCode = %d, want %d", c, CodeInternalError)
	}
	// Short/absent payload defaults to CodeNone.
	if c := decodeCode([]byte{0x00, 0x01}); c != CodeNone {
		t.Fatalf("decodeCode(short) = %d, want 0", c)
	}
}

func TestDecodeCerthash_RoundTrip(t *testing.T) {
	id, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := decodeCerthash(id.Certhash)
	if err != nil {
		t.Fatalf("decodeCerthash: %v", err)
	}
	if !bytes.Equal(digest, id.digest) {
		t.Fatalf("decoded digest != identity digest")
	}
	if len(digest) != 32 {
		t.Fatalf("digest len = %d, want 32", len(digest))
	}
}

func TestDeriveICEPwd_Deterministic(t *testing.T) {
	digest := make([]byte, 32)
	for i := range digest {
		digest[i] = byte(i)
	}
	a := deriveICEPwd(digest, "abc123")
	b := deriveICEPwd(digest, "abc123")
	if a != b {
		t.Fatal("deriveICEPwd not deterministic")
	}
	if a == deriveICEPwd(digest, "different") {
		t.Fatal("deriveICEPwd should depend on ufrag")
	}
	// Within the ICE ice-char set (base64 standard, no padding).
	if len(a) == 0 || a[len(a)-1] == '=' {
		t.Fatalf("unexpected pwd encoding: %q", a)
	}
}
