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

func TestParseFrame_Strict(t *testing.T) {
	// RESET round-trips its code.
	f, err := parseFrame(encodeCode(frameReset, CodeInternalError))
	if err != nil || f.typ != frameReset || f.code != CodeInternalError {
		t.Fatalf("parseFrame(RESET) = %+v, %v", f, err)
	}
	// Wire-rule violations (SPEC §6.2, wire version 1) are errors, not tolerated.
	for name, bad := range map[string][]byte{
		"empty message":         {},
		"empty DATA":            {0x00},
		"FIN with payload":      {0x01, 1},
		"short RESET":           {0x02, 0, 0},
		"long STOP_SENDING":     {0x03, 0, 0, 0, 0, 0},
		"short MAX_STREAM_DATA": {0x04, 1, 2, 3},
		"unknown type":          {0x05, 1},
	} {
		if _, err := parseFrame(bad); err == nil {
			t.Fatalf("parseFrame(%s) should fail", name)
		}
	}
	// MAX_STREAM_DATA round-trips within the MAX_OFFSET ceiling.
	f, err = parseFrame(encodeMaxStreamData(maxOffset))
	if err != nil || f.credit != maxOffset {
		t.Fatalf("parseFrame(MAX_STREAM_DATA) = %+v, %v", f, err)
	}
}

func TestControlCodec(t *testing.T) {
	l := flowLimits{maxStreamData: 1 << 20, maxData: 8 << 20, maxStreams: 100}
	m, err := decodeControl(encodeHello(l))
	if err != nil || m.typ != ctrlHello || m.version != wireVersion || m.limits != l {
		t.Fatalf("decodeControl(HELLO) = %+v, %v", m, err)
	}
	if len(encodeHello(l)) != 26 {
		t.Fatalf("HELLO must be 26 bytes, got %d", len(encodeHello(l)))
	}
	m, err = decodeControl(encodeConnClose(CodeReset))
	if err != nil || m.typ != ctrlConnClose || m.code != CodeReset {
		t.Fatalf("decodeControl(CLOSE) = %+v, %v", m, err)
	}
	m, err = decodeControl(encodeMaxData(42))
	if err != nil || m.typ != ctrlMaxData || m.value != 42 {
		t.Fatalf("decodeControl(MAX_DATA) = %+v, %v", m, err)
	}
	for name, bad := range map[string][]byte{
		"empty":        {},
		"short close":  {0x00, 0, 0, 0},
		"short hello":  {0x01, 1, 0},
		"unknown type": {0x07, 0},
	} {
		if _, err := decodeControl(bad); err == nil {
			t.Fatalf("decodeControl(%s) should fail", name)
		}
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
