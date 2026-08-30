package protocol

import (
	"bytes"
	"testing"

	"github.com/fxamacker/cbor/v2"
)

// TestUnknownFieldsIgnoredOnDecode proves the SPEC.md §11 forward-compat
// obligation: a message encoded with extra fields a v1 decoder doesn't
// know about must decode cleanly, ignoring them, rather than erroring or
// panicking. This simulates a v2 peer sending a Hello (and separately a
// FileInfo, since that one's future `blocks` field is called out
// explicitly in §11) with additional keys this build's structs don't
// declare.
func TestUnknownFieldsIgnoredOnDecode(t *testing.T) {
	type helloV2 struct {
		ProtoVersion int    `cbor:"proto_version"`
		NodeName     string `cbor:"node_name"`
		Ed25519Pub   []byte `cbor:"ed25519_pub"`
		Token        string `cbor:"token"`
		Nonce        []byte `cbor:"nonce"`
		// Fields a hypothetical v2 might add, unknown to this build.
		Capabilities []string `cbor:"capabilities"`
		ExtraFlag    bool     `cbor:"extra_flag"`
	}

	future := helloV2{
		ProtoVersion: 2,
		NodeName:     "alice",
		Ed25519Pub:   bytes.Repeat([]byte{1}, 32),
		Token:        "sc1AAAA",
		Nonce:        bytes.Repeat([]byte{2}, 32),
		Capabilities: []string{"block-transfer"},
		ExtraFlag:    true,
	}

	data, err := cbor.Marshal(future)
	if err != nil {
		t.Fatalf("cbor.Marshal(helloV2): %v", err)
	}

	var got Hello
	if err := DecodeMessage(data, &got); err != nil {
		t.Fatalf("DecodeMessage with unknown fields present: %v", err)
	}
	if got.NodeName != future.NodeName {
		t.Errorf("NodeName = %q, want %q", got.NodeName, future.NodeName)
	}
	if !bytes.Equal(got.Ed25519Pub, future.Ed25519Pub) {
		t.Errorf("Ed25519Pub mismatch")
	}
	if !bytes.Equal(got.Nonce, future.Nonce) {
		t.Errorf("Nonce mismatch")
	}
	if got.Token != future.Token {
		t.Errorf("Token = %q, want %q", got.Token, future.Token)
	}

	// Also round-trip through the real frame codec, not just raw
	// cbor.Unmarshal, so the guarantee holds through the path production
	// code actually uses.
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteMessage(MsgHello, future); err != nil {
		t.Fatalf("WriteMessage(helloV2): %v", err)
	}
	typ, payload, err := NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if typ != MsgHello {
		t.Fatalf("type = %s, want %s", typ, MsgHello)
	}
	var got2 Hello
	if err := DecodeMessage(payload, &got2); err != nil {
		t.Fatalf("DecodeMessage via frame codec with unknown fields: %v", err)
	}
	if got2.NodeName != future.NodeName {
		t.Errorf("via frame codec: NodeName = %q, want %q", got2.NodeName, future.NodeName)
	}
}

// TestFileInfoForwardCompatBlocksField specifically exercises SPEC.md
// §11's called-out future extension: FileInfo gaining an optional
// `blocks` field in v2 must not break a v1 decoder.
func TestFileInfoForwardCompatBlocksField(t *testing.T) {
	type block struct {
		Offset int64  `cbor:"offset"`
		Size   int64  `cbor:"size"`
		SHA256 []byte `cbor:"sha256_16"`
	}
	type fileInfoV2 struct {
		RelPath string        `cbor:"relpath"`
		Type    FileType      `cbor:"type"`
		Size    int64         `cbor:"size"`
		MTimeNS int64         `cbor:"mtime_ns"`
		Mode    uint32        `cbor:"mode"`
		SHA256  []byte        `cbor:"sha256"`
		Version VersionVector `cbor:"version"`
		Deleted bool          `cbor:"deleted"`
		Blocks  []block       `cbor:"blocks"`
	}

	future := fileInfoV2{
		RelPath: "a/b.txt",
		Type:    FileTypeFile,
		Size:    100,
		MTimeNS: 42,
		Mode:    0644,
		SHA256:  bytes.Repeat([]byte{9}, 32),
		Version: VersionVector{"aaaa1111": 1},
		Blocks:  []block{{Offset: 0, Size: 64 * 1024, SHA256: bytes.Repeat([]byte{7}, 16)}},
	}

	data, err := cbor.Marshal(future)
	if err != nil {
		t.Fatalf("cbor.Marshal(fileInfoV2): %v", err)
	}

	var got FileInfo
	if err := DecodeMessage(data, &got); err != nil {
		t.Fatalf("DecodeMessage FileInfo with blocks field: %v", err)
	}
	if got.RelPath != future.RelPath || got.Size != future.Size || !bytes.Equal(got.SHA256, future.SHA256) {
		t.Errorf("FileInfo round-trip mismatch: got %+v", got)
	}
}

func TestMsgTypeStringUnknown(t *testing.T) {
	if s := MsgType(200).String(); s == "" {
		t.Error("MsgType.String() for an unknown type returned empty string")
	}
}
