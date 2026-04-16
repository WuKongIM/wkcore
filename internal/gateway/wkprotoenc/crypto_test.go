package wkprotoenc_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/gateway/wkprotoenc"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/frame"
)

func TestGatewayEncryptionNegotiatesSameSessionKeysForClientAndServer(t *testing.T) {
	clientPriv, clientPub, err := wkprotoenc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() error = %v", err)
	}

	serverKeys, serverPublicKey, err := wkprotoenc.NegotiateServerSession(wkprotoenc.EncodePublicKey(clientPub))
	if err != nil {
		t.Fatalf("NegotiateServerSession() error = %v", err)
	}

	clientKeys, err := wkprotoenc.DeriveClientSession(clientPriv, serverPublicKey, string(serverKeys.AESIV))
	if err != nil {
		t.Fatalf("DeriveClientSession() error = %v", err)
	}

	if got, want := string(clientKeys.AESKey), string(serverKeys.AESKey); got != want {
		t.Fatalf("AESKey = %q, want %q", got, want)
	}
	if got, want := string(clientKeys.AESIV), string(serverKeys.AESIV); got != want {
		t.Fatalf("AESIV = %q, want %q", got, want)
	}
}

func TestGatewayEncryptionPayloadRoundTrip(t *testing.T) {
	clientPriv, clientPub, err := wkprotoenc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() error = %v", err)
	}

	serverKeys, serverPublicKey, err := wkprotoenc.NegotiateServerSession(wkprotoenc.EncodePublicKey(clientPub))
	if err != nil {
		t.Fatalf("NegotiateServerSession() error = %v", err)
	}
	clientKeys, err := wkprotoenc.DeriveClientSession(clientPriv, serverPublicKey, string(serverKeys.AESIV))
	if err != nil {
		t.Fatalf("DeriveClientSession() error = %v", err)
	}

	encrypted, err := wkprotoenc.EncryptPayload([]byte("hello"), clientKeys)
	if err != nil {
		t.Fatalf("EncryptPayload() error = %v", err)
	}
	decrypted, err := wkprotoenc.DecryptPayload(encrypted, serverKeys)
	if err != nil {
		t.Fatalf("DecryptPayload() error = %v", err)
	}

	if got, want := string(decrypted), "hello"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

func TestGatewayEncryptionValidatesSendMsgKey(t *testing.T) {
	keys := wkprotoenc.SessionKeys{
		AESKey: []byte("1234567890abcdef"),
		AESIV:  []byte("abcdef1234567890"),
	}
	packet := &frame.SendPacket{
		ClientSeq:   7,
		ClientMsgNo: "m1",
		ChannelID:   "u2",
		ChannelType: frame.ChannelTypePerson,
		Payload:     []byte("ciphertext"),
	}

	msgKey, err := wkprotoenc.SendMsgKey(packet, keys)
	if err != nil {
		t.Fatalf("SendMsgKey() error = %v", err)
	}
	packet.MsgKey = msgKey

	if err := wkprotoenc.ValidateSendPacket(packet, keys); err != nil {
		t.Fatalf("ValidateSendPacket() error = %v", err)
	}

	packet.MsgKey = "bad-key"
	if err := wkprotoenc.ValidateSendPacket(packet, keys); err != wkprotoenc.ErrMsgKeyMismatch {
		t.Fatalf("ValidateSendPacket() error = %v, want %v", err, wkprotoenc.ErrMsgKeyMismatch)
	}
}

func TestGatewayEncryptionSealsRecvPacketWithEncryptedPayloadAndMsgKey(t *testing.T) {
	clientPriv, clientPub, err := wkprotoenc.GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair() error = %v", err)
	}

	serverKeys, serverPublicKey, err := wkprotoenc.NegotiateServerSession(wkprotoenc.EncodePublicKey(clientPub))
	if err != nil {
		t.Fatalf("NegotiateServerSession() error = %v", err)
	}
	clientKeys, err := wkprotoenc.DeriveClientSession(clientPriv, serverPublicKey, string(serverKeys.AESIV))
	if err != nil {
		t.Fatalf("DeriveClientSession() error = %v", err)
	}

	packet := &frame.RecvPacket{
		MessageID:   99,
		MessageSeq:  8,
		ClientMsgNo: "m1",
		Timestamp:   123,
		FromUID:     "u1",
		ChannelID:   "u2",
		ChannelType: frame.ChannelTypePerson,
		Payload:     []byte("hello"),
	}

	sealed, err := wkprotoenc.SealRecvPacket(packet, serverKeys)
	if err != nil {
		t.Fatalf("SealRecvPacket() error = %v", err)
	}
	if got := string(packet.Payload); got != "hello" {
		t.Fatalf("original payload mutated to %q", got)
	}
	if got := string(sealed.Payload); got == "hello" {
		t.Fatalf("sealed payload should be encrypted, got %q", got)
	}
	if sealed.MsgKey == "" {
		t.Fatal("sealed MsgKey is empty")
	}

	decrypted, err := wkprotoenc.DecryptPayload(sealed.Payload, clientKeys)
	if err != nil {
		t.Fatalf("DecryptPayload() error = %v", err)
	}
	if got, want := string(decrypted), "hello"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}
