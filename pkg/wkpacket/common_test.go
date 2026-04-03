package wkpacket

import "testing"

func TestLatestVersionAndFrameTypes(t *testing.T) {
	if LatestVersion != 6 {
		t.Fatalf("LatestVersion = %d, want 6", LatestVersion)
	}

	if got := (&PingPacket{}).GetFrameType(); got != PING {
		t.Fatalf("PingPacket.GetFrameType() = %v, want %v", got, PING)
	}

	if got := (&PongPacket{}).GetFrameType(); got != PONG {
		t.Fatalf("PongPacket.GetFrameType() = %v, want %v", got, PONG)
	}
}

func TestSettingFlags(t *testing.T) {
	var s Setting
	s.Set(SettingReceiptEnabled)
	s.Set(SettingTopic)

	if !s.IsSet(SettingReceiptEnabled) {
		t.Fatal("expected SettingReceiptEnabled to be set")
	}
	if !s.IsSet(SettingTopic) {
		t.Fatal("expected SettingTopic to be set")
	}

	s.Clear(SettingReceiptEnabled)
	if s.IsSet(SettingReceiptEnabled) {
		t.Fatal("expected SettingReceiptEnabled to be cleared")
	}
	if !s.IsSet(SettingTopic) {
		t.Fatal("expected SettingTopic to remain set")
	}
}
