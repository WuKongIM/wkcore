package wkproto

import "github.com/WuKongIM/WuKongIM/pkg/proto/wkpacket"

const (
	MaxRemaingLength uint32 = 1024 * 1024
	PayloadMaxSize          = 1<<15 - 1
)

func ToFixHeaderUint8(f wkpacket.Frame) uint8 {
	typeAndFlags := encodeBool(f.GetDUP())<<3 | encodeBool(f.GetsyncOnce())<<2 | encodeBool(f.GetRedDot())<<1 | encodeBool(f.GetNoPersist())
	if f.GetFrameType() == wkpacket.CONNACK {
		typeAndFlags = encodeBool(f.GetHasServerVersion())
	}
	return byte(int(f.GetFrameType()<<4) | typeAndFlags)
}

func FramerFromUint8(v uint8) wkpacket.Framer {
	p := wkpacket.Framer{}
	p.NoPersist = (v & 0x01) > 0
	p.RedDot = (v >> 1 & 0x01) > 0
	p.SyncOnce = (v >> 2 & 0x01) > 0
	p.DUP = (v >> 3 & 0x01) > 0
	p.FrameType = wkpacket.FrameType(v >> 4)

	switch p.FrameType {
	case wkpacket.CONNACK:
		p.HasServerVersion = (v & 0x01) > 0
	}

	return p
}
