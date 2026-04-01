package message

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

type SendCommand struct {
	Framer      wkpacket.Framer
	Setting     wkpacket.Setting
	MsgKey      string
	Expire      uint32
	SenderUID   string
	ClientSeq   uint64
	ClientMsgNo string
	StreamNo    string
	ChannelID   string
	ChannelType uint8
	Topic       string
	Payload     []byte
}

type RecvAckCommand struct {
	UID        string
	Framer     wkpacket.Framer
	MessageID  int64
	MessageSeq uint32
}
