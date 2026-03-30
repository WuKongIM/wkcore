package wkproto

import "github.com/WuKongIM/WuKongIM/pkg/wkpacket"

type Setting = wkpacket.Setting

const (
	SettingUnknown        Setting = wkpacket.SettingUnknown
	SettingReceiptEnabled Setting = wkpacket.SettingReceiptEnabled
	SettingSignal         Setting = wkpacket.SettingSignal
	SettingNoEncrypt      Setting = wkpacket.SettingNoEncrypt
	SettingTopic          Setting = wkpacket.SettingTopic
	SettingStream         Setting = wkpacket.SettingStream
)
