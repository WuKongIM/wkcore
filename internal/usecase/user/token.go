package user

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
	"github.com/WuKongIM/WuKongIM/pkg/storage/metadb"
)

func (a *App) UpdateToken(ctx context.Context, cmd UpdateTokenCommand) error {
	if err := cmd.Validate(); err != nil {
		return err
	}
	if a.users == nil {
		return ErrUserStoreRequired
	}
	if a.devices == nil {
		return ErrDeviceStoreRequired
	}

	_, err := a.users.GetUser(ctx, cmd.UID)
	if errors.Is(err, metadb.ErrNotFound) {
		if err := a.users.UpsertUser(ctx, metadb.User{UID: cmd.UID}); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	if err := a.devices.UpsertDevice(ctx, metadb.Device{
		UID:         cmd.UID,
		DeviceFlag:  int64(cmd.DeviceFlag),
		Token:       cmd.Token,
		DeviceLevel: int64(cmd.DeviceLevel),
	}); err != nil {
		return err
	}

	if cmd.DeviceLevel != wkframe.DeviceLevelMaster || a.online == nil {
		return nil
	}

	for _, conn := range a.online.ConnectionsByUID(cmd.UID) {
		if conn.DeviceFlag != cmd.DeviceFlag || conn.Session == nil {
			continue
		}
		sess := conn.Session
		_ = sess.WriteFrame(&wkframe.DisconnectPacket{
			ReasonCode: wkframe.ReasonConnectKick,
			Reason:     "账号在其他设备上登录",
		})
		a.afterFunc(10*time.Second, func() { _ = sess.Close() })
	}
	return nil
}
