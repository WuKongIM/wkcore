package presence

import (
	"context"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/runtime/online"
	"github.com/WuKongIM/WuKongIM/pkg/protocol/wkframe"
)

const (
	routeActionKindClose         = "close"
	routeActionKindKickThenClose = "kick_then_close"
	routeActionKickReason        = "login in other device"
	defaultRouteCloseDelay       = 2 * time.Second
)

type localActionDispatcher struct {
	app *App
}

func (d localActionDispatcher) ApplyRouteAction(ctx context.Context, action RouteAction) error {
	if d.app == nil {
		return nil
	}
	if action.NodeID != 0 && d.app.localNodeID != 0 && action.NodeID != d.app.localNodeID {
		return ErrRemoteActionDispatchRequired
	}
	return d.app.ApplyRouteAction(ctx, action)
}

func (a *App) Activate(ctx context.Context, cmd ActivateCommand) error {
	if a.router == nil {
		return ErrRouterRequired
	}
	if cmd.Session == nil {
		return ErrSessionRequired
	}

	conn := a.localConnFromActivate(cmd)
	if err := a.online.Register(conn); err != nil {
		return err
	}

	groupID := a.router.SlotForKey(conn.UID)
	result, err := a.authority.RegisterAuthoritative(ctx, RegisterAuthoritativeCommand{
		GroupID: groupID,
		Route:   a.routeFromConn(conn),
	})
	if err != nil {
		a.online.Unregister(conn.SessionID)
		return err
	}
	if err := a.dispatchActions(ctx, result.Actions); err != nil {
		a.bestEffortUnregister(ctx, groupID, conn)
		a.online.Unregister(conn.SessionID)
		return err
	}
	return nil
}

func (a *App) Deactivate(ctx context.Context, cmd DeactivateCommand) error {
	conn, ok := a.online.Connection(cmd.SessionID)
	if !ok {
		return nil
	}
	a.online.Unregister(cmd.SessionID)

	if a.router == nil {
		return nil
	}
	uid := conn.UID
	if uid == "" {
		uid = cmd.UID
	}
	if uid == "" {
		return nil
	}
	a.bestEffortUnregister(ctx, a.router.SlotForKey(uid), conn)
	return nil
}

func (a *App) ApplyRouteAction(ctx context.Context, action RouteAction) error {
	_ = ctx
	if action.NodeID != 0 && a.localNodeID != 0 && action.NodeID != a.localNodeID {
		return nil
	}
	if action.BootID != a.gatewayBootID {
		return nil
	}

	conn, ok := a.online.Connection(action.SessionID)
	if !ok {
		return nil
	}
	if conn.State == online.LocalRouteStateClosing {
		return nil
	}
	if conn.UID != action.UID {
		return fmt.Errorf("presence: fenced route mismatch for session %d", action.SessionID)
	}

	conn, ok = a.online.MarkClosing(action.SessionID)
	if !ok || conn.State != online.LocalRouteStateClosing {
		return fmt.Errorf("presence: failed to move session %d to closing", action.SessionID)
	}

	a.afterFunc(0, func() {
		a.finishRouteAction(conn, action)
	})
	return nil
}

func (a *App) dispatchActions(ctx context.Context, actions []RouteAction) error {
	for _, action := range actions {
		if err := a.actions.ApplyRouteAction(ctx, action); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) bestEffortUnregister(ctx context.Context, groupID uint64, conn online.OnlineConn) {
	_ = a.authority.UnregisterAuthoritative(ctx, UnregisterAuthoritativeCommand{
		GroupID: groupID,
		Route:   a.routeFromConn(conn),
	})
}

func (a *App) localConnFromActivate(cmd ActivateCommand) online.OnlineConn {
	listener := cmd.Listener
	if listener == "" && cmd.Session != nil {
		listener = cmd.Session.Listener()
	}
	connectedAt := cmd.ConnectedAt
	if connectedAt.IsZero() {
		connectedAt = a.now()
	}
	groupID := uint64(0)
	if a.router != nil {
		groupID = a.router.SlotForKey(cmd.UID)
	}
	return online.OnlineConn{
		SessionID:   cmd.Session.ID(),
		UID:         cmd.UID,
		DeviceID:    cmd.DeviceID,
		DeviceFlag:  cmd.DeviceFlag,
		DeviceLevel: cmd.DeviceLevel,
		GroupID:     groupID,
		State:       online.LocalRouteStateActive,
		Listener:    listener,
		ConnectedAt: connectedAt,
		Session:     cmd.Session,
	}
}

func (a *App) routeFromConn(conn online.OnlineConn) Route {
	return Route{
		UID:         conn.UID,
		NodeID:      a.localNodeID,
		BootID:      a.gatewayBootID,
		SessionID:   conn.SessionID,
		DeviceID:    conn.DeviceID,
		DeviceFlag:  uint8(conn.DeviceFlag),
		DeviceLevel: uint8(conn.DeviceLevel),
		Listener:    conn.Listener,
	}
}

func (a *App) finishRouteAction(conn online.OnlineConn, action RouteAction) {
	if action.Kind == routeActionKindKickThenClose && conn.Session != nil {
		_ = conn.Session.WriteFrame(&wkframe.DisconnectPacket{
			ReasonCode: wkframe.ReasonConnectKick,
			Reason:     routeActionKickReason,
		})
	}
	delay := a.closeDelay
	if action.DelayMS > 0 {
		delay = time.Duration(action.DelayMS) * time.Millisecond
	}
	if conn.Session == nil {
		return
	}
	a.afterFunc(delay, func() {
		_ = conn.Session.Close()
	})
}
