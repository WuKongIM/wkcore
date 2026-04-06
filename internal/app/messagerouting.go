package app

import (
	"context"

	accessnode "github.com/WuKongIM/WuKongIM/internal/access/node"
	"github.com/WuKongIM/WuKongIM/internal/usecase/message"
	"github.com/WuKongIM/WuKongIM/internal/usecase/presence"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/raftcluster"
)

type messageRecipientDirectory struct {
	authority presence.Authoritative
}

func (d messageRecipientDirectory) EndpointsByUID(ctx context.Context, uid string) ([]message.Endpoint, error) {
	if d.authority == nil {
		return nil, nil
	}
	routes, err := d.authority.EndpointsByUID(ctx, uid)
	if err != nil {
		return nil, err
	}
	endpoints := make([]message.Endpoint, 0, len(routes))
	for _, route := range routes {
		endpoints = append(endpoints, message.Endpoint{
			NodeID:     route.NodeID,
			BootID:     route.BootID,
			SessionID:  route.SessionID,
			DeviceFlag: route.DeviceFlag,
		})
	}
	return endpoints, nil
}

type messageRemoteDelivery struct {
	cluster *raftcluster.Cluster
	client  *accessnode.Client
}

func (d messageRemoteDelivery) DeliverRemote(ctx context.Context, cmd message.RemoteDeliveryCommand) error {
	if d.cluster == nil || d.client == nil {
		return nil
	}
	return d.client.Deliver(ctx, accessnode.DeliveryCommand{
		NodeID:     cmd.NodeID,
		GroupID:    uint64(d.cluster.SlotForKey(cmd.UID)),
		UID:        cmd.UID,
		BootID:     cmd.BootID,
		SessionIDs: append([]uint64(nil), cmd.SessionIDs...),
		Frame:      cmd.Frame,
	})
}
