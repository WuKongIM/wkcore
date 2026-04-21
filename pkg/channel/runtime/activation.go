package runtime

import (
	"context"

	core "github.com/WuKongIM/WuKongIM/pkg/channel"
)

type ActivationSource string

const (
	ActivationSourceBusiness ActivationSource = "business"
	ActivationSourceFetch    ActivationSource = "fetch"
	ActivationSourceProbe    ActivationSource = "probe"
	ActivationSourceLaneOpen ActivationSource = "lane_open"
)

type Activator interface {
	ActivateByKey(ctx context.Context, key core.ChannelKey, source ActivationSource) (core.Meta, error)
}
