package transport

import "context"

// Client is a thin wrapper around Pool.
type Client struct {
	pool *Pool
}

func NewClient(pool *Pool) *Client {
	return &Client{pool: pool}
}

func (c *Client) Send(nodeID NodeID, shardKey uint64, msgType uint8, body []byte) error {
	return c.pool.Send(nodeID, shardKey, msgType, body)
}

func (c *Client) RPC(ctx context.Context, nodeID NodeID, shardKey uint64, payload []byte) ([]byte, error) {
	return c.pool.RPC(ctx, nodeID, shardKey, payload)
}

func (c *Client) RPCService(ctx context.Context, nodeID NodeID, shardKey uint64, serviceID uint8, payload []byte) ([]byte, error) {
	return c.RPC(ctx, nodeID, shardKey, encodeRPCServicePayload(serviceID, payload))
}

func (c *Client) Stop() {
	if c.pool != nil {
		c.pool.Close()
	}
}
