package transport

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/channel/isr"
	isrnode "github.com/WuKongIM/WuKongIM/pkg/channel/node"
)

func TestFetchRequestCodecRoundTrip(t *testing.T) {
	req := isrnode.FetchRequestEnvelope{
		GroupKey:    isr.GroupKey("g1"),
		Epoch:       3,
		Generation:  7,
		ReplicaID:   2,
		FetchOffset: 11,
		OffsetEpoch: 5,
		MaxBytes:    4096,
	}

	data, err := encodeFetchRequest(req)
	if err != nil {
		t.Fatalf("encodeFetchRequest() error = %v", err)
	}
	got, err := decodeFetchRequest(data)
	if err != nil {
		t.Fatalf("decodeFetchRequest() error = %v", err)
	}
	if got != req {
		t.Fatalf("request = %+v, want %+v", got, req)
	}
}

func TestFetchResponseCodecRoundTrip(t *testing.T) {
	truncateTo := uint64(9)
	resp := isrnode.FetchResponseEnvelope{
		GroupKey:   isr.GroupKey("g1"),
		Epoch:      3,
		Generation: 7,
		TruncateTo: &truncateTo,
		LeaderHW:   12,
		Records: []isr.Record{
			{Payload: []byte("a"), SizeBytes: 1},
			{Payload: []byte("bc"), SizeBytes: 2},
		},
	}

	data, err := encodeFetchResponse(resp)
	if err != nil {
		t.Fatalf("encodeFetchResponse() error = %v", err)
	}
	got, err := decodeFetchResponse(data)
	if err != nil {
		t.Fatalf("decodeFetchResponse() error = %v", err)
	}
	if got.GroupKey != resp.GroupKey || got.Epoch != resp.Epoch || got.Generation != resp.Generation || got.LeaderHW != resp.LeaderHW {
		t.Fatalf("response metadata = %+v, want %+v", got, resp)
	}
	if got.TruncateTo == nil || *got.TruncateTo != truncateTo {
		t.Fatalf("TruncateTo = %+v, want %d", got.TruncateTo, truncateTo)
	}
	if len(got.Records) != len(resp.Records) || string(got.Records[1].Payload) != "bc" {
		t.Fatalf("records = %+v, want %+v", got.Records, resp.Records)
	}
}

func TestFetchBatchRequestCodecRoundTrip(t *testing.T) {
	req := isrnode.FetchBatchRequestEnvelope{
		Items: []isrnode.FetchBatchRequestItem{
			{
				RequestID: 11,
				Request: isrnode.FetchRequestEnvelope{
					GroupKey:    isr.GroupKey("g1"),
					Epoch:       3,
					Generation:  7,
					ReplicaID:   2,
					FetchOffset: 11,
					OffsetEpoch: 5,
					MaxBytes:    4096,
				},
			},
			{
				RequestID: 12,
				Request: isrnode.FetchRequestEnvelope{
					GroupKey:    isr.GroupKey("g2"),
					Epoch:       4,
					Generation:  8,
					ReplicaID:   3,
					FetchOffset: 21,
					OffsetEpoch: 6,
					MaxBytes:    2048,
				},
			},
		},
	}

	data, err := encodeFetchBatchRequest(req)
	if err != nil {
		t.Fatalf("encodeFetchBatchRequest() error = %v", err)
	}
	got, err := decodeFetchBatchRequest(data)
	if err != nil {
		t.Fatalf("decodeFetchBatchRequest() error = %v", err)
	}
	if len(got.Items) != len(req.Items) {
		t.Fatalf("items len = %d, want %d", len(got.Items), len(req.Items))
	}
	for i := range req.Items {
		if got.Items[i].RequestID != req.Items[i].RequestID || got.Items[i].Request != req.Items[i].Request {
			t.Fatalf("item %d = %+v, want %+v", i, got.Items[i], req.Items[i])
		}
	}
}

func TestFetchBatchResponseCodecRoundTrip(t *testing.T) {
	truncateTo := uint64(9)
	resp := isrnode.FetchBatchResponseEnvelope{
		Items: []isrnode.FetchBatchResponseItem{
			{
				RequestID: 11,
				Response: &isrnode.FetchResponseEnvelope{
					GroupKey:   isr.GroupKey("g1"),
					Epoch:      3,
					Generation: 7,
					TruncateTo: &truncateTo,
					LeaderHW:   12,
					Records: []isr.Record{
						{Payload: []byte("a"), SizeBytes: 1},
						{Payload: []byte("bc"), SizeBytes: 2},
					},
				},
			},
			{
				RequestID: 12,
				Error:     "fetch failed",
			},
		},
	}

	data, err := encodeFetchBatchResponse(resp)
	if err != nil {
		t.Fatalf("encodeFetchBatchResponse() error = %v", err)
	}
	got, err := decodeFetchBatchResponse(data)
	if err != nil {
		t.Fatalf("decodeFetchBatchResponse() error = %v", err)
	}
	if len(got.Items) != len(resp.Items) {
		t.Fatalf("items len = %d, want %d", len(got.Items), len(resp.Items))
	}
	if got.Items[0].RequestID != 11 || got.Items[0].Response == nil {
		t.Fatalf("first item = %+v", got.Items[0])
	}
	if got.Items[0].Response.GroupKey != isr.GroupKey("g1") || got.Items[0].Response.LeaderHW != 12 {
		t.Fatalf("first response = %+v", got.Items[0].Response)
	}
	if got.Items[0].Response.TruncateTo == nil || *got.Items[0].Response.TruncateTo != truncateTo {
		t.Fatalf("first truncate = %+v, want %d", got.Items[0].Response.TruncateTo, truncateTo)
	}
	if len(got.Items[0].Response.Records) != 2 || string(got.Items[0].Response.Records[1].Payload) != "bc" {
		t.Fatalf("first records = %+v", got.Items[0].Response.Records)
	}
	if got.Items[1].RequestID != 12 || got.Items[1].Error != "fetch failed" || got.Items[1].Response != nil {
		t.Fatalf("second item = %+v", got.Items[1])
	}
}

func TestProgressAckCodecRoundTrip(t *testing.T) {
	ack := isrnode.ProgressAckEnvelope{
		GroupKey:    isr.GroupKey("g1"),
		Epoch:       3,
		Generation:  7,
		ReplicaID:   2,
		MatchOffset: 11,
	}

	data, err := encodeProgressAck(ack)
	if err != nil {
		t.Fatalf("encodeProgressAck() error = %v", err)
	}
	got, err := decodeProgressAck(data)
	if err != nil {
		t.Fatalf("decodeProgressAck() error = %v", err)
	}
	if got != ack {
		t.Fatalf("ack = %+v, want %+v", got, ack)
	}
}
