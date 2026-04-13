package handler

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
)

func (s *service) Fetch(_ context.Context, req channel.FetchRequest) (channel.FetchResult, error) {
	if req.Limit <= 0 {
		return channel.FetchResult{}, channel.ErrInvalidFetchArgument
	}
	if req.MaxBytes <= 0 {
		return channel.FetchResult{}, channel.ErrInvalidFetchBudget
	}

	key := KeyFromChannelID(req.ChannelID)
	meta, err := s.metaForKey(key)
	if err != nil {
		return channel.FetchResult{}, err
	}
	if err := compatibleWithExpectation(meta, req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch); err != nil {
		return channel.FetchResult{}, err
	}
	switch meta.Status {
	case channel.StatusDeleting:
		return channel.FetchResult{}, channel.ErrChannelDeleting
	case channel.StatusDeleted:
		return channel.FetchResult{}, channel.ErrChannelNotFound
	}

	group, ok := s.cfg.Runtime.Channel(key)
	if !ok {
		return channel.FetchResult{}, channel.ErrStaleMeta
	}
	state := group.Status()
	committedSeq := state.HW
	startSeq := req.FromSeq
	if startSeq == 0 {
		startSeq = state.LogStartOffset + 1
		if startSeq == 0 {
			startSeq = 1
		}
	}
	if startSeq > committedSeq {
		return channel.FetchResult{NextSeq: startSeq, CommittedSeq: committedSeq}, nil
	}

	records, err := s.cfg.Store.Read(key, startSeq-1, req.Limit, req.MaxBytes)
	if err != nil {
		return channel.FetchResult{}, err
	}
	result := channel.FetchResult{
		Messages:     make([]channel.Message, 0, minInt(len(records), req.Limit)),
		NextSeq:      startSeq,
		CommittedSeq: committedSeq,
	}
	for _, record := range records {
		if record.Offset >= state.HW {
			break
		}
		msg, err := decodeMessageRecord(record)
		if err != nil {
			return channel.FetchResult{}, err
		}
		result.Messages = append(result.Messages, msg)
		result.NextSeq = record.Offset + 2
	}
	return result, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
