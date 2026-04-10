package log

import "context"

func (c *cluster) Fetch(_ context.Context, req FetchRequest) (FetchResult, error) {
	if req.Limit <= 0 {
		return FetchResult{}, ErrInvalidFetchArgument
	}
	if req.MaxBytes <= 0 {
		return FetchResult{}, ErrInvalidFetchBudget
	}
	groupKey := channelGroupKey(req.Key)

	meta, err := c.metaForKey(req.Key)
	if err != nil {
		return FetchResult{}, err
	}
	if err := compatibleWithExpectation(meta, req.ExpectedChannelEpoch, req.ExpectedLeaderEpoch); err != nil {
		return FetchResult{}, err
	}
	switch meta.Status {
	case ChannelStatusDeleting:
		return FetchResult{}, ErrChannelDeleting
	case ChannelStatusDeleted:
		return FetchResult{}, ErrChannelNotFound
	}

	group, ok := c.cfg.Runtime.Group(groupKey)
	if !ok {
		return FetchResult{}, ErrStaleMeta
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
		return FetchResult{
			NextSeq:      startSeq,
			CommittedSeq: committedSeq,
		}, nil
	}

	fromOffset := startSeq - 1
	records, err := c.cfg.Log.Read(groupKey, fromOffset, req.Limit, req.MaxBytes)
	if err != nil {
		return FetchResult{}, err
	}

	result := FetchResult{
		Messages:     make([]Message, 0, len(records)),
		NextSeq:      startSeq,
		CommittedSeq: committedSeq,
	}
	for _, record := range records {
		if record.Offset >= state.HW {
			break
		}
		message, err := decodeMessageRecord(record)
		if err != nil {
			return FetchResult{}, err
		}
		result.Messages = append(result.Messages, message)
		result.NextSeq = record.Offset + 2
	}
	return result, nil
}
