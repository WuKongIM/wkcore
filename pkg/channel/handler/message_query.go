package handler

import (
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/channel"
	"github.com/WuKongIM/WuKongIM/pkg/channel/store"
)

const messageQueryChunkLimit = 256

// QueryMessagesRequest configures one channel-local message page scan.
type QueryMessagesRequest struct {
	// ChannelID identifies the channel to scan.
	ChannelID channel.ChannelID
	// BeforeSeq is the exclusive upper message sequence bound for the next page.
	BeforeSeq uint64
	// Limit is the maximum number of matched messages to return.
	Limit int
	// MessageID filters the result to one durable message identifier when set.
	MessageID uint64
	// ClientMsgNo filters the result to matching client message numbers when set.
	ClientMsgNo string
}

// QueryMessagesResult is the matched message page in descending sequence order.
type QueryMessagesResult struct {
	// Messages contains matched messages ordered from newest to oldest.
	Messages []channel.Message
	// HasMore reports whether another matched page exists.
	HasMore bool
	// NextBeforeSeq is the exclusive upper sequence bound for the next page.
	NextBeforeSeq uint64
}

// LoadCommittedHW returns the durable committed high watermark for one channel.
func LoadCommittedHW(engine *store.Engine, id channel.ChannelID) (uint64, error) {
	if engine == nil || id.ID == "" || id.Type == 0 {
		return 0, channel.ErrInvalidArgument
	}
	st := engine.ForChannel(KeyFromChannelID(id), id)
	checkpoint, err := st.LoadCheckpoint()
	if errors.Is(err, channel.ErrEmptyState) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return checkpoint.HW, nil
}

// QueryMessages scans one channel store page without materializing the full result set.
func QueryMessages(engine *store.Engine, committedHW uint64, req QueryMessagesRequest) (QueryMessagesResult, error) {
	if engine == nil {
		return QueryMessagesResult{}, channel.ErrInvalidArgument
	}
	if req.ChannelID.ID == "" || req.ChannelID.Type == 0 || req.Limit <= 0 {
		return QueryMessagesResult{}, channel.ErrInvalidArgument
	}
	if committedHW == 0 {
		return QueryMessagesResult{}, nil
	}

	startSeq := committedHW
	if req.BeforeSeq > 0 {
		if req.BeforeSeq <= 1 {
			return QueryMessagesResult{}, nil
		}
		startSeq = req.BeforeSeq - 1
		if startSeq > committedHW {
			startSeq = committedHW
		}
	}
	if startSeq == 0 {
		return QueryMessagesResult{}, nil
	}

	targetMatches := req.Limit + 1
	st := engine.ForChannel(KeyFromChannelID(req.ChannelID), req.ChannelID)
	result := QueryMessagesResult{
		Messages: make([]channel.Message, 0, minInt(req.Limit, messageQueryChunkLimit)),
	}

	nextSeq := startSeq
	for nextSeq > 0 && len(result.Messages) < targetMatches {
		remainingMatches := targetMatches - len(result.Messages)
		batchLimit := messageQueryChunkLimit
		if remainingMatches > batchLimit {
			remainingMatches = batchLimit
		}
		records, err := st.ReadOffsetsReverse(nextSeq-1, batchLimit, math.MaxInt)
		if err != nil {
			return QueryMessagesResult{}, err
		}
		if len(records) == 0 {
			break
		}

		oldestSeq := uint64(0)
		for _, record := range records {
			msg, err := decodeMessageRecord(record)
			if err != nil {
				return QueryMessagesResult{}, err
			}
			oldestSeq = msg.MessageSeq
			if !queryMessageMatches(msg, req) {
				continue
			}
			result.Messages = append(result.Messages, msg)
			if len(result.Messages) >= targetMatches {
				break
			}
		}
		if oldestSeq <= 1 {
			break
		}
		nextSeq = oldestSeq - 1
	}

	if len(result.Messages) <= req.Limit {
		return result, nil
	}

	result.HasMore = true
	result.NextBeforeSeq = result.Messages[req.Limit-1].MessageSeq
	result.Messages = result.Messages[:req.Limit]
	return result, nil
}

func queryMessageMatches(message channel.Message, req QueryMessagesRequest) bool {
	if req.MessageID != 0 && message.MessageID != req.MessageID {
		return false
	}
	if req.ClientMsgNo != "" && message.ClientMsgNo != req.ClientMsgNo {
		return false
	}
	return true
}
