package conversation

import "time"

const (
	defaultActiveScanLimit       = 2000
	defaultChannelProbeBatchSize = 512
	defaultColdThreshold         = 30 * 24 * time.Hour
	defaultFlushInterval         = 200 * time.Millisecond
	defaultFlushDirtyLimit       = 1024
	defaultSubscriberPageSize    = 512
)

type Options struct {
	States                ConversationStateStore
	ChannelUpdate         ChannelUpdateStore
	Facts                 MessageFactsStore
	Now                   func() time.Time
	ColdThreshold         time.Duration
	ActiveScanLimit       int
	ChannelProbeBatchSize int
	Async                 func(func())
}

type App struct {
	states                ConversationStateStore
	channelUpdate         ChannelUpdateStore
	facts                 MessageFactsStore
	now                   func() time.Time
	coldThreshold         time.Duration
	activeScanLimit       int
	channelProbeBatchSize int
	async                 func(func())
}

func New(opts Options) *App {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.ColdThreshold <= 0 {
		opts.ColdThreshold = defaultColdThreshold
	}
	if opts.ActiveScanLimit <= 0 {
		opts.ActiveScanLimit = defaultActiveScanLimit
	}
	if opts.ChannelProbeBatchSize <= 0 {
		opts.ChannelProbeBatchSize = defaultChannelProbeBatchSize
	}
	if opts.Async == nil {
		opts.Async = func(fn func()) { go fn() }
	}

	return &App{
		states:                opts.States,
		channelUpdate:         opts.ChannelUpdate,
		facts:                 opts.Facts,
		now:                   opts.Now,
		coldThreshold:         opts.ColdThreshold,
		activeScanLimit:       opts.ActiveScanLimit,
		channelProbeBatchSize: opts.ChannelProbeBatchSize,
		async:                 opts.Async,
	}
}
