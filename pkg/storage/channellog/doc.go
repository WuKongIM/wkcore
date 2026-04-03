// Package channellog adapts ISR replication groups to channel-oriented
// message send, fetch, and runtime metadata semantics. The only business
// identity translation in this package is the local ChannelKey -> isr.GroupKey
// derivation used to address the business-agnostic ISR runtime.
package channellog
