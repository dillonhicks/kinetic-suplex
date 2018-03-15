package constants

import "time"

const (
	CacheManagerStateInitializing = iota
	CacheManagerStateRunning

	ConnectionStateNew
	ConnectionStateNominal
	ConnectionStateClosed

	StreamBufferCacheStateInitializing
	StreamBufferCacheStateRunning
	StreamBufferCacheStateStopping
	StreamBufferCacheStateTerminated
)




const (
	BufferCacheSize                         = 1024 // in entries / log lines
	BufferCacheMaxConnectionCount           = 100
	BufferCachesMaxInstances                = 128
	BufferCacheStalenessCheckInterval       = 1 * time.Second
	BufferCacheInactivityGracePeriod        = 3 * time.Second
	BufferCacheMaxEntriesPerSend            = 512
	BufferCacheMaxWaitBetweenSends          = 1 * time.Second

	WebSocketWaitTimeBetweenPolls 			= 16 * time.Millisecond
)
