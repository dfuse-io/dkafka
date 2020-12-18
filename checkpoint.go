package dkafka

import (
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
)

type publisherCheckpoint struct {
	lastBlockSent               bstream.BlockRef
	libNum                      uint64 // TODO any fork event replayed between LIBNum and lastBlockSent upon restart has the possibility of requiring a new eventID
	maxBlocksBetweenCheckpoints int
	checkpointFrequency         time.Duration
}

type checkpointer interface {
	Save(cp *publisherCheckpoint, jobConfig *publisherJobConfig) error
	Load() (*publisherCheckpoint, *publisherJobConfig, error)
}

type localFileCheckpointer struct {
	filename string
}

func (c *localFileCheckpointer) Save(cp *publisherCheckpoint, jobConfig *publisherJobConfig) error {
	return fmt.Errorf("not implemented")
}
func (c *localFileCheckpointer) Load() (*publisherCheckpoint, *publisherJobConfig, error) {
	return nil, nil, fmt.Errorf("not implemented")
}
