package batcher

import (
	"fmt"
	"github.com/dailyburn/ratchet/data"
	"github.com/dailyburn/ratchet/util"
)

// Batch up input before sending on to output
type Batcher struct {
	BatchSize int
	rows      []map[string]interface{}
}

// NewBatcher instantiates a new instance of Batcher
func NewBatcher(batchSize int) *Batcher {
	return &Batcher{BatchSize: batchSize}
}

// ProcessData blindly sends whatever it receives to the outputChan
func (r *Batcher) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	objects, err := data.ObjectsFromJSON(d)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Error parsing into batched rows: %s", err), killChan)
	}
	r.rows = append(r.rows, objects...)
	if r.BatchSize == 0 || len(r.rows) == r.BatchSize {
		r.sendQueuedRows(outputChan, killChan)
		r.rows = nil
	}
}

// Finish - see interface for documentation.
func (r *Batcher) Finish(outputChan chan data.JSON, killChan chan error) {
	if len(r.rows) > 0 {
		r.sendQueuedRows(outputChan, killChan)
	}
}

func (r *Batcher) sendQueuedRows(outputChan chan data.JSON, killChan chan error) {
	output, err := data.NewJSON(r.rows)
	if err != nil {
		util.KillPipelineIfErr(fmt.Errorf("Error marshaling batched rows: %s", err), killChan)
	}
	outputChan <- output
}

func (r *Batcher) String() string {
	return "Batcher"
}
