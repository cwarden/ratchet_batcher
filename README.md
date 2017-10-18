Batcher Ratchet Processor
=========================

A [ratchet](https://github.com/dailyburn/ratchet) processor to batch up input
before sending as output.

Usage
=====

```
import (
	batch "github.com/cwarden/ratchet_batcher"
)

pipeline := ratchet.NewPipeline(..., batch.NewBatcher(6), ...)
```
