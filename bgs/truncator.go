package bgs

import (
	"context"
	"time"
)

func globalTruncatorThread(ctx context.Context, bgs *BGS, interval time.Duration) {
	done := ctx.Done()
	ticker := time.NewTicker(interval)
	log.Infow("starting truncate ticker", "period", interval)
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			err := bgs.repoman.GlobalTruncateShards(ctx, 0)
			if err != nil {
				log.Errorw("shard truncate", "err", err)
			}
		}
	}
}
