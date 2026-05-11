package handler

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"sync/atomic"
)

type MessageHandler func(ctx context.Context, msg *nats.Msg) error

type ConsumerStats struct {
	Processed     uint64 `json:"processed"`
	Failed        uint64 `json:"failed"`
	ActiveWorkers int32  `json:"active_workers"`
	PoolSize      int    `json:"pool_size"`
}

type ConsumerPool struct {
	sub           *nats.Subscription
	handler       MessageHandler
	poolSize      int
	logger        *zap.Logger
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	processed     atomic.Uint64
	failed        atomic.Uint64
	activeWorkers atomic.Int32
}

func NewConsumerPool(
	js nats.JetStreamContext,
	subject string,
	consumer string,
	handler MessageHandler,
	poolSize int,
	logger *zap.Logger,
) (*ConsumerPool, error) {
	sub, err := js.PullSubscribe(
		subject,
		consumer,
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(5),
	)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumerPool{
		sub:      sub,
		handler:  handler,
		poolSize: poolSize,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (cp *ConsumerPool) Start() {
	cp.logger.Info("starting consumer pool", zap.Int("pool_size", cp.poolSize))

	for i := 0; i < cp.poolSize; i++ {
		cp.wg.Add(1)
		go cp.worker(i)
	}
}

func (cp *ConsumerPool) worker(id int) {
	defer cp.wg.Done()
	cp.activeWorkers.Add(1)
	defer cp.activeWorkers.Add(-1)

	for {
		select {
		case <-cp.ctx.Done():
			return
		default:
			msgs, err := cp.sub.Fetch(100, nats.MaxWait(5*time.Second))
			if err != nil {
				if err == nats.ErrTimeout {
					continue
				}
				cp.logger.Error("fetch error", zap.Error(err), zap.Int("worker", id))
				time.Sleep(1 * time.Second)
				continue
			}

			for _, msg := range msgs {
				if err := cp.handler(cp.ctx, msg); err != nil {
					cp.failed.Add(1)
					cp.logger.Error("handler error",
						zap.Error(err),
						zap.Int("worker", id),
						zap.String("subject", msg.Subject),
					)
					msg.Nak()
				} else {
					cp.processed.Add(1)
					msg.Ack()
				}
			}
		}
	}
}

func (cp *ConsumerPool) GetStats() ConsumerStats {
	return ConsumerStats{
		Processed:     cp.processed.Load(),
		Failed:        cp.failed.Load(),
		ActiveWorkers: cp.activeWorkers.Load(),
		PoolSize:      cp.poolSize,
	}
}

func (cp *ConsumerPool) Stop() {
	cp.logger.Info("stopping consumer pool...")
	cp.cancel()
	cp.wg.Wait()
	cp.sub.Unsubscribe()
	cp.logger.Info("consumer pool stopped")
}
