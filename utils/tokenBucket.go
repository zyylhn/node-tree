package utils

import (
	"fmt"
	"time"
)

type TokenBucket struct {
	capacity int
	tokens   chan struct{}
	timeout  time.Duration
}

func NewTokenBucket(capacity int, timeout time.Duration) *TokenBucket {
	tb := &TokenBucket{
		capacity: capacity,
		tokens:   make(chan struct{}, capacity),
	}
	for i := 0; i < capacity; i++ {
		tb.tokens <- struct{}{} // 初始化令牌
	}
	tb.timeout = timeout
	return tb
}

func (tb *TokenBucket) Take() error {
	select {
	case <-tb.tokens:
		return nil
	case <-time.After(tb.timeout):
		return fmt.Errorf("get token timeout(%vs)", tb.timeout.Seconds())
	}
}

func (tb *TokenBucket) Release() {
	select {
	case tb.tokens <- struct{}{}:
	default:
		// 如果桶已满，什么都不做
	}
}

func (tb *TokenBucket) Close() {
	close(tb.tokens)
}
