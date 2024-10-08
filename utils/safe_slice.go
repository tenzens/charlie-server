package utils

import (
	"errors"
	"sync"
)

type SafeSlice[T any] struct {
	contents []T
	Capacity int
	frozen   bool
	rwMu     sync.RWMutex

	Notification chan struct{}
	notified     bool
}

func NewSafeSlice[T any](capacity int) *SafeSlice[T] {
	return &SafeSlice[T]{
		contents:     make([]T, 0, capacity),
		Capacity:     capacity,
		frozen:       false,
		notified:     false,
		Notification: make(chan struct{}, 1),
	}
}

func (s *SafeSlice[T]) Append(element []T) error {
	s.rwMu.Lock()
	defer s.rwMu.Unlock()

	if s.frozen {
		return errors.New("slice is frozen")
	}

	if !s.notified {
		s.UnsafeNotify()
	}

	if len(s.contents) >= s.Capacity {
		return errors.New("slice is full, cannot append")
	}

	s.contents = append(s.contents, element...)

	if len(s.contents) >= s.Capacity {
		s.frozen = true
	}

	return nil
}

func (s *SafeSlice[T]) UnsafeSetNotified(val bool) {
	s.notified = val
}

func (s *SafeSlice[T]) UnsafeNotify() {
	s.Notification <- struct{}{}
	s.notified = true
}

func (s *SafeSlice[T]) UnsafePeek() []T {
	copiedContent := make([]T, len(s.contents))
	copy(copiedContent, s.contents)
	return copiedContent
}

func (s *SafeSlice[T]) UnsafeConsume() {
	s.contents = make([]T, 0, s.Capacity)
}

func (s *SafeSlice[T]) UnsafeDefreeze() {
	s.frozen = false
}

func (s *SafeSlice[T]) UnsafeIsFrozen() bool {
	return s.frozen
}

func (s *SafeSlice[T]) UnsafeFreeze() {
	s.frozen = true
}

func (s *SafeSlice[T]) UnsafeSetSlice(contents []T) {
	s.contents = contents
}

func (s *SafeSlice[T]) Lock() {
	s.rwMu.Lock()
}

func (s *SafeSlice[T]) Unlock() {
	s.rwMu.Unlock()
}
