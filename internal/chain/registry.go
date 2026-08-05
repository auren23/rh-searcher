package chain

import (
	"errors"
	"fmt"
	"sync"
)

// Registry 管理多个数据源并做故障切换。
type Registry struct {
	mu      sync.RWMutex
	sources map[string]Source
	primary string
}

func NewRegistry(primary string) *Registry {
	return &Registry{sources: make(map[string]Source), primary: primary}
}

func (r *Registry) Add(name string, s Source) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sources[name]; ok {
		return fmt.Errorf("source %q already registered", name)
	}
	r.sources[name] = s
	return nil
}

func (r *Registry) Primary() (Source, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sources[r.primary]
	if !ok {
		return nil, errors.New("no primary source")
	}
	return s, nil
}

// Fallback 按注册顺序返回第一个可用的 Source。
func (r *Registry) Fallback() (Source, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sources) == 0 {
		return nil, errors.New("no sources registered")
	}
	// 优先 primary
	if s, ok := r.sources[r.primary]; ok {
		return s, nil
	}
	for _, s := range r.sources {
		return s, nil
	}
	return nil, errors.New("no sources registered")
}
