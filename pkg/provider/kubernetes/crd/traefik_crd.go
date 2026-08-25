package crd

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"k8s.io/client-go/tools/cache"
)

// Provider holds configurations of the provider.
type Provider struct {
	Namespaces       []string          `description:"Kubernetes namespaces." json:"namespaces,omitempty" toml:"namespaces,omitempty" yaml:"namespaces,omitempty" export:"true"`
	LabelSelector    string            `description:"Kubernetes label selector to use with that provider." json:"labelSelector,omitempty" toml:"labelSelector,omitempty" yaml:"labelSelector,omitempty" export:"true"`
	ThrottleDuration time.Duration     `description:"Kubernetes refresh throttle duration" json:"throttleDuration,omitempty" toml:"throttleDuration,omitempty" yaml:"throttleDuration,omitempty" export:"true"`

	informersSynced  atomic.Bool
	stopCh           chan struct{}
	mu               sync.RWMutex
	informers        map[string]cache.SharedIndexInformer
}

// NewProvider creates a new CRD provider.
func NewProvider() *Provider {
	return &Provider{
		stopCh:    make(chan struct{}),
		informers: make(map[string]cache.SharedIndexInformer),
	}
}

// IsCacheSynced returns whether all registered informers have synced their caches.
func (p *Provider) IsCacheSynced() bool {
	if p.informersSynced.Load() {
		return true
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.informers) == 0 {
		return false
	}

	for _, informer := range p.informers {
		if !informer.HasSynced() {
			return false
		}
	}

	p.informersSynced.Store(true)
	return true
}

// SetInformersSynced explicitly updates the cache synchronization status.
func (p *Provider) SetInformersSynced(synced bool) {
	p.informersSynced.Store(synced)
}

// ResolveMiddleware handles safe middleware lookup, downgrading log levels to DEBUG during transient startup states.
func (p *Provider) ResolveMiddleware(ctx context.Context, middlewareKey string, exists bool) error {
	if exists {
		return nil
	}

	if !p.IsCacheSynced() {
		log.Ctx(ctx).Debug().
			Str("middleware", middlewareKey).
			Msg("Middleware not found in cache during initial sync, will retry on next sync")
		return fmt.Errorf("middleware %q not found in cache during sync", middlewareKey)
	}

	log.Ctx(ctx).Warn().
		Str("middleware", middlewareKey).
		Msg("Middleware does not exist in provider cache")
	return fmt.Errorf("middleware %q does not exist", middlewareKey)
}

// Init the provider.
func (p *Provider) Init() error {
	return nil
}
