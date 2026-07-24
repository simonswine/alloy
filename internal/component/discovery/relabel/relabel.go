package relabel

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/grafana/alloy/internal/component"
	alloy_relabel "github.com/grafana/alloy/internal/component/common/relabel"
	"github.com/grafana/alloy/internal/component/discovery"
	"github.com/grafana/alloy/internal/featuregate"
	"github.com/grafana/alloy/internal/service/livedebugging"
)

func init() {
	component.Register(component.Registration{
		Name:      "discovery.relabel",
		Stability: featuregate.StabilityGenerallyAvailable,
		Args:      Arguments{},
		Exports:   Exports{},

		Build: func(opts component.Options, args component.Arguments) (component.Component, error) {
			return New(opts, args.(Arguments))
		},
	})
}

// Arguments holds values which are used to configure the discovery.relabel component.
type Arguments struct {
	// Targets contains the input 'targets' passed by a service discovery component.
	Targets []discovery.Target `alloy:"targets,attr"`

	// The relabelling rules to apply to each target's label set.
	RelabelConfigs []*alloy_relabel.Config `alloy:"rule,block,optional"`

	// MaxCacheSize is the maximum number of relabeled targets retained in the
	// bounded LRU cache. Set to zero to disable caching.
	MaxCacheSize int `alloy:"max_cache_size,attr,optional"`
}

var DefaultArguments = Arguments{MaxCacheSize: 100_000}

func (a *Arguments) SetToDefault() {
	*a = DefaultArguments
}

func (a Arguments) Validate() error {
	if a.MaxCacheSize < 0 {
		return fmt.Errorf("max_cache_size must be >= 0; got %d", a.MaxCacheSize)
	}
	return nil
}

// Exports holds values which are exported by the discovery.relabel component.
type Exports struct {
	Output []discovery.Target  `alloy:"output,attr"`
	Rules  alloy_relabel.Rules `alloy:"rules,attr"`
}

// Component implements the discovery.relabel component.
type Component struct {
	opts component.Options

	mut sync.RWMutex

	relabelConfigs []*alloy_relabel.Config
	cacheSize      int
	cache          *lru.Cache[uint64, cacheEntry]

	debugDataPublisher livedebugging.DebugDataPublisher
}

type cacheEntry struct {
	input  discovery.Target
	output discovery.Target
	keep   bool
}

var _ component.Component = (*Component)(nil)
var _ component.LiveDebugging = (*Component)(nil)

// New creates a new discovery.relabel component.
func New(o component.Options, args Arguments) (*Component, error) {
	debugDataPublisher, err := o.GetServiceData(livedebugging.ServiceName)
	if err != nil {
		return nil, err
	}
	c := &Component{
		opts:               o,
		debugDataPublisher: debugDataPublisher.(livedebugging.DebugDataPublisher),
	}
	if args.MaxCacheSize > 0 {
		c.cache, err = lru.New[uint64, cacheEntry](args.MaxCacheSize)
		if err != nil {
			return nil, err
		}
		c.cacheSize = args.MaxCacheSize
	}

	// Call to Update() to set the output once at the start
	if err := c.Update(args); err != nil {
		return nil, err
	}

	return c, nil
}

// Run implements component.Component.
func (c *Component) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Update implements component.Component.
func (c *Component) Update(args component.Arguments) error {
	c.mut.Lock()
	defer c.mut.Unlock()

	newArgs := args.(Arguments)
	if newArgs.MaxCacheSize < 0 {
		return fmt.Errorf("max_cache_size must be >= 0; got %d", newArgs.MaxCacheSize)
	}
	if !reflect.DeepEqual(c.relabelConfigs, newArgs.RelabelConfigs) {
		if c.cache != nil {
			c.cache.Purge()
		}
		c.relabelConfigs = newArgs.RelabelConfigs
	}
	if newArgs.MaxCacheSize != c.cacheSize {
		if newArgs.MaxCacheSize == 0 {
			c.cache = nil
		} else if c.cache == nil {
			var err error
			c.cache, err = lru.New[uint64, cacheEntry](newArgs.MaxCacheSize)
			if err != nil {
				return err
			}
		} else {
			c.cache.Resize(newArgs.MaxCacheSize)
		}
		c.cacheSize = newArgs.MaxCacheSize
	}

	targets := make([]discovery.Target, 0, len(newArgs.Targets))

	for _, t := range newArgs.Targets {
		result, found := getCached(c.cache, t)
		if len(newArgs.RelabelConfigs) == 0 {
			// No rules: the target passes through untouched, no cache needed.
			result = cacheEntry{input: t, output: t, keep: true}
		} else if !found {
			builder := discovery.NewTargetBuilderFrom(t)
			result.input = t
			result.keep = alloy_relabel.ProcessBuilder(builder, newArgs.RelabelConfigs...)
			if result.keep {
				result.output = builder.Target()
			}
			addCached(c.cache, t, result)
		}

		relabelled := result.output
		if result.keep {
			targets = append(targets, relabelled)
		}
		componentID := livedebugging.ComponentID(c.opts.ID)
		c.debugDataPublisher.PublishIfActive(livedebugging.NewData(
			componentID,
			livedebugging.Target,
			1,
			func() string { return fmt.Sprintf("%s => %s", t, relabelled) },
		))
	}

	c.opts.OnStateChange(Exports{
		Output: targets,
		Rules:  newArgs.RelabelConfigs,
	})

	return nil
}

func getCached(cache *lru.Cache[uint64, cacheEntry], target discovery.Target) (cacheEntry, bool) {
	if cache == nil {
		return cacheEntry{}, false
	}
	entry, ok := cache.Get(target.RelabelFingerprint())
	if !ok || !entry.input.EqualRelabelTarget(target) {
		return cacheEntry{}, false
	}
	return entry, true
}

func addCached(cache *lru.Cache[uint64, cacheEntry], target discovery.Target, entry cacheEntry) {
	if cache != nil {
		cache.Add(target.RelabelFingerprint(), entry)
	}
}

func (c *Component) LiveDebugging() {}
