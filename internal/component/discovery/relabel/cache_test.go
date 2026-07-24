package relabel

import (
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/grafana/alloy/internal/component/discovery"
)

func TestTargetCacheVerifiesCollisions(t *testing.T) {
	cache, err := lru.New[uint64, cacheEntry](1)
	require.NoError(t, err)

	first := discovery.NewTargetFromMap(map[string]string{"instance": "one"})
	second := discovery.NewTargetFromMap(map[string]string{"instance": "two"})
	entry := cacheEntry{input: first, output: first, keep: true}

	// Use the same cache index to exercise the equality check independently of
	// the fingerprint implementation.
	cache.Add(1, entry)
	got, found := cache.Get(1)
	require.True(t, found)
	require.True(t, got.input.EqualRelabelTarget(first))
	require.False(t, got.input.EqualRelabelTarget(second))

	cache.Add(1, cacheEntry{input: second, keep: false})
	got, found = cache.Get(1)
	require.True(t, found)
	require.False(t, got.keep)
}

func TestTargetCacheIsBounded(t *testing.T) {
	cache, err := lru.New[uint64, cacheEntry](1)
	require.NoError(t, err)

	first := discovery.NewTargetFromMap(map[string]string{"instance": "one"})
	second := discovery.NewTargetFromMap(map[string]string{"instance": "two"})
	addCached(cache, first, cacheEntry{input: first, output: first, keep: true})
	addCached(cache, second, cacheEntry{input: second, keep: false})

	require.Len(t, cache.Keys(), 1)
	require.False(t, cache.Contains(first.RelabelFingerprint()))
	require.True(t, cache.Contains(second.RelabelFingerprint()))
}
