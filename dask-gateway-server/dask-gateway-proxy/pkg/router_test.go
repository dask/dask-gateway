package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestEmpty(t *testing.T) {
	router := NewRouter()
	// No matches for empty router
	assert.False(t, router.HasMatch(""))
	assert.False(t, router.HasMatch("/"))
	assert.False(t, router.HasMatch("/foo"))
}

func TestMatch(t *testing.T) {
	r := NewRouter()

	// Add a few routes
	aV := &url.URL{Scheme: "http", Host: "a-val.com"}
	r.Put("/foo/val/a/", aV)

	bV := &url.URL{Scheme: "http", Host: "b-val.com"}
	r.Put("/foo/val/b/", bV)

	cV := &url.URL{Scheme: "http", Host: "c-val.com"}
	r.Put("/foo/c/", cV)

	testMatch := func(path string, urlSol *url.URL, pathSol string) {
		assert.True(t, r.HasMatch(path))
		u, p := r.Match(path)
		assert.Equal(t, urlSol, u)
		assert.Equal(t, pathSol, p)
	}

	testMatch("/foo/val/a", aV, "")
	testMatch("/foo/val/a/", aV, "/")
	testMatch("/foo/val/a/foo/bar", aV, "foo/bar")

	testMatch("/foo/val/b", bV, "")

	testMatch("/foo/c", cV, "")

	assert.False(t, r.HasMatch(""))
	assert.False(t, r.HasMatch("/"))

	assert.False(t, r.HasMatch("/foo/d"))
	u, p := r.Match("/foo/d")
	assert.Nil(t, u)
	assert.Equal(t, "", p)
}

func TestMatch2(t *testing.T) {
	r := NewRouter()

	// Add nested routes
	rootV := &url.URL{Scheme: "http", Host: "root-val.com"}
	r.Put("/", rootV)

	aV := &url.URL{Scheme: "http", Host: "a-val.com"}
	r.Put("/foo/a/", aV)

	bV := &url.URL{Scheme: "http", Host: "b-val.com"}
	r.Put("/foo/a/b/", bV)

	cV := &url.URL{Scheme: "http", Host: "c-val.com"}
	r.Put("/foo/a/b/c/", cV)

	dV := &url.URL{Scheme: "http", Host: "d-val.com"}
	r.Put("/foo/other/stuff/d/", dV)

	testMatch := func(path string, urlSol *url.URL, pathSol string) {
		assert.True(t, r.HasMatch(path))
		u, p := r.Match(path)
		assert.Equal(t, urlSol, u)
		assert.Equal(t, pathSol, p)
	}

	testMatch("/foo/a", aV, "")
	testMatch("/foo/a/", aV, "/")
	testMatch("/foo/a/foo/bar", aV, "foo/bar")

	testMatch("/foo/a/b", bV, "")

	testMatch("/foo/a/b/c", cV, "")
	testMatch("/foo/a/b/c/d", cV, "d")

	testMatch("", rootV, "")
	testMatch("/", rootV, "/")
	testMatch("/bar", rootV, "bar")
	testMatch("/bar/baz", rootV, "bar/baz")
	testMatch("/foo/other", rootV, "foo/other")
	testMatch("/foo/other/stuff", rootV, "foo/other/stuff")

	testMatch("/foo/other/stuff/d", dV, "")
	testMatch("/foo/other/stuff/d/here", dV, "here")
}

func TestRemove(t *testing.T) {
	r := NewRouter()
	paths := []string{"/", "/1", "/2", "/a/b/c/d", "/a/b/", "/b", "/b/c", "/b/c/d"}
	pathmap := make(map[string]*url.URL)
	for i, path := range paths {
		val := &url.URL{Scheme: "http", Host: fmt.Sprintf("http://v%d.com", i)}
		r.Put(path, val)
		pathmap[path] = val
	}

	// Original path matches
	target, rest := r.Match("/b/foo")
	assert.Equal(t, pathmap["/b"], target)
	assert.Equal(t, "foo", rest)

	// After deletion, matches on the root node
	r.Delete("/b")
	target, rest = r.Match("/b/foo")
	assert.Equal(t, pathmap["/"], target)
	assert.Equal(t, "b/foo", rest)

	// Specific sub-matches still work though
	target, rest = r.Match("/b/c/foo")
	assert.Equal(t, pathmap["/b/c"], target)
	assert.Equal(t, "foo", rest)

	// Smoketest double delete
	r.Delete("/b")

	// Original path matches
	target, rest = r.Match("/a/b/c/d/foo")
	assert.Equal(t, pathmap["/a/b/c/d"], target)
	assert.Equal(t, "foo", rest)
	r.Delete("/a/b/c/d")
	// Deleted leaf node pares the tree
	assert.Nil(t, r.branches["a"].branches["b"].branches)

	// Delete root node doesn't break things
	r.Delete("/")
	target, rest = r.Match("/b/c/foo")
	assert.Equal(t, pathmap["/b/c"], target)
	assert.Equal(t, "foo", rest)
	// But it does remove root node matches
	target, rest = r.Match("/b/foo")
	assert.Nil(t, target)
	assert.Equal(t, "", rest)
}

func BenchmarkRouterGet(b *testing.B) {
	r := NewRouter()
	paths := []string{"/", "/a", "/a/b", "/a/b/c", "/a/b/c/d", "/b", "/b/c", "/b/c/d"}
	for i, path := range paths {
		r.Put(path, &url.URL{Scheme: "http", Host: fmt.Sprintf("http://v%d.com", i)})
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = r.Match(paths[i%len(paths)])
	}
}
