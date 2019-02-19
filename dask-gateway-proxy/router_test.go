package main

import (
	"github.com/stretchr/testify/assert"
	"net/url"
	"testing"
)

func TestEmpty(t *testing.T) {
	router := NewRouter()
	// No matches for empty router
	assert.False(t, router.HasMatch("/"))
	assert.False(t, router.HasMatch("/foo"))
}

func TestMatch(t *testing.T) {
	r := NewRouter()

	// Add a few routes
	aV := &url.URL{Scheme: "https", Host: "a-val.com"}
	r.Put("/foo/val/a/", aV)

	bV := &url.URL{Scheme: "https", Host: "b-val.com"}
	r.Put("/foo/val/b/", bV)

	cV := &url.URL{Scheme: "https", Host: "c-val.com"}
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

	assert.False(t, r.HasMatch("/foo/d"))
	u, p := r.Match("/foo/d")
	assert.Nil(t, u)
	assert.Equal(t, "", p)
}

func TestMatch2(t *testing.T) {
	r := NewRouter()

	// Add nested routes
	aV := &url.URL{Scheme: "https", Host: "a-val.com"}
	r.Put("/foo/a/", aV)

	bV := &url.URL{Scheme: "https", Host: "b-val.com"}
	r.Put("/foo/a/b/", bV)

	cV := &url.URL{Scheme: "https", Host: "c-val.com"}
	r.Put("/foo/a/b/c/", cV)

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
}
