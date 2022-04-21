package main

import (
	"encoding/json"
	"net/url"
	"strings"
)

func normalizePath(path string) (string, int) {
	offset := 0
	if path == "" || path == "/" {
		return "", offset
	}
	if path[0] == '/' {
		path = path[1:]
		offset = 1
	}
	if path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	return path, offset
}

func getSegment(path string, start int) (segment string, next int) {
	if len(path) == 0 {
		return path, -1
	}
	end := strings.IndexRune(path[start:], '/')
	if end == -1 {
		return path[start:], -1
	}
	return path[start : start+end], start + end + 1
}

type Router struct {
	url      *url.URL
	branches map[string]*Router
}

func (r *Router) isLeaf() bool {
	return len(r.branches) == 0
}

func NewRouter() *Router {
	return &Router{}
}

func (router *Router) HasMatch(path string) bool {
	if router.url != nil {
		return true
	}
	path, _ = normalizePath(path)
	for part, i := getSegment(path, 0); ; part, i = getSegment(path, i) {
		router = router.branches[part]
		if router == nil {
			break
		}
		if router.url != nil {
			return true
		}
		if i == -1 {
			break
		}
	}
	return false
}

func (router *Router) Match(path string) (*url.URL, string) {
	path2, offset := normalizePath(path)
	node := router
	out := node.url
	n := 0
	offset2 := 0
	for {
		part, i := getSegment(path2, n)
		node = node.branches[part]
		if node == nil {
			break
		}
		if node.url != nil {
			out = node.url
			if i == -1 {
				offset2 = len(path2)
			} else {
				offset2 = i
			}
		}
		if i == -1 {
			break
		}
		n = i
	}
	if out == nil {
		return nil, ""
	}
	return out, path[offset+offset2:]
}

func (router *Router) Put(path string, url *url.URL) {
	path, _ = normalizePath(path)
	if path == "" {
		router.url = url
		return
	}
	node := router
	for part, i := getSegment(path, 0); ; part, i = getSegment(path, i) {
		child, _ := node.branches[part]
		if child == nil {
			child = NewRouter()
			if node.branches == nil {
				node.branches = make(map[string]*Router)
			}
			node.branches[part] = child
		}
		node = child
		if i == -1 {
			break
		}
	}
	node.url = url
}

func (router *Router) Delete(path string) {
	path, _ = normalizePath(path)

	if path == "" {
		// Handle root node
		router.url = nil
		return
	}

	type record struct {
		node *Router
		part string
	}

	var paths []record
	node := router
	for part, i := getSegment(path, 0); ; part, i = getSegment(path, i) {
		paths = append(paths, record{part: part, node: node})
		node = node.branches[part]
		if node == nil {
			return
		}
		if i == -1 {
			break
		}
	}
	node.url = nil
	if node.isLeaf() {
		for i := len(paths) - 1; i >= 0; i-- {
			parent := paths[i].node
			part := paths[i].part
			delete(parent.branches, part)
			// If completely empty, deallocate whole map
			if len(parent.branches) == 0 {
				parent.branches = nil
			}
			if parent.url != nil || !parent.isLeaf() {
				break
			}
		}
	}
}

func (r *Router) traverse(prefix string, f func(prefix string, value *url.URL)) {
	if r.url != nil {
		f(prefix, r.url)
	}
	prefix = prefix + "/"
	for path, node := range r.branches {
		node.traverse(prefix+path, f)
	}
}

func (r *Router) MarshalJSON() ([]byte, error) {
	out := make(map[string]string)
	r.traverse("", func(prefix string, value *url.URL) {
		out[prefix] = value.String()
	})
	b, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return b, nil
}
