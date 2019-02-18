package main

import (
	"log"
	"net/url"
	"strings"
)

func getSegment(path string, start int) (segment string, next int) {
	if len(path) == 0 || start < 0 || start > len(path)-1 {
		return "", -1
	}
	end := strings.IndexRune(path[start+1:], '/')
	if end == -1 {
		return path[start:], -1
	}
	return path[start : start+end+1], start + end + 1
}

type Router struct {
	url      *url.URL
	children map[string]*Router
}

func (r *Router) isLeaf() bool {
	return len(r.children) == 0
}

func NewRouter() *Router {
	return &Router{
		children: make(map[string]*Router),
	}
}

func (router *Router) Get(key string) (*url.URL, int) {
	node := router
	out := node.url
	n := 0
	log.Printf("Routing on %s", key)
	for part, i := getSegment(key, 0); ; part, i = getSegment(key, i) {
		log.Printf("part -> %s, i -> %d", part, i)
		node = node.children[part]
		if node == nil {
			log.Printf("Breaking out")
			break
		}
		if node.url != nil {
			log.Printf("Updating url: %s", node.url)
			out = node.url
		}
		if i == -1 {
			log.Printf("Breaking out")
			break
		}
		n = i
	}
	log.Printf("Returning %s, %d", out, n)
	return out, n
}

func (router *Router) Put(key string, url *url.URL) {
	node := router
	for part, i := getSegment(key, 0); ; part, i = getSegment(key, i) {
		child, _ := node.children[part]
		if child == nil {
			child = NewRouter()
			node.children[part] = child
		}
		node = child
		if i == -1 {
			break
		}
	}
	node.url = url
}

func (router *Router) Delete(key string) bool {
	type record struct {
		node *Router
		part string
	}

	var path []record
	node := router
	for part, i := getSegment(key, 0); ; part, i = getSegment(key, i) {
		path = append(path, record{part: part, node: node})
		node = node.children[part]
		if node == nil {
			return false
		}
		if i == -1 {
			break
		}
	}
	node.url = nil
	if node.isLeaf() {
		for i := len(path) - 1; i >= 0; i-- {
			parent := path[i].node
			part := path[i].part
			delete(parent.children, part)
			if parent.url != nil || !parent.isLeaf() {
				break
			}
		}
	}
	return true
}
