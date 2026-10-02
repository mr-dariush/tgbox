// Package router implements a high-performance parametric Radix Tree (Trie)
// designed for Telegram command routing and dynamic parameter extraction.
package router

import (
	"slices"
	"strings"
)

type routePart struct {
	isParam bool
	value   string // contains static text or parameter name
}

// parseRoute tokenizes a route path containing placeholders like {user_id}.
func parseRoute(path string) []routePart {
	var parts []routePart
	for {
		start := strings.IndexByte(path, '{')
		if start == -1 {
			if path != "" {
				parts = append(parts, routePart{isParam: false, value: path})
			}
			break
		}
		end := strings.IndexByte(path[start:], '}')
		if end == -1 {
			parts = append(parts, routePart{isParam: false, value: path})
			break
		}
		end += start // Convert to absolute index

		if start > 0 {
			parts = append(parts, routePart{isParam: false, value: path[:start]})
		}
		parts = append(parts, routePart{isParam: true, value: path[start+1 : end]})
		path = path[end+1:]
	}
	return parts
}

// Node represents a single node within the parametric Radix Tree.
type Node[T any] struct {
	value      string
	isParam    bool
	paramName  string
	children   []*Node[T]
	handler    T
	hasHandler bool
}

// NewNode instantiates a root Radix Tree node.
func NewNode[T any]() *Node[T] {
	return &Node[T]{}
}

// Clone returns a deep copy of the Radix Tree node and all its descendants.
func (n *Node[T]) Clone() *Node[T] {
	if n == nil {
		return nil
	}
	cp := &Node[T]{
		value:      n.value,
		isParam:    n.isParam,
		paramName:  n.paramName,
		handler:    n.handler,
		hasHandler: n.hasHandler,
	}
	if len(n.children) > 0 {
		cp.children = make([]*Node[T], len(n.children))
		for i, child := range n.children {
			cp.children[i] = child.Clone()
		}
	}
	return cp
}

// Insert compiles the path pattern and builds/merges nodes in the tree.
func (n *Node[T]) Insert(path string, handler T) {
	parts := parseRoute(path)
	curr := n
	for _, part := range parts {
		var found *Node[T]
		for _, child := range curr.children {
			if child.isParam == part.isParam && (child.isParam || child.value == part.value) {
				found = child
				break
			}
		}
		if found == nil {
			newNode := &Node[T]{
				isParam: part.isParam,
			}
			if part.isParam {
				newNode.paramName = part.value
			} else {
				newNode.value = part.value
			}
			curr.children = append(curr.children, newNode)
			// Sort children using generic slices.SortFunc: static nodes before params, longer before shorter.
			slices.SortFunc(curr.children, func(a, b *Node[T]) int {
				if a.isParam != b.isParam {
					if !a.isParam {
						return -1
					}
					return 1
				}
				if !a.isParam {
					if len(a.value) > len(b.value) {
						return -1
					}
					if len(a.value) < len(b.value) {
						return 1
					}
				}
				return 0
			})
			found = newNode
		}
		curr = found
	}
	curr.handler = handler
	curr.hasHandler = true
}

// Match evaluates a path, returning the registered handler and extracted parameters.
func (n *Node[T]) Match(path string) (T, map[string]string, bool) {
	params := make(map[string]string)
	handler, ok := n.match(path, params)
	if !ok {
		return handler, nil, false
	}
	return handler, params, true
}

// match recursively traverses tree branches with full backtracking on param mismatches.

func (n *Node[T]) match(path string, params map[string]string) (T, bool) {
	var zero T
	if path == "" {
		if n.hasHandler {
			return n.handler, true
		}
		return zero, false
	}

	// 1. Try to find a precise deeper match among children first (Parametric or Explicit Static)
	for _, child := range n.children {
		if !child.isParam {
			// Static match path
			if strings.HasPrefix(path, child.value) {
				remaining := path[len(child.value):]
				if h, ok := child.match(remaining, params); ok {
					return h, true
				}
			}
		} else {
			// Parameter match
			if len(child.children) == 0 {
				// Last node: consumes the entire remaining path
				params[child.paramName] = path
				if child.hasHandler {
					return child.handler, true
				}
				// Backtrack if invalid branch
				delete(params, child.paramName)
			} else {
				// Intermediate param node: must look ahead and split at child static prefixes
				for _, grandChild := range child.children {
					if grandChild.isParam {
						continue
					}
					idx := 0
					for {
						pos := strings.Index(path[idx:], grandChild.value)
						if pos == -1 {
							break
						}
						pos += idx // absolute position
						paramVal := path[:pos]
						remaining := path[pos+len(grandChild.value):]

						// Speculatively descend down the tree
						params[child.paramName] = paramVal
						if h, ok := grandChild.match(remaining, params); ok {
							return h, true
						}
						// Backtrack and search for next delimiter if match fails
						delete(params, child.paramName)
						idx = pos + 1
					}
				}
			}
		}
	}

	// 2. Fallback: If no deeper child matched, but the current node has a handler
	// and the remaining path starts with whitespace (signifying raw space-separated arguments).
	if n.hasHandler && (path[0] == ' ' || path[0] == '\t' || path[0] == '\n') {
		args := strings.TrimSpace(path)
		if args != "" {
			params["args"] = args
		}
		return n.handler, true
	}

	return zero, false
}
