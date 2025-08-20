//go:build windows
// +build windows

package winfsp

import (
	"sync"

	"github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	path "github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"golang.org/x/sys/windows"
)

// openedNodesPool holds the state of nodes that are opened by WinFSP.
type openedNodesPool struct {
	nodeLock   sync.Mutex
	handles    map[uintptr]*openedNode
	nextHandle uintptr
}

type openedNode struct {
	name      path.Component
	parent    virtual.Directory
	node      virtual.DirectoryChild
	shareMask virtual.ShareMask
}

func newOpenedNodesPool() openedNodesPool {
	return openedNodesPool{
		handles:    make(map[uintptr]*openedNode),
		nextHandle: 1,
	}
}

func (ofp *openedNodesPool) nodeForHandle(handle uintptr) (virtual.DirectoryChild, error) {
	ofp.nodeLock.Lock()
	defer ofp.nodeLock.Unlock()
	node, ok := ofp.handles[handle]
	if !ok {
		return virtual.DirectoryChild{}, windows.STATUS_INVALID_HANDLE
	}
	return node.node, nil
}

func (ofp *openedNodesPool) nodeForDirectoryHandle(handle uintptr) (virtual.Directory, error) {
	child, err := ofp.nodeForHandle(handle)
	if err != nil {
		return nil, err
	}
	dir, _ := child.GetPair()
	if dir == nil {
		return nil, windows.STATUS_NOT_A_DIRECTORY
	}
	return dir, nil
}

func (ofp *openedNodesPool) nodeForLeafHandle(handle uintptr) (virtual.Leaf, error) {
	child, err := ofp.nodeForHandle(handle)
	if err != nil {
		return nil, err
	}
	_, leaf := child.GetPair()
	if leaf == nil {
		return nil, windows.STATUS_FILE_IS_A_DIRECTORY
	}
	return leaf, nil
}

func (ofp *openedNodesPool) trackedNodeForHandle(handle uintptr) (*openedNode, error) {
	ofp.nodeLock.Lock()
	defer ofp.nodeLock.Unlock()
	node, ok := ofp.handles[handle]
	if !ok {
		return nil, windows.STATUS_INVALID_HANDLE
	}
	return node, nil
}

func (ofp *openedNodesPool) createHandle(parent virtual.Directory, name path.Component, node virtual.DirectoryChild, shareMask virtual.ShareMask) uintptr {
	ofp.nodeLock.Lock()
	defer ofp.nodeLock.Unlock()
	handle := ofp.nextHandle
	ofp.nextHandle++
	ofp.handles[handle] = &openedNode{
		parent:    parent,
		name:      name,
		node:      node,
		shareMask: shareMask,
	}
	return handle
}

func (ofp *openedNodesPool) removeHandle(handle uintptr) (*openedNode, error) {
	ofp.nodeLock.Lock()
	defer ofp.nodeLock.Unlock()
	node, ok := ofp.handles[handle]
	if !ok {
		return nil, windows.STATUS_INVALID_HANDLE
	}
	delete(ofp.handles, handle)
	return node, nil
}
