package topology

import (
	"testing"
)

var topo = &Topology{}

func TestTopo(t *testing.T) {
	topo.getNodeTree()
}

func TestCalculate(t *testing.T) {
	topo.calculate()
	topo.getNodeTree()
}

func init() {
	topo = NewTopology()
	node0 := &Node{Uuid: "0000000000", ParentUUID: "IAMADMINXD", ChildrenUUID: []string{"1111111111", "2222222222", "3333333333"}}
	node1 := &Node{Uuid: "1111111111", ParentUUID: "0000000000", ChildrenUUID: []string{"5555555555", "6666666666"}}
	node2 := &Node{Uuid: "2222222222", ParentUUID: "0000000000", ChildrenUUID: []string{"4444444444", "7777777777"}}
	node3 := &Node{Uuid: "3333333333", ParentUUID: "0000000000", ChildrenUUID: []string{}}
	node4 := &Node{Uuid: "4444444444", ParentUUID: "2222222222", ChildrenUUID: []string{}}
	node5 := &Node{Uuid: "5555555555", ParentUUID: "1111111111", ChildrenUUID: []string{}}
	node6 := &Node{Uuid: "6666666666", ParentUUID: "1111111111", ChildrenUUID: []string{}}
	node7 := &Node{Uuid: "7777777777", ParentUUID: "2222222222", ChildrenUUID: []string{"8888888888"}}
	node8 := &Node{Uuid: "8888888888", ParentUUID: "7777777777", ChildrenUUID: []string{}}
	topo.nodes[0] = node0
	topo.nodes[1] = node1
	topo.nodes[2] = node2
	topo.nodes[3] = node3
	topo.nodes[4] = node4
	topo.nodes[5] = node5
	topo.nodes[6] = node6
	topo.nodes[7] = node7
	topo.nodes[8] = node8
}
