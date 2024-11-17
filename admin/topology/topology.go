package topology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/agent/process/system_info"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/utils"
	"net"
	"strings"
	"time"
)

const (
	AddNode    = iota //添加节点
	GetUuid           //获取uuid
	GetUuidNum        //获取id num
	CheckNode         //检查node存在
	Calculate
	GetRoute
	DelNode
	ReOnlineNode
	UpdateDetail
	GetNodeTree
	UpdateMemo
	SingleNodeInfo
	GetParentInfo
	GetAllNodeUuid
	UpdateCurrentIP
)

// Topology IDNum is only for user-friendly,Uuid is used internally
type Topology struct {
	currentIDNum int
	nodes        map[int]*Node     // we use UuidNum as the map's key,that's the only special exception，存着所有节点的信息
	route        map[string]string // map[Uuid]route
	history      map[string]int    //map[Uuid] currentIdNum //记录uuid和对应uuid num的记录，不会被删除
	NumUuid      map[string]int    //记录uuid和对应uuid num的记录，会被删除，起索引作用

	TaskChan chan *managerTask //接收任务，根据类型执行对应操作

	AddNodeResultChan           chan int    //add node操作返回的结果返回序号
	GetUUidResultChan           chan string //获取节点的uuid操作返回结果
	GetIdNumResultChan          chan int    //获取节点的uuidNum操作返回结果
	CheckNodeResultChan         chan bool   //检查节点是否存在操作返回结果
	CalculateResultChan         chan bool   //重新初始化路由（仅仅是通知完成操作）
	UpdateCurrentIpResultChan   chan bool
	GetRouteResultChan          chan routeInfo      //根据uuid获取节点路由
	DelNodeResultChan           chan map[int]string //删除节点操作会返回所有删除节点的map[idNum]uuid 删除一个节点的同时也会删除他所有的子节点
	ReOnLineResultChan          chan bool           //节点重新上线（仅通知完成操作）
	GetNodeTreeResultChan       chan *RootNodeTree  //获取所有节点的树形拓扑
	UpdateNodeMemoResultChan    chan bool           //更新节点备注(仅通知完成操作)
	GetSingleNodeInfoResultChan chan *Node          //获取单个节点信息
	GetParentNodeInfoResultChan chan *Node          //获取父节点信息
	GetAllNodeUUidResultChan    chan map[string]int //获取所有节点的uuid和对应的id num
	//todo channel 的关闭
}

type routeInfo struct {
	route   string
	isExist bool
}

type Node struct {
	NodeInfo              //当前节点的基本信息
	UuidNum      int      `json:"uuid_num"`      //当前节点的编号
	Uuid         string   `json:"uuid"`          //当前节点的id
	ParentUUID   string   `json:"parent_uuid"`   //当前子节点的父节点id
	ChildrenUUID []string `json:"children_uuid"` //当前节点的所有子节点
}

type NodeJoin struct {
	*Node
}

type NodeUpdate struct {
	*Node
}

type NodeReOnline struct {
	*Node
}

type NodeInfo struct {
	SystemInfo       *system_info.SystemInfo `json:"system_info"`
	CurrentIp        string                  `json:"current_ip"`
	Memo             []string                `json:"memo"`
	ConnectInfo      *protocol.ConnectMethod `json:"connect_info"`
	AgentUpLineTime  time.Time               `json:"agent_up_line_time"`  //需要agent传递过来，agent拿到uuid的时间，agent本地的时间
	AdminSetUuidTime time.Time               `json:"admin_set_uuid_time"` //admin再给agent设置uuid的admin的时间，admin的时间，他们二者中间会相差一个路由通信的时间和代码执运行时间
}

func (n *NodeInfo) GetLocalAddr() (system_info.NetWorkCards, error) {
	if n.SystemInfo != nil {
		return n.SystemInfo.NetWorkCardInfo, nil
	} else {
		return system_info.NetWorkCards{}, errors.New("node system is nil")
	}
}

func (n *NodeInfo) String() string {
	re := ""
	if n.CurrentIp != "" {
		re += fmt.Sprintf("IP:%v  ", n.CurrentIp)
	}
	if n.SystemInfo != nil {
		re += n.SystemInfo.String()
	}
	if n.ConnectInfo != nil {
		re += fmt.Sprintf("ConnectInfo:%v  ", n.ConnectInfo)
	}
	if n.Memo != nil {
		re += fmt.Sprintf("Memo:%v", n.Memo)
	}
	return re
}

type managerTask struct {
	Mode        int
	UUID        string
	UUIDNum     int
	ParentUUID  string
	Target      *Node
	Memo        []string
	SystemInfo  string
	ConnectInfo string
	IsFirst     bool
	CurrentIp   string
}

func NewTopology() *Topology {
	topology := new(Topology)
	topology.nodes = make(map[int]*Node)
	topology.route = make(map[string]string)
	topology.history = make(map[string]int)
	topology.currentIDNum = 0
	topology.TaskChan = make(chan *managerTask, 100)
	topology.NumUuid = make(map[string]int)
	topology.AddNodeResultChan = make(chan int)
	topology.GetUUidResultChan = make(chan string)
	topology.GetIdNumResultChan = make(chan int)
	topology.CheckNodeResultChan = make(chan bool)
	topology.CalculateResultChan = make(chan bool)
	topology.UpdateCurrentIpResultChan = make(chan bool)
	topology.GetRouteResultChan = make(chan routeInfo)
	topology.DelNodeResultChan = make(chan map[int]string)
	topology.ReOnLineResultChan = make(chan bool)
	topology.UpdateNodeMemoResultChan = make(chan bool)
	topology.GetNodeTreeResultChan = make(chan *RootNodeTree)
	topology.GetSingleNodeInfoResultChan = make(chan *Node)
	topology.GetParentNodeInfoResultChan = make(chan *Node)
	topology.GetAllNodeUUidResultChan = make(chan map[string]int)

	return topology
}

// NewNode 新建一个节点
func NewNode(uuid string, addr string) *Node {
	ip, _, _ := net.SplitHostPort(addr)
	n := new(Node)
	n.Uuid = uuid
	n.CurrentIp = ip
	return n
}

// Run 类似一个单例广播器，不断的从chan中读取任务，根据任务类型执行对应操作
func (topology *Topology) Run(ctx context.Context) {
	//不断从管道中接收Task，然后判断类型运行对应操作
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-topology.TaskChan:
			switch task.Mode {
			case AddNode:
				topology.addNode(task) //添加新节点,保存新节点、修改父节点
			case GetUuid:
				topology.getUUID(task)
			case GetUuidNum:
				topology.getUUIDNum(task)
			case CheckNode:
				topology.checkNode(task)
			case UpdateDetail:
				topology.updateDetail(task)
			case GetNodeTree:
				topology.getNodeTree()
			case UpdateMemo:
				topology.updateMemo(task)
			case Calculate:
				topology.calculate() //重新初始化路由节点
			case GetRoute:
				topology.getRoute(task)
			case DelNode:
				topology.delNode(task)
			case ReOnlineNode:
				topology.reOnlineNode(task)
			case SingleNodeInfo:
				topology.getSingleNodeInfo(task)
			case GetParentInfo:
				topology.getParentInfo(task)
			case GetAllNodeUuid:
				topology.getAllUuid()
			case UpdateCurrentIP:
				topology.updateCurrentIp(task)
			}
		}
	}
}

// AddNode 添加一个新节点
func (topology *Topology) AddNode(first bool, target *Node, parentUUID string) int {
	task := &managerTask{
		Mode:       AddNode,
		Target:     target,
		ParentUUID: parentUUID,
		IsFirst:    first,
	}
	topology.TaskChan <- task

	num := <-topology.AddNodeResultChan
	return num
}

func (topology *Topology) UpdateCurrentIp(uuid string, ipAddr string) error {
	ip := net.ParseIP(ipAddr)
	if ip == nil {
		return errors.New("ip format error:" + ipAddr)
	}
	task := &managerTask{
		Mode:      UpdateCurrentIP,
		UUID:      uuid,
		CurrentIp: ipAddr,
	}
	topology.TaskChan <- task
	if ok := <-topology.UpdateCurrentIpResultChan; ok {
		return nil
	} else {
		return errors.New(fmt.Sprintf("node %v not exist", uuid))
	}
}

// GetUUID 根据节点id num获取他的uuid
func (topology *Topology) GetUUID(uuidNum int) string {
	task := &managerTask{
		Mode:    GetUuid,
		UUIDNum: uuidNum,
	}
	topology.TaskChan <- task
	uuid := <-topology.GetUUidResultChan
	return uuid
}

// GetUUIDNum 根据uuid获取idNum
func (topology *Topology) GetUUIDNum(uuid string) int {
	task := &managerTask{
		Mode: GetUuidNum,
		UUID: uuid,
	}
	topology.TaskChan <- task
	idNum := <-topology.GetIdNumResultChan
	return idNum
}

func (topology *Topology) CheckNodeWithUUID(uuid string) bool {
	idNum := topology.id2IDNum(uuid)
	if idNum == -1 {
		return false
	}
	return topology.CheckNode(idNum)
}

// CheckNode 检查节点是否存在
func (topology *Topology) CheckNode(uuidNum int) bool {
	task := &managerTask{
		Mode:    CheckNode,
		UUIDNum: uuidNum,
	}
	topology.TaskChan <- task

	result := <-topology.CheckNodeResultChan
	return result
}

func (topology *Topology) UpdateDetail(uuid string, memo []string, systemInfo, connectInfo string) *Node {
	task := &managerTask{
		Mode:        UpdateDetail,
		UUID:        uuid,
		Memo:        memo,
		SystemInfo:  systemInfo,
		ConnectInfo: connectInfo,
	}
	topology.TaskChan <- task
	node := <-topology.GetSingleNodeInfoResultChan
	return node
}

func (topology *Topology) GetNodeTree() *RootNodeTree {
	task := &managerTask{
		Mode: GetNodeTree,
	}
	topology.TaskChan <- task
	re := <-topology.GetNodeTreeResultChan
	return re
}

func (topology *Topology) UpdateMemo(uuid string, memo []string) {
	task := &managerTask{
		Mode: UpdateMemo,
		UUID: uuid,
		Memo: memo,
	}
	topology.TaskChan <- task
	<-topology.UpdateNodeMemoResultChan
}

func (topology *Topology) Calculate() {
	task := &managerTask{ //刷新路由
		Mode: Calculate,
	}
	topology.TaskChan <- task
	<-topology.CalculateResultChan
}

func (topology *Topology) GetRoute(uuid string) (string, bool) {
	task := &managerTask{
		Mode: GetRoute,
		UUID: uuid,
	}
	topology.TaskChan <- task
	route := <-topology.GetRouteResultChan
	return route.route, route.isExist
}

func (topology *Topology) DelNode(uuid string) map[int]string {
	tTask := &managerTask{
		Mode: DelNode,
		UUID: uuid,
	}
	topology.TaskChan <- tTask
	result := <-topology.DelNodeResultChan
	return result
}

func (topology *Topology) ReOnLineNode(first bool, node *Node, parentUuid string) {
	task := &managerTask{
		Mode:       ReOnlineNode,
		Target:     node,
		ParentUUID: parentUuid,
		IsFirst:    first,
	}
	topology.TaskChan <- task
	<-topology.ReOnLineResultChan
}

func (topology *Topology) GetSingleNodeInfo(uuid string) (*Node, error) {
	task := &managerTask{
		Mode: SingleNodeInfo,
		UUID: uuid,
	}
	topology.TaskChan <- task
	info := <-topology.GetSingleNodeInfoResultChan
	if info == nil {
		return nil, errors.New(fmt.Sprintf("%v node does not exist", topology.GetUUIDNum(uuid)))
	}
	return info, nil
}

func (topology *Topology) GetParentInfo(uuid string) (*Node, error) {
	task := &managerTask{
		Mode: GetParentInfo,
		UUID: uuid,
	}
	topology.TaskChan <- task
	re := <-topology.GetParentNodeInfoResultChan
	if re == nil {
		return nil, errors.New(fmt.Sprintf("failed to get the parent node of node %v", topology.GetUUIDNum(uuid)))
	}
	return re, nil
}

func (topology *Topology) GetAllUuid() map[string]int {
	task := &managerTask{
		Mode: GetAllNodeUuid,
	}
	topology.TaskChan <- task
	result := <-topology.GetAllNodeUUidResultChan
	return result
}

func (topology *Topology) getAllUuid() {
	topology.GetAllNodeUUidResultChan <- topology.NumUuid
}

// 通过uuid去nodes中查询id num索引
func (topology *Topology) id2IDNum(uuid string) int {
	if idNum, ok := topology.NumUuid[uuid]; ok {
		return idNum
	}
	return -1
}

func (topology *Topology) idNum2ID(uuidNum int) string {
	return topology.nodes[uuidNum].Uuid
}

func (topology *Topology) getUUID(task *managerTask) {
	topology.GetUUidResultChan <- topology.idNum2ID(task.UUIDNum)
}

func (topology *Topology) getUUIDNum(task *managerTask) {
	topology.GetIdNumResultChan <- topology.id2IDNum(task.UUID)
}

func (topology *Topology) checkNode(task *managerTask) {
	if _, ok := topology.nodes[task.UUIDNum]; ok {
		topology.CheckNodeResultChan <- true
	} else {
		topology.CheckNodeResultChan <- false
	}
}

func (topology *Topology) updateCurrentIp(task *managerTask) {
	if _, ok := topology.nodes[topology.id2IDNum(task.UUID)]; ok {
		topology.nodes[topology.id2IDNum(task.UUID)].CurrentIp = task.CurrentIp
		topology.UpdateCurrentIpResultChan <- true
	} else {
		topology.UpdateCurrentIpResultChan <- false
	}
}

// 添加节点：设置node信息的父节点，将他的id放到父节点的子节点中，将当前节点存下来，存到history中(map[Uuid]currentIDNum)，编号加一
func (topology *Topology) addNode(task *managerTask) {
	//设置目标的父节点id
	if task.IsFirst {
		task.Target.ParentUUID = protocol.AdminUuid
	} else {
		task.Target.ParentUUID = task.ParentUUID
		//去查找他的父节点的id num
		parentIDNum := topology.id2IDNum(task.ParentUUID)
		if parentIDNum >= 0 { //如果查到了，变将此节点的uuid添加到他的父节的childrenUUID中
			topology.nodes[parentIDNum].ChildrenUUID = append(topology.nodes[parentIDNum].ChildrenUUID, task.Target.Uuid)
		} else {
			topology.AddNodeResultChan <- -1
			return
		}
	}

	topology.nodes[topology.currentIDNum] = task.Target //使用当前编号（每当新加一个节点编号就加一）作为uuid num存贮节点信息
	topology.nodes[topology.currentIDNum].UuidNum = topology.currentIDNum
	topology.history[task.Target.Uuid] = topology.currentIDNum //根据uuid存贮uuid num

	topology.NumUuid[task.Target.Uuid] = topology.currentIDNum

	topology.AddNodeResultChan <- topology.currentIDNum //把当前编号返回

	topology.currentIDNum++ //编号加一
}

func (topology *Topology) getParentInfo(task *managerTask) {
	//根据uuid获取到当前节点的id num
	uuid := task.UUID
	uuidNum, ok := topology.NumUuid[uuid]
	if !ok {
		topology.GetParentNodeInfoResultChan <- nil
		return
	}
	//根据uuid num获取当前节点的信息
	nodeInfo, ok := topology.nodes[uuidNum]
	if !ok {
		topology.GetParentNodeInfoResultChan <- nil
		return
	}
	//根据父节点的uuid获取id num
	if nodeInfo.ParentUUID == protocol.AdminUuid {
		topology.GetParentNodeInfoResultChan <- &Node{Uuid: protocol.AdminUuid, ChildrenUUID: []string{uuid}}
		return
	}
	parentUuidNum, ok := topology.NumUuid[nodeInfo.ParentUUID]
	if !ok {
		topology.GetParentNodeInfoResultChan <- nil
		return
	}
	//根据id num获取父节点的信息
	parentNodeInfo, ok := topology.nodes[parentUuidNum]
	if !ok {
		topology.GetParentNodeInfoResultChan <- nil
		return
	}
	topology.GetParentNodeInfoResultChan <- parentNodeInfo
}

func (topology *Topology) getSingleNodeInfo(task *managerTask) {
	uuid := task.UUID
	if info, ok := topology.nodes[topology.id2IDNum(uuid)]; ok {
		topology.GetSingleNodeInfoResultChan <- info
	} else {
		topology.GetSingleNodeInfoResultChan <- nil
	}
}

// 重新初始化路由，得到去往指定节点的路径
func (topology *Topology) calculate() {
	newRouteInfo := make(map[string]string) // 创建一个route info

	for currentIDNum := range topology.nodes {
		var tempRoute []string
		currentID := topology.nodes[currentIDNum].Uuid
		tempIDNum := currentIDNum

		if topology.nodes[currentIDNum].ParentUUID == protocol.AdminUuid { //如果父节点是主节点，那么路由为空
			newRouteInfo[currentID] = ""
			continue
		}

		for {
			if topology.nodes[tempIDNum].ParentUUID != protocol.AdminUuid { //如果上级节点不是admin
				tempRoute = append(tempRoute, topology.nodes[tempIDNum].Uuid) //那么将当前uuid追加到路由中
				for nextIDNum := range topology.nodes {                       // Fix bug,thanks to @lz520520
					if topology.nodes[nextIDNum].Uuid == topology.nodes[tempIDNum].ParentUUID { //查找当前节点的父节点
						tempIDNum = nextIDNum //拿到父节点继续循环、追加
						break
					}
				}
			} else { //循环找节点找到最上层的父节点
				utils.StringSliceReverse(tempRoute)        //逆置字符串 正常是 孙，父，爷
				finalRoute := strings.Join(tempRoute, ":") //拼接成字符串    爷:父:孙
				newRouteInfo[currentID] = finalRoute       //设置成路由
				break
			}
		}
	}

	topology.route = newRouteInfo

	topology.CalculateResultChan <- true // Just tell upstream: work done!
}

func (topology *Topology) getRoute(task *managerTask) {
	route, ok := topology.route[task.UUID]
	topology.GetRouteResultChan <- routeInfo{
		route:   route,
		isExist: ok,
	}
}

// 更新节点信息
func (topology *Topology) updateDetail(task *managerTask) {
	uuidNum := topology.id2IDNum(task.UUID)
	if uuidNum >= 0 {
		var connectInfo *protocol.ConnectMethod
		err := json.Unmarshal([]byte(task.ConnectInfo), &connectInfo)
		if err != nil {
			connectInfo = nil
		}
		var systemInfo *system_info.SystemInfo
		err = json.Unmarshal([]byte(task.SystemInfo), &systemInfo)
		if err != nil {
			systemInfo = nil
		}

		topology.nodes[uuidNum].Memo = task.Memo
		topology.nodes[uuidNum].SystemInfo = systemInfo
		topology.nodes[uuidNum].ConnectInfo = connectInfo
		topology.GetSingleNodeInfoResultChan <- topology.nodes[uuidNum]
	} else {
		topology.GetSingleNodeInfoResultChan <- nil
	}
}

func (topology *Topology) getNodeTree() {
	var nodes []int
	for uuidNum := range topology.nodes {
		nodes = append(nodes, uuidNum)
	}

	utils.CheckRange(nodes)

	tree := NewRootTree()
	tree.Insert(topology.route, topology) //生成树

	topology.GetNodeTreeResultChan <- tree //返回树节点
}

func (topology *Topology) updateMemo(task *managerTask) {
	uuidNum := topology.id2IDNum(task.UUID)
	if uuidNum >= 0 {
		topology.nodes[uuidNum].Memo = task.Memo
	}
	topology.UpdateNodeMemoResultChan <- true
}

func (topology *Topology) delNode(task *managerTask) {
	// find all Children Node,del them
	var ready []int
	readyUUID := make(map[int]string)

	//这里并没有删除路由，是因为节点断线新增重连都会刷新路由信息，所以并不需要在这做管理
	idNum := topology.id2IDNum(task.UUID)                              //根据uuid获取uuid num
	parentIDNum := topology.id2IDNum(topology.nodes[idNum].ParentUUID) //获取断连节点的父节点的uuid num

	for pointer, childUUID := range topology.nodes[parentIDNum].ChildrenUUID { // 从他的父节点中删除该节点
		if childUUID == task.UUID {
			if pointer == len(topology.nodes[parentIDNum].ChildrenUUID)-1 {
				topology.nodes[parentIDNum].ChildrenUUID = topology.nodes[parentIDNum].ChildrenUUID[:pointer]
			} else {
				topology.nodes[parentIDNum].ChildrenUUID = append(topology.nodes[parentIDNum].ChildrenUUID[:pointer], topology.nodes[parentIDNum].ChildrenUUID[pointer+1:]...)
			}
		}
	}
	topology.findChildrenNodes(&ready, idNum)

	ready = append(ready, idNum)

	for _, num := range ready {
		readyUUID[num] = topology.idNum2ID(num)
		delete(topology.nodes, num) //从nodes将其删除
		delete(topology.NumUuid, readyUUID[num])
	}

	topology.DelNodeResultChan <- readyUUID //返回删除的所有节点的uuid
}

// 迭代获取所有子节点的uuidNum
func (topology *Topology) findChildrenNodes(ready *[]int, idNum int) {
	for _, uuid := range topology.nodes[idNum].ChildrenUUID {
		idNum = topology.id2IDNum(uuid)
		*ready = append(*ready, idNum)
		topology.findChildrenNodes(ready, idNum)
	}
}

// 根新添加节点类似，只不过在获取uuid num的时候，会优先检查当前uuid之前是否被设置过uuid num，会优先使用之前的
func (topology *Topology) reOnlineNode(task *managerTask) {
	//设置节点的父节点
	if task.IsFirst {
		task.Target.ParentUUID = protocol.AdminUuid
	} else {
		task.Target.ParentUUID = task.ParentUUID
		parentIDNum := topology.id2IDNum(task.ParentUUID)
		//将节点添加到父节点的子节点中
		topology.nodes[parentIDNum].ChildrenUUID = append(topology.nodes[parentIDNum].ChildrenUUID, task.Target.Uuid)
	}

	var idNum int
	if _, ok := topology.history[task.Target.Uuid]; ok {
		idNum = topology.history[task.Target.Uuid]
	} else {
		idNum = topology.currentIDNum
		topology.history[task.Target.Uuid] = idNum
		topology.currentIDNum++
	}
	topology.NumUuid[task.Target.Uuid] = idNum
	topology.nodes[idNum] = task.Target
	topology.nodes[idNum].UuidNum = idNum
	topology.ReOnLineResultChan <- true
}

func (topology *Topology) getNodeInfoByUuid(uuid string) (int, *Node) {
	uuidNum := topology.id2IDNum(uuid)
	if uuidNum == -1 {
		return -1, nil
	}
	v := topology.nodes[uuidNum]
	return uuidNum, v
}

func (topology *Topology) getNodeInfoByUuidNum(num int) *Node {
	node := topology.nodes[num]
	return node
}

// NodeTree 用于生成节点树
type NodeTree struct {
	IsEnding bool                 `json:"is_ending"`
	NodeInfo *Node                `json:"node_info"`
	Children map[string]*NodeTree `json:"children"` //map[Uuid]nodeTree   uuid为当前节点的uuid，所以这里有个问题，就是节点0没有id（node info里面有）
}

func NewNodeTree() *NodeTree {
	return &NodeTree{
		IsEnding: false,
		Children: make(map[string]*NodeTree),
	}
}

type RootNodeTree struct {
	Root *NodeTree `json:"root"`
}

func NewRootTree() *RootNodeTree {
	return &RootNodeTree{NewNodeTree()}
}

func (r *RootNodeTree) Insert(route map[string]string, t *Topology) {
	rootNode := r.Root
	rootNode.NodeInfo = t.getNodeInfoByUuidNum(0)
	//将uuid根据id num排序
	for _, j := range route {
		for _, singleRoute := range strings.Split(j, ":") {
			if singleRoute == "" {
				break
			}
			if _, ok := rootNode.Children[singleRoute]; !ok { //如果不存在这个节点，就新建一个
				rootNode.Children[singleRoute] = NewNodeTree()
				rootNode = rootNode.Children[singleRoute] //跳转到当前节点
				_, nodeInfo := t.getNodeInfoByUuid(singleRoute)
				if nodeInfo != nil {
					rootNode.NodeInfo = nodeInfo
				}
				continue
			} else { //如果已经存在此节点，就不需要在新建了
				rootNode = rootNode.Children[singleRoute]
				continue
			}
		}
		if len(rootNode.Children) == 0 {
			rootNode.IsEnding = true
		}
		rootNode = r.Root //重新变成根节点
	}
}
