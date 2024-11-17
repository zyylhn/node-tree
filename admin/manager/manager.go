package manager

import (
	"context"
	"encoding/json"
	"github.com/kataras/golog"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"strings"
)

// Manager is used to maintain all status and keep different parts connected
// Really complicated when trying to keep running stable
// So there may be some bugs in manager
// Plz let me know if panic when you are using forward/stop forward/backward/stop backward/socks/stop socks or node offline
type Manager struct {
	FileUploadManager *fileUploadManager
	SocksManager      *socksManager
	ForwardManager    *forwardManager
	BackwardManager   *backwardManager
	SSHManager        *sshManager
	SSHTunnelManager  *sshTunnelManager
	ShellManager      *shellManager
	ListenManager     *listenManager
	ConnectManager    *connectManager
	ReSetAgentOption  *reSetAgentOption
	RemoteLoadManager *remoteLoadManager
	ManagerLog        *public.NodeManagerLog

	ChildrenMessChan chan *protocol.CompletePacket //子节点下线重连消息
	InfoMessChan     chan *protocol.CompletePacket //节点信息消息
	OutPushMessage   chan interface{}              //向外推送的消息

	t *topology.Topology
}

//todo manager关闭问题

func NewManager(logger *golog.Logger, t *topology.Topology, outPutChan chan interface{}) *Manager {
	re := new(Manager)
	re.ChildrenMessChan = make(chan *protocol.CompletePacket, 5)
	re.InfoMessChan = make(chan *protocol.CompletePacket, 5)
	re.OutPushMessage = outPutChan
	re.ManagerLog = public.NewNodeManagerLog(logger, 20)
	re.FileUploadManager = newFileUploadManager(re.ManagerLog, t)
	re.SocksManager = newSocksManager(re.ManagerLog, t)
	re.ForwardManager = newForwardManager(re.ManagerLog, t)
	re.BackwardManager = newBackwardManager(re.ManagerLog, t)
	re.SSHManager = newSSHManager(re.ManagerLog, t)
	re.SSHTunnelManager = newSSHTunnelManager(re.ManagerLog, t)
	re.ShellManager = newShellManager(re.ManagerLog, t)
	re.ListenManager = newListenManager(re.ManagerLog, t, re.OutPushMessage)
	re.ConnectManager = newConnectManager(re.ManagerLog, t)
	re.ReSetAgentOption = newReSetAgentOption(re.ManagerLog, t)
	re.RemoteLoadManager = newLoadModuleManager(re.ManagerLog, t)
	re.t = t
	return re
}

func (m *Manager) Close() {
	//todo g关闭channel
	close(m.OutPushMessage)
}

func (m *Manager) Run(ctx context.Context) {
	go m.SocksManager.run(ctx)
	go m.ForwardManager.run(ctx)
	go m.BackwardManager.run(ctx)
	go m.RemoteLoadManager.run(ctx)
	go m.FileUploadManager.run(ctx)
	m.startDispatch(ctx)
}

func (m *Manager) startDispatch(ctx context.Context) {
	go m.DispatchChildrenMess(ctx)
	go m.DispatchInfo(ctx)
	go m.FileUploadManager.Dispatch(ctx)
	go m.ForwardManager.Dispatch(ctx)
	go m.BackwardManager.Dispatch(ctx)
	go m.ListenManager.Dispatch(ctx)
	go m.ConnectManager.Dispatch(ctx)
	go m.ReSetAgentOption.Dispatch(ctx)
	go m.RemoteLoadManager.Dispatch(ctx)
	go m.SocksManager.Dispatch(ctx)
	go m.SSHManager.Dispatch(ctx)
	go m.SSHTunnelManager.Dispatch(ctx)
	go m.ShellManager.Dispatch(ctx)
	go m.ConnectManager.Dispatch(ctx)
}

func (m *Manager) DispatchChildrenMess(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case packet := <-m.ChildrenMessChan:
			switch mess := packet.ReqBody.(type) {
			case *protocol.NodeOffline:
				m.nodeOffline(mess.UUID)
			case *protocol.NodeReOnline:
				m.nodeReOnline(mess, packet.ReqHeader)
			}
		}
	}
}

func (m *Manager) nodeOffline(node string) {
	//将断点信息传递回去f

	mess, err := m.GetUpAndDownStreamNode(node)
	if err != nil {
		m.ManagerLog.GeneralError(err)
	}

	m.OutPushMessage <- mess

	allNodes := m.t.DelNode(node) //所有被删除的节点

	for idNum, nodeUUID := range allNodes { //强行结束和删除节点的服务
		//强制停止反向端口转发
		m.BackwardManager.ForceStopOnNode(nodeUUID)
		//强制停止端口转发
		m.ForwardManager.ForceStopOnNode(nodeUUID)

		//强制停止socks
		m.SocksManager.ForceStopOnNode(nodeUUID)
		//强制停止远程模块加载
		m.RemoteLoadManager.StopNodeAllTask(nodeUUID)
		//强制停止文件上传
		m.FileUploadManager.StopNodeTask(nodeUUID)
		m.ManagerLog.GeneralErrorf("node %d (id=%v) record is deleted!", idNum, nodeUUID)
	}
	m.t.Calculate()
}

func (m *Manager) nodeReOnline(mess *protocol.NodeReOnline, header *protocol.Header) {
	//检查id是否已经存在，然后返回检查结果，根据检查结果决定是否更新服务端
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	route, ok := m.t.GetRoute(header.Sender)
	if !ok {
		return
	}

	resHeader := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    header.Sender,
		MessageType: protocol.MessageTypeNodeReOnlineRes,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(header.TaskID)),
		TaskID:      header.TaskID,
	}
	resMess := &protocol.NodeReOnlineRes{
		OK: 1,
	}

	allNode := m.t.GetAllUuid()
	if _, ok := allNode[mess.UUID]; ok {
		//该节点已经存在，返回失败消息
		resMess.OK = 0
		protocol.ConstructMessage(sMessage, resHeader, resMess, false)
		sMessage.SendMessage()
		if mess.Backlink == 1 {
			m.ManagerLog.GeneralErrorf("the child node failed to be reconnected. The node with this id is already online. Related information -> Parent node :%v, listener port :%v, corresponding online node :%v, retry address :%v", mess.ParentUUID, mess.ConnectPort, mess.UUID, mess.IP)
		}
		return
	}
	protocol.ConstructMessage(sMessage, resHeader, resMess, false)
	sMessage.SendMessage()
	node := topology.NewNode(mess.UUID, mess.IP)
	m.t.ReOnLineNode(false, node, mess.ParentUUID)
	m.t.Calculate()
	m.OutPushMessage <- &topology.NodeReOnline{Node: node}
}

// GetUpAndDownStreamNode 获取以指定节点的父节点和子节点信息
func (m *Manager) GetUpAndDownStreamNode(node string) (*NodeUpAndDownStreamInfo, error) {
	var err error
	re := new(NodeUpAndDownStreamInfo)
	re.ParentNode, err = m.t.GetParentInfo(node)
	if err != nil {
		return nil, err
	}
	idNum := m.t.GetUUIDNum(node)
	allNodeTree := m.t.GetNodeTree()
	agent0 := allNodeTree.Root
	if idNum == agent0.NodeInfo.UuidNum {
		re.ChildrenNodeTree = agent0
	} else {
		//获取该节点的路由
		route, ok := m.t.GetRoute(node)
		if !ok {
			return nil, newNodeNotFoundError(node)
		}
		//拆分路由
		routList := strings.Split(route, ":")
		//根据路由从树中获取到他的位置
		var nodeTree *topology.NodeTree
		nodeTree = agent0 //等于agent0
		for _, uuid := range routList {
			nodeTree = nodeTree.Children[uuid]
		}
		re.ChildrenNodeTree = nodeTree
	}
	return re, nil
}

func (m *Manager) shutdown(node string, deleteSelf bool) {
	route, ok := m.t.GetRoute(node)
	if !ok {
		return
	}
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeUpStreamReOnlineShutdown,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	shutdownMess := &protocol.Shutdown{
		OK: 1,
	}
	if deleteSelf {
		shutdownMess.DeleteSelf = uint16(1)
	}

	protocol.ConstructMessage(sMessage, header, shutdownMess, false)
	sMessage.SendMessage()
}

// Shutdown 停止指定节点运行
func (m *Manager) Shutdown(node string) {
	m.shutdown(node, false)
}

// Unload 卸载指定节点
func (m *Manager) Unload(node string) {
	m.shutdown(node, true)
}

func (m *Manager) shutdownAll(node string, deleteSelf bool) {
	route, ok := m.t.GetRoute(node)
	if !ok {
		return
	}
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeShutdownAll,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	shutdownMess := &protocol.ShutdownAll{}
	if deleteSelf {
		shutdownMess.DeleteSelf = uint16(1)
	}

	protocol.ConstructMessage(sMessage, header, shutdownMess, false)
	sMessage.SendMessage()
}

// ShutdownAll 停止指定节点和子节点运行
func (m *Manager) ShutdownAll(node string) {
	m.shutdownAll(node, false)
}

// UnloadAll 卸载指定节点和子节点
func (m *Manager) UnloadAll(node string) {
	m.shutdownAll(node, true)
}

func (m *Manager) letOffline(node string) {
	route, ok := m.t.GetRoute(node)
	if !ok {
		return
	}
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeActiveOffLine,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	shutdownMess := &protocol.ActiveOffline{}

	protocol.ConstructMessage(sMessage, header, shutdownMess, false)
	sMessage.SendMessage()
}

func (m *Manager) DispatchInfo(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-m.InfoMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						m.ManagerLog.GeneralErrorf("info handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.MyInfo:
					var memo []string
					err := json.Unmarshal([]byte(mess.Memo), &memo)
					if err != nil {
						m.ManagerLog.GeneralErrorf("unmarshal node push memo information error:%v", mess.Memo)
					}
					node := m.t.UpdateDetail(mess.UUID, memo, mess.SystemInfo, mess.ConnectInfo)
					if node == nil {
						m.ManagerLog.GeneralErrorf("update ndoe info error:node does not exist")
					} else {
						m.OutPushMessage <- &topology.NodeUpdate{Node: node}
					}
				}
			}(messageData)
		}
	}
}

// AddMemo 为指定节点添加备注信息
func (m *Manager) AddMemo(info []string, node string) error {
	route, ok := m.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	memo, err := json.Marshal(info)
	if err != nil {
		return err
	}

	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeMyMemo,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	myMemoMess := &protocol.MyMemo{
		MemoLen: uint64(len(memo)),
		Memo:    string(memo),
	}

	protocol.ConstructMessage(sMessage, header, myMemoMess, false)
	sMessage.SendMessage()
	RefreshAgentInfo(node, route)
	return nil
}

func (m *Manager) DelMemo(node string) {
	route, ok := m.t.GetRoute(node)
	if !ok {
		return
	}
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeMyMemo,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	myMemoMess := &protocol.MyMemo{
		MemoLen: uint64(len("null")),
		Memo:    "null",
	}

	protocol.ConstructMessage(sMessage, header, myMemoMess, false)
	sMessage.SendMessage()
	RefreshAgentInfo(node, route)
}
