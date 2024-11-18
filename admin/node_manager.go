package admin

import (
	"context"
	"fmt"
	"github.com/kataras/golog"
	"github.com/zyylhn/node-tree/admin/initial"
	"github.com/zyylhn/node-tree/admin/manager"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"io"
	"net"
)

type NodeManager struct {
	Log          *golog.Logger
	StartOptions *initial.Options

	Topology *topology.Topology
	Mgr      *manager.Manager

	ctx    context.Context
	cancel context.CancelFunc

	downStream           string                     //下游协议类型
	downStreamConnection *protocol.MessageComponent //下游连接信息
}

func NewAdmin(op *initial.Options, log *golog.Logger) *NodeManager {
	re := new(NodeManager)
	re.Log = log
	re.StartOptions = op
	protocol.Downstream = op.Downstream
	return re
}

//todo 测试admin停止的协程泄漏情况

// Start 启动节点管理服务和节点功能manager,给定一个pushChannel 会推送一些节点消息等等，可以为空
func (n *NodeManager) Start(ctx context.Context, pushChan chan interface{}) error {
	if pushChan == nil {
		pushChan = make(chan interface{}, 100)
		go func() {
			for range pushChan {
			}
		}()
	}
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.Topology = topology.NewTopology()
	go n.Topology.Run(n.ctx)
	var conn net.Conn
	var err error
	//检查连接方式
	conn, err = initial.GetConnWithAgent(n.StartOptions, n.Topology)
	if err != nil {
		return fmt.Errorf("admin initial with agent error:%v", err)
	}
	n.Topology.Calculate() //刷新路由
	n.downStreamConnection = n.newDownStreamConnection(conn)
	public.DownstreamConnection = n.downStreamConnection //todo 暂时还是使用global的吧，后面把消息发送重构一下
	n.startManager(pushChan)
	go n.handleMessFromDownstream()
	return nil
}

func (n *NodeManager) newDownStreamConnection(conn net.Conn) *protocol.MessageComponent {
	return &protocol.MessageComponent{
		UUID:   protocol.AdminUuid,
		Conn:   conn,
		Secret: n.StartOptions.Secret,
	}
}

// Close 结束manager
func (n *NodeManager) Close() {
	//todo 关闭时所有channel和协程的关闭
	//todo 其他内容的处理
	if n.downStreamConnection != nil && n.downStreamConnection.Conn != nil {
		_ = n.downStreamConnection.Conn.Close() //todo 暂时这样处理，不然admin停不了
	}
	n.cancel()
}

func (n *NodeManager) CloseWithAllNode(unload bool) {
	tree := n.Topology.GetNodeTree()
	if unload {
		n.Mgr.UnloadAll(tree.Root.NodeInfo.Uuid)
	} else {
		n.Mgr.ShutdownAll(tree.Root.NodeInfo.Uuid)
	}
	n.Close()

}

func (n *NodeManager) startManager(pushChan chan interface{}) {
	n.Mgr = manager.NewManager(n.Log, n.Topology, pushChan)
	n.Mgr.Run(n.ctx)
}

// 主节点监听使用全局的连接，不断监听发送过来的消息，然后通过manager中channel发送给对应的handle
// todo  优化调度机制，这个函数承接了所有的消息路由，如果有速度需求的话需要对其进行优化
func (n *NodeManager) handleMessFromDownstream() {
	rMessage := protocol.PrepareAndDecideWhichRProtoFromLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	for {
		select {
		case <-n.ctx.Done():
			return
		//todo 用default不能立马监听到ctx的停止这块后面考虑优化一下,甚至根本不会停
		default:
			header, message, err := protocol.DestructMessage(rMessage)
			if err != nil {
				n.Log.Errorf("fail to read message from agent0 connect:%v", err)
				return
			}
			packet := &protocol.CompletePacket{ReqHeader: header, ReqBody: message}
			switch header.MessageType {
			case protocol.MessageTypeMyInfo:
				n.Mgr.InfoMessChan <- packet
			case protocol.MessageTypeShellRes:
				fallthrough
			case protocol.MessageTypeShellResult:
				fallthrough
			case protocol.MessageTypeShellExit:
				n.Mgr.ShellManager.ShellMessChan <- packet
			case protocol.MessageTypeSshRes:
				fallthrough
			case protocol.MessageTypeSshResult:
				fallthrough
			case protocol.MessageTypeSshExit:
				n.Mgr.SSHManager.SSHMessChan <- packet
			case protocol.MessageTypeSshTunnelRes:
				n.Mgr.SSHTunnelManager.SSHTunnelMessChan <- packet
			case protocol.MessageTypeUploadFileChecksSizeRes:
				fallthrough
			case protocol.MessageTypeFileUploadRes:
				fallthrough
			case protocol.MessageTypeWriteFileError:
				n.Mgr.FileUploadManager.FileUploadMessChan <- packet
			case protocol.MessageTypeSocksReady:
				fallthrough
			case protocol.MessageTypeSocksTcpData:
				fallthrough
			case protocol.MessageTypeSocksTcpFin:
				fallthrough
			case protocol.MessageTypeUdpAssReq:
				fallthrough
			case protocol.MessageTypeSocksUdpData:
				n.Mgr.SocksManager.SocksMessChan <- packet
			case protocol.MessageTypeForwardReady:
				fallthrough
			case protocol.MessageTypeForwardData:
				fallthrough
			case protocol.MessageTypeForwardFin:
				n.Mgr.ForwardManager.ForwardMessChan <- packet
			case protocol.MessageTypeBackwardReady:
				fallthrough
			case protocol.MessageTypeBackwardData:
				fallthrough
			case protocol.MessageTypeBackwardFin:
				fallthrough
			case protocol.MessageTypeBackwardStopDone:
				fallthrough
			case protocol.MessageTypeBackwardStart:
				n.Mgr.BackwardManager.BackwardMessChan <- packet
			case protocol.MessageTypeChildUuidReq: // include "initial" && "listen" func, let ListenManager do all this stuff,ConnectManager can just watch
				fallthrough
			case protocol.MessageTypeGetListenerRes:
				fallthrough
			case protocol.MessageTypeListenerCloseRes:
				fallthrough
			case protocol.MessageTypeReverseNodeUuid:
				fallthrough
			case protocol.MessageTypeListenRes:
				n.Mgr.ListenManager.ListenMessChan <- packet
			case protocol.MessageTypeConnectDone:
				n.Mgr.ConnectManager.ConnectMessChan <- packet
			case protocol.MessageTypeNodeReOnline:
				fallthrough
			case protocol.MessageTypeNodeOffLine:
				n.Mgr.ChildrenMessChan <- packet
			case protocol.MessageTypeGetAgentOptionRes:
				fallthrough
			case protocol.MessageTypeReSetAgentOptionRes:
				n.Mgr.ReSetAgentOption.ReSetAgentOption <- packet
			case protocol.MessageTypeSendTaskInfoRes:
				fallthrough
			case protocol.MessageTypeSendModuleData:
				fallthrough
			case protocol.MessageTypeSendModuleEnd:
				fallthrough
			case protocol.MessageTypeCheckSizeResult:
				fallthrough
			case protocol.MessageTypeLoadResultInfo:
				n.Mgr.RemoteLoadManager.LoadModuleMessChan <- packet
			default:
				n.Log.Errorf("Unknown message type:%v,read form agent0", header.MessageType)
			}
		}
	}
}

func (n *NodeManager) ConnectToNewAgent(node, addr, proxy, proxyUser, proxyPass string) (string, error) {
	return n.Mgr.ConnectManager.ConnectToAgent(node, addr, proxy, proxyUser, proxyPass)
}

// CreateListener Creates a listener on the specified node. 'autoStop' is used to set whether to automatically pause when a node is received online, and 'addr' is the listening address on the specified node
func (n *NodeManager) CreateListener(node string, autoStop bool, addr string) error {
	return n.Mgr.ListenManager.NewListener(node, autoStop, addr)
}

// CreateWaitOnLineListener Unlike 'CreateListener', which creates a single listener, 'CreateWaitOnLineListener' creates the listener and waits for the node to go online, then automatically closes the listener and returns the node id that went online
func (n *NodeManager) CreateWaitOnLineListener(ctx context.Context, node string, addr string) (string, error) {
	return n.Mgr.ListenManager.NewListenerNode(ctx, node, addr)
}

func (n *NodeManager) GetListener(node string) ([]string, error) {
	return n.Mgr.ListenManager.GetNodeListener(node)
}

// StopListener Stop the listeners at the specified address on the specified node and stop all listeners on the node when 'choice' is' all '
func (n *NodeManager) StopListener(node string, choice string) error {
	return n.Mgr.ListenManager.StopListener(node, choice)
}

// CreateRemoteLoad todo 去掉cancel，只传输ctx
func (n *NodeManager) CreateRemoteLoad(module io.ReadCloser, args string, moduleName string, ctx *context.Context, c *context.CancelFunc) (*manager.RemoteLoad, error) {
	return n.Mgr.RemoteLoadManager.NewRemoteLoad(module, args, moduleName, ctx, c)
}

// CreateBackward Create a reverse port forwarding. The agent listens to the specified rPort and forwards all requests to the rPort to lAddr
func (n *NodeManager) CreateBackward(lAddr string, rPort string, node string) error {
	return n.Mgr.BackwardManager.Create(lAddr, rPort, node)
}

// StopBackward To stop port forwarding on a specified port on a specified node, the choice is either 'all' or the selected rPort
func (n *NodeManager) StopBackward(node string, choice string) error {
	return n.Mgr.BackwardManager.StopWithChoice(node, choice)
}

func (n *NodeManager) GetBackWardInfo(node string) map[string]*manager.Backward {
	return n.Mgr.BackwardManager.GetInfoSingleNode(node)
}
