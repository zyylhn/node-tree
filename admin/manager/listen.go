package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	NORMAL = iota
)

type listenManager struct {
	ListenReady    sync.Map //是否监听成功
	StopOK         sync.Map //是否删除成功
	ListenerAddr   sync.Map
	NodeUpThread   sync.Map //新的节点通过主动连接的方式上线
	ListenMessChan chan *protocol.CompletePacket

	log         *public.NodeManagerLog
	t           *topology.Topology
	pushMessage chan interface{}
}

func newListenManager(log *public.NodeManagerLog, t *topology.Topology, messChan chan interface{}) *listenManager {
	manager := new(listenManager)
	manager.ListenMessChan = make(chan *protocol.CompletePacket, 5)
	manager.ListenReady = sync.Map{}
	manager.StopOK = sync.Map{}
	manager.ListenerAddr = sync.Map{}
	manager.NodeUpThread = sync.Map{}
	manager.t = t
	manager.log = log
	manager.pushMessage = messChan
	return manager
}

func (l *listenManager) newListener(node string, taskID string, stop bool, wait bool, addr string) error {
	route, ok := l.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	method := NORMAL                         //目前还没有其他类型
	ip, port, err := utils.SplitIpPort(addr) //todo 更完善的检查,这个检查有缺陷。如果给:9001，会监听到0.0.0.0:9001，但是停止的时候也要使用0.0.0.0:9001不能使用：9001这块后面看看怎么优化吧
	if err != nil {
		return err
	}
	addr = net.JoinHostPort(ip, strconv.Itoa(port))

	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeListenReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}

	listenReqMess := &protocol.ListenReq{
		Method:  uint16(method),
		AddrLen: uint64(len(addr)),
		Addr:    addr,
	}
	if stop {
		listenReqMess.StopListener = 1
	}
	if wait {
		listenReqMess.Wait = 1
	}
	l.ListenReady.Store(taskID, make(chan string))
	defer func() {
		v, ok := l.ListenReady.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		l.ListenReady.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, listenReqMess, false)
	sMessage.SendMessage()
	channel, ok := l.ListenReady.Load(taskID)
	if !ok {
		return errors.New("not find listen ready channel")
	}
	select {
	case re := <-channel.(chan string):
		if re == "" {
			return nil
		} else {
			return errors.New(fmt.Sprintf("node cannot listen on %s:%v", addr, re))
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("listen error:" + ChannelTimeOut.Error())
	}
}

// NewListener 在指定节点上新建一个监听起autoStop代表在接收到一个节点上线时是否关闭该监听器
func (l *listenManager) NewListener(node string, autoStop bool, addr string) error {
	taskID := utils.GenerateUUID()
	return l.newListener(node, taskID, autoStop, false, addr)
}

// NewListenerNode 在指定节点上创建监听器并等到节点上线返回上线节点的id。完成后自动关闭监听器
func (l *listenManager) NewListenerNode(ctx context.Context, node string, addr string) (string, error) {
	taskID := utils.GenerateUUID()
	_, ok := l.t.GetRoute(node)
	if !ok {
		return "", newNodeNotFoundError(node)
	}
	l.NodeUpThread.Store(taskID, make(chan string))
	defer func() {
		v, ok := l.NodeUpThread.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		l.NodeUpThread.Delete(taskID)
	}()
	err := l.newListener(node, taskID, true, true, addr)
	if err != nil {
		return "", err
	}
	channel, ok := l.NodeUpThread.Load(taskID)
	if !ok {
		return "", errors.New("not find node up thread channel")
	}
	select {
	case childUuid := <-channel.(chan string):
		if childUuid != "" {
			return childUuid, nil
		} else {
			return "", errors.New("listener start success but don't get new node uuid")
		}
	case <-ctx.Done():
		err = l.StopListener(node, addr)
		if err != nil {
			err = l.StopListener(node, "0.0.0.0"+addr) //todo 暂时应对0.0.0.0:port的情况
			if err != nil {
				return "", errors.New("listener start success but wait node revers timeout and close listener error:" + err.Error())
			}
		}
		return "", errors.New("listener start success but wait node revers timeout(context cancel)")
	}
}

func (l *listenManager) StopListener(node string, choice string) error {
	route, ok := l.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	taskID := utils.GenerateUUID()
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeListenerCloseReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	var stopMess *protocol.StopListenerReq
	if choice == "all" {
		stopMess = &protocol.StopListenerReq{
			All: 1,
		}
	} else {
		stopMess = &protocol.StopListenerReq{
			All:     0,
			ADDRLen: uint64(len([]byte(choice))),
			ADDR:    choice,
		}
	}
	l.StopOK.Store(taskID, make(chan bool))
	defer func() {
		v, ok := l.StopOK.Load(taskID)
		if ok {
			close(v.(chan bool))
		}
		l.StopOK.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, stopMess, false)
	sMessage.SendMessage()
	channel, ok1 := l.StopOK.Load(taskID)
	if !ok1 {
		return errors.New("not find stop listen ok channel")
	}
	select {
	case ok := <-channel.(chan bool):
		if ok {
			return nil
		} else {
			return errors.New("stop the listener failed.listener does not exist")
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("stop listener error:" + ChannelTimeOut.Error())
	}
}

func (l *listenManager) GetNodeListener(node string) ([]string, error) {
	route, ok := l.t.GetRoute(node)
	if !ok {
		return nil, newNodeNotFoundError(node)
	}
	taskID := utils.GenerateUUID()
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeGetListenerReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	getListenerMess := &protocol.GetListenerReq{OK: 1}
	l.ListenerAddr.Store(taskID, make(chan string))
	defer func() {
		v, ok := l.ListenerAddr.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		l.ListenerAddr.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, getListenerMess, false)
	sMessage.SendMessage()
	channel, ok := l.ListenerAddr.Load(taskID)
	if !ok {
		return nil, errors.New("not find download result channel")
	}
	select {
	case re := <-channel.(chan string):
		if re == "null" {
			return []string{}, nil
		}
		var addrList []string
		err := json.Unmarshal([]byte(re), &addrList)
		if err != nil {
			return nil, err
		}
		return addrList, nil
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return nil, errors.New("get listener error:" + ChannelTimeOut.Error())
	}
}

func (l *listenManager) dispatchChildUUID(parentUUID, ip string, taskID string) {
	uuid := utils.GenerateUUID()
	node := topology.NewNode(uuid, ip)
	_ = l.t.AddNode(false, node, parentUUID)
	//刷新节点路由
	l.t.Calculate()
	//获取父节点的路由
	route, _ := l.t.GetRoute(uuid)
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    parentUUID,
		MessageType: protocol.MessageTypeChildUuidRes,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}

	cUUIDResMess := &protocol.ChildUUIDRes{
		UUIDLen: uint16(len(uuid)),
		UUID:    uuid,
	}

	protocol.ConstructMessage(sMessage, header, cUUIDResMess, false)
	sMessage.SendMessage()

	l.pushMessage <- &topology.NodeJoin{Node: node}
}

func (l *listenManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-l.ListenMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						l.log.GeneralErrorf("listen handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.ListenRes:
					if channel, ok := l.ListenReady.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Err
					} else {
						l.log.GeneralErrorf("listen ready %v", ChannelClose)
					}
				case *protocol.ChildUUIDReq: //新节点的父节点来通知主节点有新节点加入
					go l.dispatchChildUUID(mess.ParentUUID, mess.Addr, packet.ReqHeader.TaskID)
				case *protocol.StopListenerRes:
					if channel, ok := l.StopOK.Load(packet.ReqHeader.TaskID); ok {
						if mess.OK == 1 {
							channel.(chan bool) <- true
						} else {
							channel.(chan bool) <- false
						}
					} else {
						l.log.GeneralErrorf("stop linstener %v", ChannelClose)
					}
				case *protocol.GetListenerRes:
					//接收到的是字符串，需要给反序列化成[]string
					if channel, ok := l.ListenerAddr.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Listener
					} else {
						l.log.GeneralErrorf("get all lisntener %v", ChannelClose)
					}
				case *protocol.ReverseNodeUuid:
					if channel, ok := l.NodeUpThread.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Uuid
					} else {
						l.log.GeneralErrorf("get node reverse node uuid %v", ChannelClose)
					}
				}
			}(messageData)
		}
	}
}
