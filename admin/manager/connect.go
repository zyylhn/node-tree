package manager

import (
	"context"
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"sync"
	"time"
)

type connectManager struct {
	ConnectMessChan chan *protocol.CompletePacket
	ConnectReady    sync.Map

	t   *topology.Topology
	log *public.NodeManagerLog
}

func newConnectManager(log *public.NodeManagerLog, t *topology.Topology) *connectManager {
	manager := new(connectManager)
	manager.ConnectMessChan = make(chan *protocol.CompletePacket, 5)
	manager.ConnectReady = sync.Map{}
	manager.log = log
	manager.t = t
	return manager
}

// ConnectToAgent 命令指定节点根据地址等信息去连接新的agent，并返回新的agent的uuid
func (c *connectManager) ConnectToAgent(node, addr, proxy, proxyUser, proxyPass string) (string, error) {
	route, ok := c.t.GetRoute(node)
	if !ok {
		return "", newNodeNotFoundError(node)
	}
	normalAddr, _, err := utils.CheckIPPort(addr)
	if err != nil {
		return "", err
	}
	taskID := utils.GenerateUUID()
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeConnectStart,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	connMess := &protocol.ConnectStart{
		AddrLen:   uint16(len([]byte(normalAddr))),
		Addr:      normalAddr,
		ProxyLen:  uint16(len(proxy)),
		Proxy:     proxy,
		ProxyPLen: uint16(len(proxyPass)),
		ProxyP:    proxyPass,
		ProxyULen: uint16(len(proxyUser)),
		ProxyU:    proxyUser,
	}
	c.ConnectReady.Store(taskID, make(chan *protocol.ConnectRes))
	defer func() {
		v, ok := c.ConnectReady.Load(taskID)
		if ok {
			close(v.(chan *protocol.ConnectRes))
		}
		c.ConnectReady.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, connMess, false)
	sMessage.SendMessage()
	channel, ok := c.ConnectReady.Load(taskID)
	if !ok {
		return "", errors.New("not find connect ready channel")
	}
	select {
	case connectRes := <-channel.(chan *protocol.ConnectRes):
		if connectRes.ErrLen == 0 {
			return connectRes.Uuid, nil
		} else {
			return "", errors.New(fmt.Sprintf("cannot initial to node %s:%v", addr, connectRes.Err))
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return "", errors.New("connect error:" + ChannelTimeOut.Error())
	}
}

func (c *connectManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-c.ConnectMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						c.log.GeneralErrorf("connect handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.ConnectRes:
					if channel, ok := c.ConnectReady.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan *protocol.ConnectRes) <- mess
					} else {
						c.log.GeneralErrorf("connect %v", ChannelClose)
					}
				}
			}(messageData)
		}
	}
}
