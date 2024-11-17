package manager

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"sync"
	"time"
)

type reSetAgentOption struct {
	ReSetAgentOption chan *protocol.CompletePacket
	Information      sync.Map //使用uuid作为索引如果成功会返回Ok,失败返回错误原因
	OptionsInfo      sync.Map //返回agent节点的参数信息

	log *public.NodeManagerLog
	t   *topology.Topology
}

func newReSetAgentOption(log *public.NodeManagerLog, t *topology.Topology) *reSetAgentOption {
	manager := new(reSetAgentOption)
	manager.ReSetAgentOption = make(chan *protocol.CompletePacket, 5)
	manager.Information = sync.Map{}
	manager.OptionsInfo = sync.Map{}
	manager.log = log
	manager.t = t
	return manager
}

func (r *reSetAgentOption) GetAgentOption(node string) (*protocol.Options, error) {
	route, ok := r.t.GetRoute(node)
	if !ok {
		return nil, newNodeNotFoundError(node)
	}
	taskID := utils.GenerateUUID()
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeGetAgentOptionsReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	mess := &protocol.GETAgentOptionReq{}
	r.OptionsInfo.Store(taskID, make(chan string))
	defer func() {
		v, ok := r.OptionsInfo.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		r.OptionsInfo.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, mess, false)
	sMessage.SendMessage()
	channel, ok := r.OptionsInfo.Load(taskID)
	if !ok {
		return nil, errors.New("not find reset option info channel")
	}
	select {
	case agentOptions := <-channel.(chan string):
		if agentOptions != "" {
			var options *protocol.Options
			err := json.Unmarshal([]byte(agentOptions), &options)
			if err != nil {
				return nil, err
			}
			return options, nil
		} else {
			return nil, errors.New("get options error:agent return options information id null")
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return nil, errors.New("get option error:" + ChannelTimeOut.Error())
	}
}

func (r *reSetAgentOption) ResetAgentOption(op *protocol.Options, node string) error {
	route, ok := r.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	taskID := utils.GenerateUUID()
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeReSetAgentOptionReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}

	reSetByte, err := json.Marshal(op)
	if err != nil {
		return err
	}

	reSetMessage := &protocol.RestAgentOptionReq{
		OpLen: uint32(uint64(len(reSetByte))),
		Op:    string(reSetByte),
	}
	r.Information.Store(taskID, make(chan string))
	defer func() {
		v, ok := r.Information.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		r.Information.Delete(taskID)
	}()
	//发送修改请求等待响应
	protocol.ConstructMessage(sMessage, header, reSetMessage, false)
	sMessage.SendMessage()
	channel, ok := r.Information.Load(taskID)
	if !ok {
		return errors.New("not find reset option channel")
	}
	select {
	case re := <-channel.(chan string):
		if re == "OK" {
			RefreshAgentInfo(node, route)
			return nil
		} else {
			return errors.New(re)
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("reset option error:" + ChannelTimeOut.Error())
	}
}

func (r *reSetAgentOption) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-r.ReSetAgentOption:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if re := recover(); r != nil {
						r.log.GeneralErrorf("agentOptions handle panic:%v", re)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.ResetAgentOptionRes:
					if channel, ok := r.Information.Load(packet.ReqHeader.TaskID); ok {
						if mess.ErrorLen == 0 {
							channel.(chan string) <- "OK"
						} else {
							channel.(chan string) <- mess.Error
						}
					} else {
						r.log.GeneralErrorf("reset option %v", ChannelClose)
					}
				case *protocol.GETAgentOptionRes:
					if channel, ok := r.OptionsInfo.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Option
					} else {
						r.log.GeneralErrorf("get option %v", ChannelClose)
					}
				}
			}(messageData)
		}
	}
}
