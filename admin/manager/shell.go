package manager

import (
	"context"
	"errors"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"strings"
	"sync"
	"time"
)

type shellManager struct {
	ShellMessChan chan *protocol.CompletePacket
	ShellReady    sync.Map
	ShellExit     sync.Map
	ShellResult   sync.Map

	log *public.NodeManagerLog
	t   *topology.Topology
}

func newShellManager(log *public.NodeManagerLog, t *topology.Topology) *shellManager {
	manager := new(shellManager)
	manager.ShellMessChan = make(chan *protocol.CompletePacket, 5)
	manager.ShellReady = sync.Map{}
	manager.ShellExit = sync.Map{}
	manager.ShellResult = sync.Map{}
	manager.log = log
	manager.t = t

	return manager
}

func (s *shellManager) OpenTerminal(ctx context.Context, node string, shell string, stdin, stdout, stderr chan string) error {
	route, ok := s.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	if strings.Contains(shell, "bash") {
		shell = "/bin/bash"
	} else if strings.Contains(shell, "sh") {
		shell = "/bin/sh"
	} else if shell == "" {
	} else {
		return errors.New("virtual terminals support bash and sh only")
	}
	taskID := utils.GenerateUUID()
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeShellReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}

	shellReqMess := &protocol.ShellReq{
		Start:    1,
		ShellLen: uint64(len(shell)),
		Shell:    shell,
	}
	s.ShellReady.Store(taskID, make(chan string))
	defer func() {
		v, ok := s.ShellReady.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		s.ShellReady.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, shellReqMess, false)
	sMessage.SendMessage()
	channel, ok1 := s.ShellReady.Load(taskID)
	if !ok1 {
		return errors.New("not find shell ready channel")
	}
	select {
	case ok := <-channel.(chan string):
		if ok == "" {
			go s.handleShellPanelCommand(ctx, route, node, taskID, stdin, stdout, stderr)
			return nil
		} else {
			return errors.New("(agent error)call terminal failed,node shell is not ready:" + ok)
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("shell error:" + ChannelTimeOut.Error())
	}
}

func (s *shellManager) handleShellPanelCommand(ctx context.Context, route, node string, taskID string, stdin, stdout, stderr chan string) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeShellCmd,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	s.ShellResult.Store(taskID, make(chan string))
	s.ShellExit.Store(taskID, make(chan bool))
	defer func() {
		v, ok := s.ShellResult.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		s.ShellResult.Delete(taskID)
		v1, ok1 := s.ShellExit.Load(taskID)
		if ok1 {
			close(v1.(chan bool))
		}
		s.ShellExit.Delete(taskID)
		close(stdout)
		close(stderr)
	}()
	channel, ok := s.ShellResult.Load(taskID)
	if !ok {
		return
	}
	channel2, ok2 := s.ShellExit.Load(taskID)
	if !ok2 {
		return
	}
	for {
		select {
		case tCommand := <-stdin:
			shellCommandMess := &protocol.ShellCommand{
				CommandLen: uint64(len(tCommand)),
				Command:    tCommand,
			}
			protocol.ConstructMessage(sMessage, header, shellCommandMess, false)
			sMessage.SendMessage()
		case re := <-channel.(chan string):
			stdout <- re
		case <-channel2.(chan bool):
			stderr <- "EXIT"
			return
		case <-ctx.Done():
			s.closeTerminal(taskID, node, route)
			return
		}
	}
}

func (s *shellManager) closeTerminal(taskID string, node string, route string) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeShellExit,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	shellExit := &protocol.ShellExit{
		OK: 1,
	}
	protocol.ConstructMessage(sMessage, header, shellExit, false)
	sMessage.SendMessage()
}

func (s *shellManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-s.ShellMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						s.log.GeneralErrorf("shell handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.ShellRes:
					if channel, ok := s.ShellReady.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Err
					} else {
						s.log.GeneralErrorf("shell %v", ChannelClose)
					}
				case *protocol.ShellResult:
					if channel, ok := s.ShellResult.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Result
					} else {
						s.log.GeneralErrorf("shell result %v", ChannelClose)
					}
				case *protocol.ShellExit:
					if channel, ok := s.ShellExit.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan bool) <- true
					} else {
						s.log.GeneralErrorf("shell exit %v", ChannelClose)
					}
				}
			}(messageData)
		}
	}
}
