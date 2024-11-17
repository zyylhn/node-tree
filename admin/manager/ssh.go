package manager

import (
	"context"
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"net"
	"strings"
	"sync"
	"time"
)

type sshManager struct {
	SSHMessChan chan *protocol.CompletePacket
	SshReady    sync.Map
	SshExit     sync.Map
	SshResult   sync.Map

	log *public.NodeManagerLog
	t   *topology.Topology
}

func newSSHManager(log *public.NodeManagerLog, t *topology.Topology) *sshManager {
	re := new(sshManager)
	re.SSHMessChan = make(chan *protocol.CompletePacket, 5)
	re.SshReady = sync.Map{}
	re.SshExit = sync.Map{}
	re.SshResult = sync.Map{}
	re.log = log
	re.t = t
	return re
}

func (s *sshManager) LetSSH(ctx context.Context, node string, stdin, stdout, stderr chan string, ssh *SSH) error {
	if !strings.Contains(ssh.Addr, ":") {
		ssh.Addr = net.JoinHostPort(ssh.Addr, "22")
	}
	_, _, err := utils.CheckIPPort(ssh.Addr)
	if err != nil {
		return err
	}

	if ssh.Method == CERMethod && ssh.Certificate == nil {
		if err = ssh.getCertificate(); err != nil {
			return err
		}
	}
	if ssh.Method == UPMethod && ssh.Password == "" {
		return fmt.Errorf("you must set password")
	}

	if ssh.Username == "" {
		return fmt.Errorf("you must set username")
	}
	route, ok := s.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}

	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	taskID := utils.GenerateUUID()
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeSshReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}

	sshReqMess := &protocol.SSHReq{
		Method:         uint16(ssh.Method),
		AddrLen:        uint16(len(ssh.Addr)),
		Addr:           ssh.Addr,
		UsernameLen:    uint64(len(ssh.Username)),
		Username:       ssh.Username,
		PasswordLen:    uint64(len(ssh.Password)),
		Password:       ssh.Password,
		CertificateLen: uint64(len(ssh.Certificate)),
		Certificate:    ssh.Certificate,
	}
	s.SshReady.Store(taskID, make(chan string))
	defer func() {
		v, ok := s.SshReady.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		s.SshReady.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, sshReqMess, false)
	sMessage.SendMessage()
	channel, ok := s.SshReady.Load(taskID)
	if !ok {
		return errors.New("not find ssh ready channel")
	}
	select {
	case errStr := <-channel.(chan string):
		if errStr == "" {
			go s.handleSSHPanelCommand(ctx, route, node, taskID, stdin, stdout, stderr)
			return nil
		} else {
			return errors.New("(agent error)fail to initial to target host,ssh is not ready:" + errStr)
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("ssh error:" + ChannelTimeOut.Error())
	}

}

func (s *sshManager) handleSSHPanelCommand(ctx context.Context, route string, uuid, taskID string, stdin, stdout, stderr chan string) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeSshCmd,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}

	s.SshResult.Store(taskID, make(chan string))
	s.SshExit.Store(taskID, make(chan bool))
	defer func() {
		v, ok := s.SshResult.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		s.SshResult.Delete(taskID)
		v1, ok1 := s.SshExit.Load(taskID)
		if ok1 {
			close(v1.(chan bool))
		}
		s.SshExit.Delete(taskID)
		close(stdout)
		close(stderr)
	}()
	channel, ok := s.SshResult.Load(taskID)
	if !ok {
		return
	}
	channel2, ok2 := s.SshExit.Load(taskID)
	if !ok2 {
		return
	}
	for {
		select {
		case tCommand := <-stdin:
			sshCommandMess := &protocol.SSHCommand{
				CommandLen: uint64(len(tCommand)),
				Command:    tCommand,
			}
			protocol.ConstructMessage(sMessage, header, sshCommandMess, false)
			sMessage.SendMessage()
		case re := <-channel.(chan string):
			stdout <- re
		case <-channel2.(chan bool):
			stderr <- "EXIT"
			return
		case <-ctx.Done():
			s.closeSshTerminal(taskID, uuid, route)
			return
		}
	}
}

func (s *sshManager) closeSshTerminal(taskId, uuid, route string) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeSshExit,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskId)),
		TaskID:      taskId,
	}
	sshExit := &protocol.SSHExit{
		OK: 1,
	}
	protocol.ConstructMessage(sMessage, header, sshExit, false)
	sMessage.SendMessage()
}

func (s *sshManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-s.SSHMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						s.log.GeneralErrorf("ssh handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.SSHRes:
					if channel, ok := s.SshReady.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Err
					} else {
						s.log.GeneralErrorf("ssh %v", ChannelClose)
					}
				case *protocol.SSHResult:
					if channel, ok := s.SshResult.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Result
					} else {
						s.log.GeneralErrorf("ssh result %v", ChannelClose)
					}
				case *protocol.SSHExit:
					if channel, ok := s.SshExit.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan bool) <- true
					} else {
						s.log.GeneralErrorf("ssh exit %v", ChannelClose)
					}
				}
			}(messageData)
		}
	}
}
