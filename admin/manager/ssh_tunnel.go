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

type SSHTunnel struct {
	Port string
	SSH
}

type sshTunnelManager struct {
	SSHTunnelMessChan chan *protocol.CompletePacket
	SshTunnelReady    sync.Map

	log *public.NodeManagerLog
	t   *topology.Topology
}

func newSSHTunnelManager(log *public.NodeManagerLog, t *topology.Topology) *sshTunnelManager {
	re := new(sshTunnelManager)
	re.SSHTunnelMessChan = make(chan *protocol.CompletePacket, 5)
	re.SshTunnelReady = sync.Map{}
	re.log = log
	re.t = t

	return re
}

func (s *sshTunnelManager) LetSSHTunnel(node string, sshTunnel *SSHTunnel) error {
	_, _, err := utils.CheckIPPort(sshTunnel.Addr)
	if err != nil {
		return err
	}

	if sshTunnel.Port == "" {
		return fmt.Errorf("you must set connect port")
	}

	if sshTunnel.Method == CERMethod && sshTunnel.Certificate == nil {
		if err = sshTunnel.getCertificate(); err != nil {
			return err
		}
	}

	if sshTunnel.Method == UPMethod && sshTunnel.Password == "" {
		return fmt.Errorf("you must set password")
	}

	if sshTunnel.Username == "" {
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
		MessageType: protocol.MessageTypeSshTunnelReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}

	sshTunnelReqMess := &protocol.SSHTunnelReq{
		Method:         uint16(sshTunnel.Method),
		AddrLen:        uint16(len(sshTunnel.Addr)),
		Addr:           sshTunnel.Addr,
		PortLen:        uint16(len(sshTunnel.Port)),
		Port:           sshTunnel.Port,
		UsernameLen:    uint64(len(sshTunnel.Username)),
		Username:       sshTunnel.Username,
		PasswordLen:    uint64(len(sshTunnel.Password)),
		Password:       sshTunnel.Password,
		CertificateLen: uint64(len(sshTunnel.Certificate)),
		Certificate:    sshTunnel.Certificate,
	}
	s.SshTunnelReady.Store(taskID, make(chan string))
	defer func() {
		v, ok := s.SshTunnelReady.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		s.SshTunnelReady.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, sshTunnelReqMess, false)
	sMessage.SendMessage()
	channel, ok := s.SshTunnelReady.Load(taskID)
	if !ok {
		return errors.New("not find ssh tunnel ready channel")
	}
	select {
	case errStr := <-channel.(chan string):
		if errStr == "" {
			return nil
		} else {
			return errors.New("(agent error)fail to initial to target node by ssh tunnel,node is not ready:" + errStr)
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("ssh tunnel error:" + ChannelTimeOut.Error())
	}
}

func (s *sshTunnelManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-s.SSHTunnelMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						s.log.GeneralErrorf("sshTunnel handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.SSHTunnelRes:
					if channel, ok := s.SshTunnelReady.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Err
					} else {
						s.log.GeneralErrorf("sshtunnel %v", ChannelClose)
					}
				}
			}(messageData)
		}
	}
}
