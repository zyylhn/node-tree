package manager

import (
	"context"
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"net"
	"time"
)

const (
	SockNewSocks = iota
	SockAddTCPSocket
	SockGetNewSwq
	SockGetTcpDataChan
	SockGetUDPDataChan
	SockGetTCPDataChanWithoutUUID
	SockGetUDPDataChanWithoutUUID
	SockCloseTCP
	SockGetUDPStartInfo
	SockUpdateUDP
	SockGetSocksInfo
	SockCloseSocks
	SockForceShutdown
)

type socksManager struct {
	socksSeq    uint64
	socksSeqMap map[uint64]string // map[seq]uuid  just for accelerate the speed of searching detail only by seq
	socksMap    map[string]*socks // map[uuid]sock's detail

	SocksMessChan chan *protocol.CompletePacket
	SocksReady    map[string]chan string //todo 换成sync.map

	TaskChan   chan *SocksTask
	ResultChan chan *socksResult
	Done       chan bool

	log *public.NodeManagerLog
	t   *topology.Topology
}

type SocksTask struct {
	Mode int
	UUID string // node uuid
	Seq  uint64 // seq

	SocksPort        string
	SocksUsername    string
	SocksPassword    string
	SocksTCPListener net.Listener
	SocksTCPSocket   net.Conn
	SocksUDPListener *net.UDPConn
}

type socksResult struct {
	OK   bool
	UUID string

	SocksSeq    uint64
	TCPAddr     string
	SocksInfo   SocksInfo
	TCPDataChan chan []byte
	UDPDataChan chan []byte
}

type socks struct {
	SocksInfo
	listener net.Listener

	socksStatusMap map[uint64]*socksStatus
}

type SocksInfo struct {
	Port     string `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type socksStatus struct {
	isUDP bool
	tcp   *tcpSocks
	udp   *udpSocks
}

type tcpSocks struct {
	dataChan chan []byte
	conn     net.Conn
}

type udpSocks struct {
	dataChan chan []byte
	listener *net.UDPConn
}

func newSocksManager(log *public.NodeManagerLog, t *topology.Topology) *socksManager {
	re := new(socksManager)

	re.socksMap = make(map[string]*socks)
	re.socksSeqMap = make(map[uint64]string)
	re.SocksMessChan = make(chan *protocol.CompletePacket, 5)
	re.SocksReady = make(map[string]chan string)

	re.TaskChan = make(chan *SocksTask)
	re.ResultChan = make(chan *socksResult)
	re.Done = make(chan bool)
	re.log = log
	re.t = t

	return re
}

func (s *socksManager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.TaskChan:
			switch task.Mode {
			case SockNewSocks:
				s.newSocks(task)
			case SockAddTCPSocket:
				s.addSocksTCPSocket(task)
			case SockGetNewSwq:
				s.getSocksSeq(task)
			case SockGetTcpDataChan:
				s.getTCPDataChan(task)
			case SockGetUDPDataChan:
				s.getUDPDataChan(task)
			case SockGetTCPDataChanWithoutUUID:
				s.getTCPDataChanWithoutUUID(task)
				<-s.Done
			case SockGetUDPDataChanWithoutUUID:
				s.getUDPDataChanWithoutUUID(task)
				<-s.Done
			case SockCloseTCP:
				s.closeTCP(task)
			case SockGetUDPStartInfo:
				s.getUDPStartInfo(task)
			case SockUpdateUDP:
				s.updateUDP(task)
			case SockGetSocksInfo:
				s.getSocksInfo(task)
			case SockCloseSocks:
				s.closeSocks(task)
			case SockForceShutdown:
				s.forceShutdown(task)
			}
		}
	}
}

func (s *socksManager) newSocks(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; !ok {
		s.socksMap[task.UUID] = new(socks)
		s.socksMap[task.UUID].Port = task.SocksPort
		s.socksMap[task.UUID].Username = task.SocksUsername
		s.socksMap[task.UUID].Password = task.SocksPassword
		s.socksMap[task.UUID].socksStatusMap = make(map[uint64]*socksStatus)
		s.socksMap[task.UUID].listener = task.SocksTCPListener
		s.ResultChan <- &socksResult{OK: true}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

func (s *socksManager) addSocksTCPSocket(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; ok {
		s.socksMap[task.UUID].socksStatusMap[task.Seq] = new(socksStatus)
		s.socksMap[task.UUID].socksStatusMap[task.Seq].tcp = new(tcpSocks) // no need to check if socksStatusMap[task.Seq] exist,because it must exist
		s.socksMap[task.UUID].socksStatusMap[task.Seq].tcp.dataChan = make(chan []byte, 5)
		s.socksMap[task.UUID].socksStatusMap[task.Seq].tcp.conn = task.SocksTCPSocket
		s.ResultChan <- &socksResult{OK: true}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

func (s *socksManager) getSocksSeq(task *SocksTask) {
	// Use seq map to record the UUIDNum <-> Seq relationship to make search quicker
	s.socksSeqMap[s.socksSeq] = task.UUID
	s.ResultChan <- &socksResult{SocksSeq: s.socksSeq}
	s.socksSeq++
}

func (s *socksManager) getTCPDataChan(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; ok {
		s.ResultChan <- &socksResult{
			OK:          true,
			TCPDataChan: s.socksMap[task.UUID].socksStatusMap[task.Seq].tcp.dataChan,
		}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

func (s *socksManager) getUDPDataChan(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; ok {
		if _, ok := s.socksMap[task.UUID].socksStatusMap[task.Seq]; ok {
			s.ResultChan <- &socksResult{
				OK:          true,
				UDPDataChan: s.socksMap[task.UUID].socksStatusMap[task.Seq].udp.dataChan,
			}
		} else {
			s.ResultChan <- &socksResult{OK: false}
		}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

func (s *socksManager) getTCPDataChanWithoutUUID(task *SocksTask) {
	if _, ok := s.socksSeqMap[task.Seq]; !ok {
		s.ResultChan <- &socksResult{OK: false}
		return
	}

	uuid := s.socksSeqMap[task.Seq]
	// if "s.socksSeqMap[task.Seq]" exist, "s.socksMap[uuid]" must exist too
	if _, ok := s.socksMap[uuid].socksStatusMap[task.Seq]; ok {
		s.ResultChan <- &socksResult{
			OK:          true,
			TCPDataChan: s.socksMap[uuid].socksStatusMap[task.Seq].tcp.dataChan,
		}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

func (s *socksManager) getUDPDataChanWithoutUUID(task *SocksTask) {
	if _, ok := s.socksSeqMap[task.Seq]; !ok {
		s.ResultChan <- &socksResult{OK: false}
		return
	}

	uuid := s.socksSeqMap[task.Seq]
	// s.socksMap[uuid] must exist if s.socksSeqMap[task.Seq] exist
	if _, ok := s.socksMap[uuid].socksStatusMap[task.Seq]; ok {
		s.ResultChan <- &socksResult{
			OK:          true,
			UDPDataChan: s.socksMap[uuid].socksStatusMap[task.Seq].udp.dataChan,
		}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

// close TCP include close UDP,cuz UDP control channel is TCP,if TCP broken,UDP is also forced to be shut down
func (s *socksManager) closeTCP(task *SocksTask) {
	if _, ok := s.socksSeqMap[task.Seq]; !ok {
		return
	}

	uuid := s.socksSeqMap[task.Seq]

	close(s.socksMap[uuid].socksStatusMap[task.Seq].tcp.dataChan)

	if s.socksMap[uuid].socksStatusMap[task.Seq].isUDP {
		close(s.socksMap[uuid].socksStatusMap[task.Seq].udp.dataChan)
	}

	delete(s.socksMap[uuid].socksStatusMap, task.Seq)
}

func (s *socksManager) getUDPStartInfo(task *SocksTask) {
	if _, ok := s.socksSeqMap[task.Seq]; !ok {
		s.ResultChan <- &socksResult{OK: false}
		return
	}

	uuid := s.socksSeqMap[task.Seq]

	if _, ok := s.socksMap[uuid].socksStatusMap[task.Seq]; ok {
		s.ResultChan <- &socksResult{
			OK:      true,
			TCPAddr: s.socksMap[uuid].socksStatusMap[task.Seq].tcp.conn.LocalAddr().(*net.TCPAddr).IP.String(),
			UUID:    uuid,
		}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

func (s *socksManager) updateUDP(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; ok {
		if _, ok := s.socksMap[task.UUID].socksStatusMap[task.Seq]; ok {
			s.socksMap[task.UUID].socksStatusMap[task.Seq].isUDP = true
			s.socksMap[task.UUID].socksStatusMap[task.Seq].udp = new(udpSocks)
			s.socksMap[task.UUID].socksStatusMap[task.Seq].udp.dataChan = make(chan []byte, 5)
			s.socksMap[task.UUID].socksStatusMap[task.Seq].udp.listener = task.SocksUDPListener
			s.ResultChan <- &socksResult{OK: true}
		} else {
			s.ResultChan <- &socksResult{OK: false}
		}
	} else {
		s.ResultChan <- &socksResult{OK: false}
	}
}

func (s *socksManager) getSocksInfo(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; ok {
		s.ResultChan <- &socksResult{
			OK:        true,
			SocksInfo: SocksInfo{Port: s.socksMap[task.UUID].Port, Username: s.socksMap[task.UUID].Username, Password: s.socksMap[task.UUID].Password},
		}
	} else {
		s.ResultChan <- &socksResult{
			OK:        false,
			SocksInfo: SocksInfo{},
		}
	}
}

func (s *socksManager) closeSocks(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; !ok {
		s.ResultChan <- &socksResult{OK: true}
		return
	}
	_ = s.socksMap[task.UUID].listener.Close()
	for seq, status := range s.socksMap[task.UUID].socksStatusMap {
		// bugfix: In order to avoid data loss,so not close conn&listener here.Thx to @lz520520
		close(status.tcp.dataChan)
		if status.isUDP {
			close(status.udp.dataChan)
		}
		delete(s.socksMap[task.UUID].socksStatusMap, seq)
	}

	for seq, uuid := range s.socksSeqMap {
		if uuid == task.UUID {
			delete(s.socksSeqMap, seq)
		}
	}

	delete(s.socksMap, task.UUID) // we delete corresponding "socksMap"
	s.ResultChan <- &socksResult{OK: true}
}

func (s *socksManager) forceShutdown(task *SocksTask) {
	if _, ok := s.socksMap[task.UUID]; ok {
		s.closeSocks(task)
	} else {
		s.ResultChan <- &socksResult{OK: true}
	}
}

func (s *socksManager) OpenSocks(node string, port string, user, pass string) error {
	route, ok := s.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	addr := fmt.Sprintf("0.0.0.0:%s", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// register brand-new socks service
	mgrTask := &SocksTask{
		Mode:             SockNewSocks,
		UUID:             node,
		SocksPort:        port,
		SocksUsername:    user,
		SocksPassword:    pass,
		SocksTCPListener: listener,
	}

	s.TaskChan <- mgrTask
	result := <-s.ResultChan // wait for "add" done
	if !result.OK {          // node and socks service must be one-to-one
		err = errors.New("socks has already running on current node! Use 'stop socks' to stop the old one")
		_ = listener.Close()
		return err
	}

	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeSocksStart,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	socksStartMess := &protocol.SocksStart{
		UsernameLen: uint64(len([]byte(user))),
		Username:    user,
		PasswordLen: uint64(len([]byte(pass))),
		Password:    pass,
	}
	s.SocksReady[node] = make(chan string)
	defer func() {
		close(s.SocksReady[node])
		delete(s.SocksReady, node)
	}()
	protocol.ConstructMessage(sMessage, header, socksStartMess, false)
	sMessage.SendMessage()

	select {
	case errStr := <-s.SocksReady[node]:
		if errStr == "" {
			go s.handleSocksListener(listener, route, node)
			return nil
		} else {
			err = errors.New("fail to start socks:" + errStr)
			s.StopSocks(node)
			return err
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("socks error:" + ChannelTimeOut.Error())
	}
}

// GetSocksInfo bool值返回是否存在socks服务器
func (s *socksManager) GetSocksInfo(node string) (bool, SocksInfo) {
	mgrTask := &SocksTask{
		Mode: SockGetSocksInfo,
		UUID: node,
	}
	s.TaskChan <- mgrTask
	result := <-s.ResultChan

	return result.OK, result.SocksInfo
}

func (s *socksManager) StopSocks(node string) {
	mgrTask := &SocksTask{
		Mode: SockCloseSocks,
		UUID: node,
	}
	s.TaskChan <- mgrTask
	<-s.ResultChan
}

func (s *socksManager) ForceStopOnNode(node string) {
	socksTask := &SocksTask{
		Mode: SockForceShutdown,
		UUID: node,
	}
	//强制停止socks
	s.TaskChan <- socksTask
	<-s.ResultChan
}

func (s *socksManager) handleSocksListener(listener net.Listener, route string, uuid string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			_ = listener.Close()
			return
		}

		// ask new seq num
		mgrTask := &SocksTask{
			Mode: SockGetNewSwq,
			UUID: uuid,
		}
		s.TaskChan <- mgrTask
		result := <-s.ResultChan
		seq := result.SocksSeq

		// save the socket 初始化tcp的datachannel
		mgrTask = &SocksTask{
			Mode:           SockAddTCPSocket,
			UUID:           uuid,
			Seq:            seq,
			SocksTCPSocket: conn,
		}
		s.TaskChan <- mgrTask
		result = <-s.ResultChan
		if !result.OK {
			_ = conn.Close()
			return
		}

		// handle it! 开始转发流量
		go s.handleSocks(conn, route, uuid, seq)
	}
}

func (s *socksManager) handleSocks(conn net.Conn, route string, uuid string, seq uint64) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeSocksTcpData,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	mgrTask := &SocksTask{
		Mode: SockGetTcpDataChan,
		UUID: uuid,
		Seq:  seq,
	}
	s.TaskChan <- mgrTask
	result := <-s.ResultChan
	if !result.OK {
		return
	}

	tcpDataChan := result.TCPDataChan

	go func() {
		for {
			if data, ok := <-tcpDataChan; ok {
				_, _ = conn.Write(data)
			} else {
				_ = conn.Close()
				return
			}
		}
	}()

	var sendSth bool

	// SendTCPFin after browser close the conn
	defer func() {
		// check if "sendSth" is true
		// if true, then tell agent that the-conn is closed
		// but keep "handle received data" working to achieve socks data from agent that still on the way
		// if false, don't tell agent and do cleanup alone
		if !sendSth {
			// call HandleTCPFin by myself
			mgrTask = &SocksTask{
				Mode: SockCloseTCP,
				Seq:  seq,
			}
			s.TaskChan <- mgrTask
			return
		}

		finHeader := &protocol.Header{
			Sender:      protocol.AdminUuid,
			Acceptor:    uuid,
			MessageType: protocol.MessageTypeSocksTcpFin,
			RouteLen:    uint32(len([]byte(route))),
			Route:       route,
		}
		finMess := &protocol.SocksTCPFin{
			Seq: seq,
		}

		protocol.ConstructMessage(sMessage, finHeader, finMess, false)
		sMessage.SendMessage()
	}()

	// handling data that comes from browser
	buffer := make([]byte, 20480)

	// try to receive first packet
	// avoid browser to close the conn but sends nothing
	length, err := conn.Read(buffer)
	if err != nil {
		_ = conn.Close() // close conn immediately
		return
	}

	socksDataMess := &protocol.SocksTCPData{
		Seq:     seq,
		DataLen: uint64(length),
		Data:    buffer[:length],
	}

	protocol.ConstructMessage(sMessage, header, socksDataMess, false)
	sMessage.SendMessage()

	// browser sends sth, so handling conn normally and setting sendSth->true
	for {
		length, err = conn.Read(buffer)
		if err != nil {
			sendSth = true
			_ = conn.Close() // close conn immediately,in case of sth is sent after TCPFin
			return
		}

		socksDataMess = &protocol.SocksTCPData{
			Seq:     seq,
			DataLen: uint64(length),
			Data:    buffer[:length],
		}

		protocol.ConstructMessage(sMessage, header, socksDataMess, false)
		sMessage.SendMessage()
	}
}

func (s *socksManager) startUDPAss(seq uint64) {
	var (
		err             error
		udpListenerAddr *net.UDPAddr
		udpListener     *net.UDPConn
	)

	mgrTask := &SocksTask{
		Mode: SockGetUDPStartInfo,
		Seq:  seq,
	}
	s.TaskChan <- mgrTask
	re := <-s.ResultChan
	uuid := re.UUID

	route, ok := s.t.GetRoute(uuid)
	if !ok {
		s.log.GeneralError(newNodeNotFoundError(uuid))
		return
	}

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeUdpAssRes,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	failMess := &protocol.UDPAssRes{
		Seq: seq,
		OK:  0,
	}

	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	defer func() {
		if err != nil {
			protocol.ConstructMessage(sMessage, header, failMess, false)
			sMessage.SendMessage()
		}
	}()

	if re.OK {
		udpListenerAddr, err = net.ResolveUDPAddr("udp", re.TCPAddr+":0")
		if err != nil {
			return
		}

		udpListener, err = net.ListenUDP("udp", udpListenerAddr)
		if err != nil {
			return
		}

		mgrTask = &SocksTask{
			Mode:             SockUpdateUDP,
			Seq:              seq,
			UUID:             uuid,
			SocksUDPListener: udpListener,
		}
		s.TaskChan <- mgrTask
		re = <-s.ResultChan
		if !re.OK {
			err = errors.New("TCP conn seems disconnected")
			return
		}

		go s.handleUDPAss(udpListener, route, uuid, seq)

		successMess := &protocol.UDPAssRes{
			Seq:     seq,
			OK:      1,
			AddrLen: uint16(len(udpListener.LocalAddr().String())),
			Addr:    udpListener.LocalAddr().String(),
		}

		protocol.ConstructMessage(sMessage, header, successMess, false)
		sMessage.SendMessage()
	} else {
		err = errors.New("TCP conn seems disconnected")
		return
	}
}

func (s *socksManager) handleUDPAss(listener *net.UDPConn, route string, node string, seq uint64) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	dataHeader := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeSocksUdpData,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	mgrTask := &SocksTask{
		Mode: SockGetUDPDataChan,
		UUID: node,
		Seq:  seq,
	}
	s.TaskChan <- mgrTask
	result := <-s.ResultChan

	if !result.OK {
		return
	}

	udpDataChan := result.UDPDataChan

	buffer := make([]byte, 20480)

	var alreadyGetAddr bool
	for {
		length, addr, err := listener.ReadFromUDP(buffer)
		if !alreadyGetAddr {
			go func() {
				for {
					if data, ok := <-udpDataChan; ok {
						_, _ = listener.WriteToUDP(data, addr)
					} else {
						_ = listener.Close()
						return
					}
				}
			}()
			alreadyGetAddr = true
		}

		if err != nil {
			_ = listener.Close()
			return
		}

		udpDataMess := &protocol.SocksUDPData{
			Seq:     seq,
			DataLen: uint64(length),
			Data:    buffer[:length],
		}

		protocol.ConstructMessage(sMessage, dataHeader, udpDataMess, false)
		sMessage.SendMessage()
	}
}

func (s *socksManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-s.SocksMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						s.log.GeneralErrorf("socks handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.SocksReady:
					if channel, ok := s.SocksReady[packet.ReqHeader.Sender]; ok {
						channel <- mess.Err
					} else {
						s.log.GeneralErrorf("socks %v", ChannelClose)
					}
				case *protocol.SocksTCPData:
					mgrTask := &SocksTask{
						Mode: SockGetTCPDataChanWithoutUUID,
						Seq:  mess.Seq,
					}
					s.TaskChan <- mgrTask
					result := <-s.ResultChan
					if result.OK {
						result.TCPDataChan <- mess.Data
					}

					s.Done <- true
				case *protocol.SocksTCPFin:
					mgrTask := &SocksTask{
						Mode: SockCloseTCP,
						Seq:  mess.Seq,
					}
					s.TaskChan <- mgrTask
				case *protocol.UDPAssStart:
					go s.startUDPAss(mess.Seq)
				case *protocol.SocksUDPData:
					mgrTask := &SocksTask{
						Mode: SockGetUDPDataChanWithoutUUID,
						Seq:  mess.Seq,
					}
					s.TaskChan <- mgrTask
					result := <-s.ResultChan
					if result.OK {
						result.UDPDataChan <- mess.Data
					}

					s.Done <- true
				}
			}(messageData)
		}
	}
}
