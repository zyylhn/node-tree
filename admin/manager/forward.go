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
	"sync"
	"time"
)

const (
	FWGetNewSeq              = iota //get new seq
	FWNewForWard                    //new Forward
	FWAddConn                       //add conn
	FWGetDataChan                   //get data chan
	FWGetDataChanWithoutUUID        //get data chan with out uuid
	FWGetForWardInfo                //get Forward info
	FWCloseTCP                      //close tcp
	FWCloseSingle                   //close single
	FWCloseSingleAll                //close single all
	FWForceShutdown                 //force shutdown
)

type forwardManager struct {
	forwardSeq    uint64                         //类似于uuid num
	forwardSeqMap map[uint64]*fwSeqRelationship  // map[seq](port+uuid)用来记录转发的每一条连接
	forwardMap    map[string]map[string]*Forward // map[uuid]map[port]*Forward's detail record Forward status 使用uuid作为键，存这个uuid哪个端口下面开的转发

	ForwardMessChan chan *protocol.CompletePacket //接收request
	ForwardReady    sync.Map

	TaskChan   chan *ForwardTask   //接收任务
	ResultChan chan *forwardResult //返回结果
	Done       chan bool           //保证数据完全写入之后在进行下一个任务，而不是拿到data chan就下一次任务

	log *public.NodeManagerLog
	t   *topology.Topology
}

type ForwardTask struct {
	Mode int
	UUID string // node uuid
	Seq  uint64 // seq

	Port        string
	RemoteAddr  string
	CloseTarget string
	Listener    net.Listener
}

type forwardResult struct {
	OK bool

	ForwardSeq  uint64
	DataChan    chan []byte
	ForwardInfo map[string]*Forward
}

type Forward struct {
	RemoteAddr string
	listener   net.Listener

	ForwardStatusMap map[uint64]*forwardStatus //记录当前这个转发服务所有转发的连接
}

// 用来对转发数据的channel
type forwardStatus struct {
	dataChan chan []byte
}

type fwSeqRelationship struct {
	uuid string
	port string
}

func newForwardManager(log *public.NodeManagerLog, t *topology.Topology) *forwardManager {
	m := new(forwardManager)

	m.forwardMap = make(map[string]map[string]*Forward)
	m.forwardSeqMap = make(map[uint64]*fwSeqRelationship)
	m.ForwardMessChan = make(chan *protocol.CompletePacket, 5)
	m.ForwardReady = sync.Map{}

	m.TaskChan = make(chan *ForwardTask)
	m.ResultChan = make(chan *forwardResult)
	m.Done = make(chan bool)
	m.t = t
	m.log = log

	return m
}

// Create  创建一个端口转发，将节点上的指定地址转发到admin的一个端口上
func (f *forwardManager) Create(lPort string, rAddr string, node string) error {
	route, ok := f.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	listenAddr := fmt.Sprintf("0.0.0.0:%s", lPort)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	//向子节点发送test请求，告诉子节点我们要将流量转发到他的哪个地址上
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	taskID := utils.GenerateUUID()
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeForwardTest,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	testMess := &protocol.ForwardTest{
		AddrLen: uint16(len([]byte(rAddr))),
		Addr:    rAddr,
	}
	f.ForwardReady.Store(taskID, make(chan string))
	defer func() {
		v, ok := f.ForwardReady.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		f.ForwardReady.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, testMess, false)
	sMessage.SendMessage()
	channel, ok := f.ForwardReady.Load(taskID)
	if !ok {
		return errors.New("not find forward ready channel")
	}
	//子节点会返回是否能开启转发（其实就是确定目标地址是否开启）
	select {
	case errStr := <-channel.(chan string):
		if errStr != "" {
			_ = listener.Close()
			err = fmt.Errorf("fail to forward port %s to remote addr %s:%v", lPort, rAddr, errStr)
			return err
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("forward error:" + ChannelTimeOut.Error())
	}

	//新建一个转发
	mgrTask := &ForwardTask{
		Mode:       FWNewForWard,
		UUID:       node,
		Listener:   listener,
		Port:       lPort,
		RemoteAddr: rAddr,
	}

	f.TaskChan <- mgrTask
	<-f.ResultChan
	go f.handleForwardListener(listener, node, route, lPort, rAddr)
	return nil
}

// GetInfoSingleNode  获取指定节点上的端口转发信息,返回map[lPort]转发的节点地址和连接数
func (f *forwardManager) GetInfoSingleNode(uuid string) map[string]*Forward {
	mgrTask := &ForwardTask{
		Mode: FWGetForWardInfo,
		UUID: uuid,
	}
	f.TaskChan <- mgrTask
	result := <-f.ResultChan

	return result.ForwardInfo
}

// StopWithChoice 停止指定节点上指定的端口转发，choice指定为admin本地的端口。
func (f *forwardManager) StopWithChoice(node string, choice string) {
	if choice == "all" {
		mgrTask := &ForwardTask{
			Mode: FWCloseSingleAll,
			UUID: node,
		}
		f.TaskChan <- mgrTask
		<-f.ResultChan
	} else {
		mgrTask := &ForwardTask{
			Mode:        FWCloseSingle,
			UUID:        node,
			CloseTarget: choice,
		}
		f.TaskChan <- mgrTask
		<-f.ResultChan
	}
}

func (f *forwardManager) ForceStopOnNode(node string) {
	forwardTask := &ForwardTask{
		Mode: FWForceShutdown,
		UUID: node,
	}
	f.TaskChan <- forwardTask
	<-f.ResultChan
}

func (f *forwardManager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-f.TaskChan:
			switch task.Mode {
			case FWNewForWard:
				f.newForward(task)
			case FWGetNewSeq:
				f.getNewSeq(task)
			case FWAddConn:
				f.addConn(task)
			case FWGetDataChan:
				f.getDataChan(task)
			case FWGetDataChanWithoutUUID:
				f.getDataChanWithoutUUID(task)
				<-f.Done
			case FWGetForWardInfo:
				f.getForwardInfo(task)
			case FWCloseTCP:
				f.closeTCP(task)
			case FWCloseSingle:
				f.closeSingle(task)
			case FWCloseSingleAll:
				f.closeSingleAll(task)
			case FWForceShutdown:
				f.forceShutdown(task)
			}
		}
	}
}

func (f *forwardManager) handleForwardListener(listener net.Listener, node string, route string, lPort, rAddr string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			_ = listener.Close() // todo:map没有释放,影响不大
			return
		}
		//接收到一个连接之后
		mgrTask := &ForwardTask{
			Mode: FWGetNewSeq,
			UUID: node,
			Port: lPort,
		}
		f.TaskChan <- mgrTask
		result := <-f.ResultChan
		//获取编号
		seq := result.ForwardSeq
		//初始化转发的channel
		mgrTask = &ForwardTask{
			Mode: FWAddConn,
			UUID: node,
			Seq:  seq,
			Port: lPort,
		}
		f.TaskChan <- mgrTask
		result = <-f.ResultChan
		if !result.OK {
			_ = conn.Close()
			continue
		}
		go f.handleForward(conn, node, route, lPort, rAddr, seq)
	}
}

func (f *forwardManager) handleForward(conn net.Conn, node string, route string, lPort, rAddr string, seq uint64) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	// 通知agent开始
	startHeader := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeForwardStart,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	startMess := &protocol.ForwardStart{
		Seq:     seq,
		AddrLen: uint16(len([]byte(rAddr))),
		Addr:    rAddr,
	}

	//先获取到data chan防止刚start data chan 就关闭
	mgrTask := &ForwardTask{
		Mode: FWGetDataChan,
		UUID: node,
		Seq:  seq,
		Port: lPort,
	}
	f.TaskChan <- mgrTask
	result := <-f.ResultChan

	//通知agent节点开始端口转发
	protocol.ConstructMessage(sMessage, startHeader, startMess, false)
	sMessage.SendMessage()
	//agent开启转发，会不断从与目标的连接中读取信息发回来并不断接收发过去的数据并写入到连接中
	defer func() { //通知结束当前连接转发
		finHeader := &protocol.Header{
			Sender:      protocol.AdminUuid,
			Acceptor:    node,
			MessageType: protocol.MessageTypeForwardFin,
			RouteLen:    uint32(len([]byte(route))),
			Route:       route,
		}

		finMess := &protocol.ForwardFin{
			Seq: seq,
		}

		protocol.ConstructMessage(sMessage, finHeader, finMess, false)
		sMessage.SendMessage()
	}()

	if !result.OK {
		return
	}

	dataChan := result.DataChan

	go func() {
		for { //不断从dataChan(其实就是从forwardStatus的接收数据的channel中读取信息写入到连接中)
			if data, ok := <-dataChan; ok {
				_, _ = conn.Write(data)
			} else {
				_ = conn.Close()
				return
			}
		}
	}()

	dataHeader := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeForwardData,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	buffer := make([]byte, 20480)

	for {
		//不断从连接中读取信息发送给子节点
		length, err := conn.Read(buffer)
		if err != nil {
			_ = conn.Close()
			return
		}

		forwardDataMess := &protocol.ForwardData{
			Seq:     seq,
			DataLen: uint64(length),
			Data:    buffer[:length],
		}

		protocol.ConstructMessage(sMessage, dataHeader, forwardDataMess, false)
		sMessage.SendMessage()
	}
}

// 在全局的存贮记录中根据uuid和监听的端口新建一个端口转发服务
func (f *forwardManager) newForward(task *ForwardTask) {
	if _, ok := f.forwardMap[task.UUID]; !ok { //判断此节点是否已经存在转发，不存在新建map
		//f.forwardMap = make(map[string]map[string]*Forward)
		f.forwardMap[task.UUID] = make(map[string]*Forward)
	}

	f.forwardMap[task.UUID][task.Port] = new(Forward) //根据uuid和端口新建一个转发服务
	f.forwardMap[task.UUID][task.Port].listener = task.Listener
	f.forwardMap[task.UUID][task.Port].RemoteAddr = task.RemoteAddr
	f.forwardMap[task.UUID][task.Port].ForwardStatusMap = make(map[uint64]*forwardStatus)

	f.ResultChan <- &forwardResult{OK: true}
}

// 新地连接需要被转发，在此获取一个id，并将此连接记录下来
func (f *forwardManager) getNewSeq(task *ForwardTask) {
	f.forwardSeqMap[f.forwardSeq] = &fwSeqRelationship{uuid: task.UUID, port: task.Port} //使用当前序号新建一个索引
	f.ResultChan <- &forwardResult{ForwardSeq: f.forwardSeq}                             //将序号返回
	f.forwardSeq++                                                                       //序号加一
}

// 新建一个连接，也就是转发的listener接收到一个请求需要转发
func (f *forwardManager) addConn(task *ForwardTask) {
	if _, ok := f.forwardSeqMap[task.Seq]; !ok { //根据编号查找是否存在端口转发记录
		f.ResultChan <- &forwardResult{OK: false}
		return
	}
	//存在的话
	f.forwardMap[task.UUID][task.Port].ForwardStatusMap[task.Seq] = new(forwardStatus)
	f.forwardMap[task.UUID][task.Port].ForwardStatusMap[task.Seq].dataChan = make(chan []byte, 5)
	f.ResultChan <- &forwardResult{OK: true}
}

// 获取指定转发器转发连接的datachannel
func (f *forwardManager) getDataChan(task *ForwardTask) {
	if _, ok := f.forwardSeqMap[task.Seq]; !ok {
		f.ResultChan <- &forwardResult{OK: false}
		return
	}

	if _, ok := f.forwardMap[task.UUID][task.Port].ForwardStatusMap[task.Seq]; ok { // need to check ,because you will never know when fin come
		f.ResultChan <- &forwardResult{
			OK:       true,
			DataChan: f.forwardMap[task.UUID][task.Port].ForwardStatusMap[task.Seq].dataChan,
		}
	} else {
		f.ResultChan <- &forwardResult{OK: false}
	}
}

// 从索引中获取datachannel相关信息去获取datachannel
func (f *forwardManager) getDataChanWithoutUUID(task *ForwardTask) {
	if _, ok := f.forwardSeqMap[task.Seq]; !ok {
		f.ResultChan <- &forwardResult{OK: false}
		return
	}

	uuid := f.forwardSeqMap[task.Seq].uuid
	port := f.forwardSeqMap[task.Seq].port

	f.ResultChan <- &forwardResult{ // no need to check ForwardStatusMap[task.Seq] like above,because no more data after fin
		OK:       true,
		DataChan: f.forwardMap[uuid][port].ForwardStatusMap[task.Seq].dataChan,
	}
}

func (f *forwardManager) getForwardInfo(task *ForwardTask) {

	if re, ok := f.forwardMap[task.UUID]; ok {
		f.ResultChan <- &forwardResult{
			OK:          true,
			ForwardInfo: re,
		}
	} else {
		f.ResultChan <- &forwardResult{
			OK:          false,
			ForwardInfo: nil,
		}
	}
}

func (f *forwardManager) closeTCP(task *ForwardTask) {
	if _, ok := f.forwardSeqMap[task.Seq]; !ok {
		return
	}

	uuid := f.forwardSeqMap[task.Seq].uuid
	port := f.forwardSeqMap[task.Seq].port

	close(f.forwardMap[uuid][port].ForwardStatusMap[task.Seq].dataChan)

	delete(f.forwardMap[uuid][port].ForwardStatusMap, task.Seq)
}

func (f *forwardManager) closeSingle(task *ForwardTask) {
	if _, ok := f.forwardMap[task.UUID]; !ok {
		f.ResultChan <- &forwardResult{OK: true}
		return
	}
	port := task.CloseTarget //在这获取选定索引对应的转发端口
	//关闭转发器的listener
	_ = f.forwardMap[task.UUID][port].listener.Close()
	// 关闭当前转发器相关的连接的data channel并删除
	for seq, status := range f.forwardMap[task.UUID][port].ForwardStatusMap {
		close(status.dataChan)
		delete(f.forwardMap[task.UUID][port].ForwardStatusMap, seq)
	}
	// 删除当前节点的转发器
	delete(f.forwardMap[task.UUID], port)
	//删除索引列表中的记录
	for seq, relationship := range f.forwardSeqMap {
		if relationship.uuid == task.UUID && relationship.port == port {
			delete(f.forwardSeqMap, seq)
		}
	}
	// if no other Forward services running on current node,delete node from f.forwardMap
	if len(f.forwardMap[task.UUID]) == 0 {
		delete(f.forwardMap, task.UUID)
	}

	f.ResultChan <- &forwardResult{OK: true}
}

func (f *forwardManager) closeSingleAll(task *ForwardTask) {
	for port, forward := range f.forwardMap[task.UUID] {
		_ = forward.listener.Close()

		for seq, status := range forward.ForwardStatusMap {
			close(status.dataChan)
			delete(forward.ForwardStatusMap, seq)
		}

		delete(f.forwardMap[task.UUID], port)
	}

	for seq, relationship := range f.forwardSeqMap {
		if relationship.uuid == task.UUID {
			delete(f.forwardSeqMap, seq)
		}
	}

	delete(f.forwardMap, task.UUID)

	f.ResultChan <- &forwardResult{OK: true}
}

func (f *forwardManager) forceShutdown(task *ForwardTask) {
	if _, ok := f.forwardMap[task.UUID]; ok {
		f.closeSingleAll(task)
	} else {
		f.ResultChan <- &forwardResult{OK: true}
	}
}

func (f *forwardManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case message := <-f.ForwardMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						f.log.GeneralErrorf("forward handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.ForwardReady:
					if channel, ok := f.ForwardReady.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Err
					} else {
						f.log.GeneralErrorf("forward response return but channel alreday stop")
					}
				case *protocol.ForwardData:
					mgrTask := &ForwardTask{
						Mode: FWGetDataChanWithoutUUID,
						Seq:  mess.Seq,
					}
					f.TaskChan <- mgrTask
					result := <-f.ResultChan
					if result.OK {
						result.DataChan <- mess.Data
					}
					f.Done <- true
				case *protocol.ForwardFin:
					mgrTask := &ForwardTask{
						Mode: FWCloseTCP,
						Seq:  mess.Seq,
					}
					f.TaskChan <- mgrTask
				}
			}(message)
		}
	}
}
