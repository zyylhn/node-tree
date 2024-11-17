package manager

import (
	"context"
	"errors"
	"fmt"
	"github.com/zyylhn/bufferChannel"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"net"
	"sync"
	"time"
)

const (
	BWNewBackWard = iota
	BWGetNewSEQ
	BWAddConn
	BWCheckBackWard
	BWGetDataChan
	BWGetDataChanWithoutUUID
	BWCloseTCP
	BWGetBackWardInfo
	BWCloseSingle
	BWCloseAll
	BWForceShutdown
)

type backwardManager struct {
	log            *public.NodeManagerLog
	backwardSeq    uint64
	backwardSeqMap map[uint64]*bwSeqRelationship   // map[seq](port+uuid) just for accelerate the speed of searching detail only by seq
	backwardMap    map[string]map[string]*Backward // map[uuid][remote_port]Backward status

	BackwardMessChan chan *protocol.CompletePacket
	BackwardReady    sync.Map

	TaskChan   chan *BackwardTask
	ResultChan chan *backwardResult

	Done chan bool
	t    *topology.Topology
}

type BackwardTask struct {
	Mode int
	UUID string // node uuid
	Seq  uint64 // seq

	LPort  string
	RPort  string
	Choice int
}

type backwardResult struct {
	OK bool

	DataChan     chan []byte
	BackwardSeq  uint64
	BackwardInfo map[string]*Backward
	RPort        string
}

type Backward struct {
	LocalAddr string

	BackwardStatusMap map[uint64]*backwardStatus
}

type backwardStatus struct {
	dataChan chan []byte
}

type bwSeqRelationship struct {
	uuid  string
	rPort string
}

func newBackwardManager(logInfo *public.NodeManagerLog, t *topology.Topology) *backwardManager {
	manager := new(backwardManager)
	manager.log = logInfo
	manager.backwardMap = make(map[string]map[string]*Backward)
	manager.backwardSeqMap = make(map[uint64]*bwSeqRelationship)
	manager.BackwardMessChan = make(chan *protocol.CompletePacket, 50)
	manager.BackwardReady = sync.Map{}
	manager.TaskChan = make(chan *BackwardTask)
	manager.ResultChan = make(chan *backwardResult)
	manager.Done = make(chan bool)
	manager.t = t

	return manager
}

// Create 在指定节点上打开一个反向端口转发服务。将admin可访问的lAddr，转发到agent的0.0.0.0:rPort上
func (b *backwardManager) Create(lAddr string, rPort string, node string) error {
	localAddr, err := utils.GetBackWardAddr(lAddr) //todo 完善check规则,并检查rPort，rPort其实也可以填写一个地址。
	if err != nil {
		return err
	}
	route, ok := b.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	taskID := utils.GenerateUUID()
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeBackwardTest,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
		TaskIDLen:   uint16(len(taskID)),
		TaskID:      taskID,
	}
	testMess := &protocol.BackwardTest{
		LAddrLen: uint16(len([]byte(localAddr))),
		LAddr:    localAddr,
		RPortLen: uint16(len([]byte(rPort))),
		RPort:    rPort,
	}
	b.BackwardReady.Store(taskID, make(chan string, 1))
	defer func() {
		v, ok := b.BackwardReady.Load(taskID)
		if ok {
			close(v.(chan string))
		}
		b.BackwardReady.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, testMess, false)
	sMessage.SendMessage()
	channel, ok := b.BackwardReady.Load(taskID)
	if !ok {
		return errors.New("not find backward ready channel")
	}
	select {
	case errStr := <-channel.(chan string):
		if errStr != "" {
			return fmt.Errorf("fail to map remote port %s to local addr %s,node cannot listen on port %s:%v", rPort, lAddr, rPort, errStr)
		}
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return errors.New("backward error:" + ChannelTimeOut.Error())
	}
	b.log.BackWardInfof("the backward[listen on admin %v agent(%v) %v]start success", lAddr, node, rPort)
	// node can listen,it means no backward service is running on the assigned port,so just register a brand-new backward
	backwardTask := &BackwardTask{
		Mode:  BWNewBackWard,
		LPort: lAddr,
		RPort: rPort,
		UUID:  node,
	}
	b.TaskChan <- backwardTask
	<-b.ResultChan
	// tell upstream all good,just go ahead
	return nil
}

// GetInfoSingleNode 返回指定节点上的map[rPort]的端口转发信息map[agent端口]admin地址及存在的连接数量
func (b *backwardManager) GetInfoSingleNode(node string) map[string]*Backward {
	mgrTask := &BackwardTask{
		Mode: BWGetBackWardInfo,
		UUID: node,
	}
	b.TaskChan <- mgrTask
	result := <-b.ResultChan
	return result.BackwardInfo
}

// StopWithChoice 停止指定节点上的指定端口转发,choice用来指定agent上监听的端口。可以指定为all来关闭所有。该函数会会先通知agent关闭，然后agent处理完关闭消息后会给admin发送关闭消息，admin在关闭本地的转发服务
func (b *backwardManager) StopWithChoice(node string, choice string) error {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	route, ok := b.t.GetRoute(node)
	if !ok {
		return newNodeNotFoundError(node)
	}
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeBackwardStop,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	if choice == "all" {
		stopMess := &protocol.BackwardStop{
			All: 1,
		}

		protocol.ConstructMessage(sMessage, header, stopMess, false)
		sMessage.SendMessage()
		b.log.BackWardInfof("the backward[agent(%v)]all service will be close", node)
	} else {
		stopMess := &protocol.BackwardStop{
			All:      0,
			RPortLen: uint16(len([]byte(choice))),
			RPort:    choice,
		}
		protocol.ConstructMessage(sMessage, header, stopMess, false)
		sMessage.SendMessage()
		b.log.BackWardInfof("the backward[listen on admin %v agent(%v) %v]the service will be close", b.getLocalAddr(node, choice), node, choice)
	}
	return nil
}

// ForceStopOnNode 强制关闭指定节点上所有转发服务，不会先通知agent，之间先在admin上就将服务关闭。一般用于节点下线之后服务自动清除。如果节点没有下线建议使用普通的关闭
func (b *backwardManager) ForceStopOnNode(node string) {
	backwardTask := &BackwardTask{
		Mode: BWForceShutdown,
		UUID: node,
	}
	b.TaskChan <- backwardTask
	<-b.ResultChan
}

func (b *backwardManager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-b.TaskChan:
			switch task.Mode {
			case BWNewBackWard:
				b.newBackward(task)
			case BWGetNewSEQ:
				b.getNewSeq(task)
			case BWAddConn:
				b.addConn(task)
			case BWCheckBackWard:
				b.checkBackward(task)
			case BWGetDataChan:
				b.getDataChan(task)
			case BWGetDataChanWithoutUUID:
				b.getDataChanWithoutUUID(task) //todo 用manager.Done来控制写入会导致整个转发逻辑阻塞，会影响速度，得优化一下（比如设置单转发连接的Done）
				<-b.Done
			case BWCloseTCP:
				b.closeTCP(task)
			case BWGetBackWardInfo:
				b.getBackwardInfo(task)
			case BWCloseSingle:
				b.closeSingle(task)
			case BWCloseAll:
				b.closeSingleAll(task)
			case BWForceShutdown:
				b.forceShutdown(task)
			}
		}
	}
}

// register a brand new back forward
func (b *backwardManager) newBackward(task *BackwardTask) {
	if _, ok := b.backwardMap[task.UUID]; !ok {
		//b.backwardMap = make(map[string]map[string]*Backward)
		b.backwardMap[task.UUID] = make(map[string]*Backward)
	}

	b.backwardMap[task.UUID][task.RPort] = new(Backward)
	b.backwardMap[task.UUID][task.RPort].LocalAddr = task.LPort
	b.backwardMap[task.UUID][task.RPort].BackwardStatusMap = make(map[uint64]*backwardStatus)

	b.ResultChan <- &backwardResult{OK: true}
}

func (b *backwardManager) getNewSeq(task *BackwardTask) {
	b.backwardSeqMap[b.backwardSeq] = &bwSeqRelationship{rPort: task.RPort, uuid: task.UUID}
	b.ResultChan <- &backwardResult{BackwardSeq: b.backwardSeq}
	b.backwardSeq++
}

func (b *backwardManager) addConn(task *BackwardTask) {
	if _, ok := b.backwardSeqMap[task.Seq]; !ok {
		b.ResultChan <- &backwardResult{OK: false}
		return
	}

	b.backwardMap[task.UUID][task.RPort].BackwardStatusMap[task.Seq] = new(backwardStatus)
	b.backwardMap[task.UUID][task.RPort].BackwardStatusMap[task.Seq].dataChan = make(chan []byte, 5)
	b.ResultChan <- &backwardResult{OK: true}
}

func (b *backwardManager) checkBackward(task *BackwardTask) {
	if _, ok := b.backwardSeqMap[task.Seq]; !ok {
		b.ResultChan <- &backwardResult{OK: false}
		return
	}

	if _, ok := b.backwardMap[task.UUID][task.RPort].BackwardStatusMap[task.Seq]; ok {
		b.ResultChan <- &backwardResult{OK: true}
	} else {
		b.ResultChan <- &backwardResult{OK: false}
	}

}

func (b *backwardManager) getDataChan(task *BackwardTask) {
	if _, ok := b.backwardSeqMap[task.Seq]; !ok {
		b.ResultChan <- &backwardResult{OK: false}
		return
	}

	if _, ok := b.backwardMap[task.UUID][task.RPort].BackwardStatusMap[task.Seq]; ok {
		b.ResultChan <- &backwardResult{
			OK:       true,
			DataChan: b.backwardMap[task.UUID][task.RPort].BackwardStatusMap[task.Seq].dataChan,
		}
	} else {
		b.ResultChan <- &backwardResult{OK: false}
	}

}

func (b *backwardManager) getDataChanWithoutUUID(task *BackwardTask) {
	if _, ok := b.backwardSeqMap[task.Seq]; !ok {
		b.ResultChan <- &backwardResult{OK: false}
		return
	}

	uuid := b.backwardSeqMap[task.Seq].uuid
	rPort := b.backwardSeqMap[task.Seq].rPort

	b.ResultChan <- &backwardResult{
		OK:       true,
		DataChan: b.backwardMap[uuid][rPort].BackwardStatusMap[task.Seq].dataChan,
	}
}

func (b *backwardManager) closeTCP(task *BackwardTask) {
	if _, ok := b.backwardSeqMap[task.Seq]; !ok {
		return
	}
	uuid := b.backwardSeqMap[task.Seq].uuid
	rPort := b.backwardSeqMap[task.Seq].rPort
	b.log.BackWardInfof("the backward[listen on admin %v agent(%v) %v]close tcp connect %v,beacause of agent send fin flag", b.getLocalAddr(uuid, rPort), uuid, rPort, task.Seq)
	close(b.backwardMap[uuid][rPort].BackwardStatusMap[task.Seq].dataChan)

	delete(b.backwardMap[uuid][rPort].BackwardStatusMap, task.Seq)
}

func (b *backwardManager) getBackwardInfo(task *BackwardTask) {

	if re, ok := b.backwardMap[task.UUID]; ok {

		b.ResultChan <- &backwardResult{
			OK:           true,
			BackwardInfo: re,
		}
	} else {
		b.ResultChan <- &backwardResult{
			OK:           false,
			BackwardInfo: nil,
		}
	}
}

func (b *backwardManager) closeSingle(task *BackwardTask) {
	//将对应的转发的channel停止
	if _, ok := b.backwardMap[task.UUID]; !ok {
		b.ResultChan <- &backwardResult{OK: true}
		return
	}

	rPort := task.RPort
	if _, ok2 := b.backwardMap[task.UUID][rPort]; !ok2 {
		b.ResultChan <- &backwardResult{OK: true}
		return
	}
	if l := len(b.backwardMap[task.UUID][rPort].BackwardStatusMap); l != 0 {
		b.log.BackWardWarnf("the backward[listen on admin %v agent(%v) %v]the backward is stopped, but there are still %v connections alive, ready to forcibly disconnect", b.getLocalAddr(task.UUID, rPort), task.UUID, rPort, l)
	}

	//将所有的channel都关闭
	for _, n := range b.backwardMap[task.UUID][rPort].BackwardStatusMap {
		close(n.dataChan)
	}

	//todo 这里检查一下channel有没有被回收，或者我们在这主动关闭：应该是被回收了，在对应的Handel中使用完成后就被垃圾回收机制回收了，在此不关闭的话可以让没有转发完成的数据转发完成。
	//todo 出现了问题，当转发被关闭，但是我没没有处理里面的channel，会导致在接收到客户端断开连接请求的时候到manager中找不到channel进而无法关闭，从而一只处于阻塞状态，后面特殊处理这种，做软停止，这个函数里面做硬删除
	delete(b.backwardMap[task.UUID], rPort)

	for seq, relationship := range b.backwardSeqMap {
		if relationship.uuid == task.UUID && relationship.rPort == rPort {
			delete(b.backwardSeqMap, seq)
		}
	}

	if len(b.backwardMap[task.UUID]) == 0 {
		delete(b.backwardMap, task.UUID)
	}

	b.ResultChan <- &backwardResult{OK: true}
}

func (b *backwardManager) closeSingleAll(task *BackwardTask) {
	for rPort := range b.backwardMap[task.UUID] {
		//将所有的channel都关闭
		if l := len(b.backwardMap[task.UUID][rPort].BackwardStatusMap); l != 0 {
			b.log.BackWardWarnf("the backward[listen on admin %v agent(%v) %v]the backward is stopped, but there are still %v connections alive, ready to forcibly disconnect", b.getLocalAddr(task.UUID, rPort), task.UUID, rPort, l)
		}
		for _, n := range b.backwardMap[task.UUID][rPort].BackwardStatusMap {
			close(n.dataChan)
		}
		delete(b.backwardMap[task.UUID], rPort)
	}

	for seq, relationship := range b.backwardSeqMap {
		if relationship.uuid == task.UUID {
			delete(b.backwardSeqMap, seq)
		}
	}

	delete(b.backwardMap, task.UUID)

	b.ResultChan <- &backwardResult{OK: true}
}

func (b *backwardManager) getLocalAddr(node, rPort string) string {
	if b.backwardMap[node] != nil && b.backwardMap[node][rPort] != nil {
		return b.backwardMap[node][rPort].LocalAddr
	}
	return ""
}

func (b *backwardManager) forceShutdown(task *BackwardTask) {
	if _, ok := b.backwardMap[task.UUID]; ok {
		for rPort := range b.backwardMap[task.UUID] {
			for seq, status := range b.backwardMap[task.UUID][rPort].BackwardStatusMap {
				close(status.dataChan)
				delete(b.backwardMap[task.UUID][rPort].BackwardStatusMap, seq)
			}
			delete(b.backwardMap[task.UUID], rPort)
		}

		for seq, relationship := range b.backwardSeqMap {
			if relationship.uuid == task.UUID {
				delete(b.backwardSeqMap, seq)
			}
		}

		delete(b.backwardMap, task.UUID)
	}

	b.ResultChan <- &backwardResult{OK: true}
}

// agent发送过来消息要开始进行转发了。
func (b *backwardManager) startBackWard(info *protocol.BackwardStart) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	// first , admin need to know the route to target node,so ask t for the answer
	route, ok := b.t.GetRoute(info.UUID)
	if !ok {
		b.log.BackWardError(newNodeNotFoundError(info.UUID))
		return
	}
	// ask backward manager to assign a new seq num
	//todo  新建和获取seq是不是可以合并到一起
	backwardTask := &BackwardTask{
		Mode:  BWGetNewSEQ,
		RPort: info.RPort,
		UUID:  info.UUID,
	}
	b.TaskChan <- backwardTask
	result := <-b.ResultChan
	seq := result.BackwardSeq

	backwardTask = &BackwardTask{
		Mode:  BWAddConn,
		RPort: info.RPort,
		UUID:  info.UUID,
		Seq:   seq,
	}
	b.TaskChan <- backwardTask
	<-b.ResultChan
	b.log.BackWardInfof("the backward[listen on admin %v agent(%v) %v(seq:%v)]receive a request form agent", info.LAddr, info.UUID, info.RPort, seq)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    info.UUID,
		MessageType: protocol.MessageTypeBackwardSeq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	seqMess := &protocol.BackwardSeq{
		Seq:      seq,
		RPortLen: uint16(len([]byte(info.RPort))),
		RPort:    info.RPort,
	}

	protocol.ConstructMessage(sMessage, header, seqMess, false)
	sMessage.SendMessage()

	// send fin after all done
	defer func() {
		finHeader := &protocol.Header{
			Sender:      protocol.AdminUuid,
			Acceptor:    info.UUID,
			MessageType: protocol.MessageTypeBackwardFin,
			RouteLen:    uint32(len([]byte(route))),
			Route:       route,
		}

		finMess := &protocol.BackWardFin{
			Seq: seq,
		}

		protocol.ConstructMessage(sMessage, finHeader, finMess, false)
		sMessage.SendMessage()
	}()
	backwardConn, err := net.DialTimeout("tcp", info.LAddr, 10*time.Second)
	if err != nil {
		return
	}

	backwardTask = &BackwardTask{
		Mode:  BWCheckBackWard,
		RPort: info.RPort,
		UUID:  info.UUID,
		Seq:   seq,
	}
	b.TaskChan <- backwardTask
	result = <-b.ResultChan
	if !result.OK {
		_ = backwardConn.Close()
		return
	}

	backwardTask = &BackwardTask{
		Mode:  BWGetDataChan,
		RPort: info.RPort,
		UUID:  info.UUID,
		Seq:   seq,
	}
	b.TaskChan <- backwardTask
	result = <-b.ResultChan
	if !result.OK {
		return
	}

	dataChan := result.DataChan
	transferChannel := bufferChannel.NewInfiniteChan(100)
	go func() {
		i := 0
		for {
			if data, ok := <-dataChan; ok {
				i++
				transferChannel.In <- data
			} else {
				transferChannel.Close()
				return
			}
		}
	}()
	go func() {
		i := 0
		for {
			if data, ok := <-transferChannel.Out; ok {
				i++
				_, err = backwardConn.Write(data.([]byte))
				b.log.BackWardDebugf("the backward[listen on admin %v agent(%v) %v(seq:%v)]get data from longChan and write to Admin local success \"%v\"", info.LAddr, info.UUID, info.RPort, seq, string(data.([]byte)))
				if err != nil {
					b.log.BackWardErrorf("the backward([listen on admin %v agent(%v) %v(seq:%v)]An error occurred in forwarding data received from the dataChannel to the admin connection：%v", info.LAddr, info.UUID, info.RPort, seq, err)
				}
			} else {
				b.log.BackWardInfof("the backward[listen on admin %v agent(%v) %v(seq:%v)]close tcp initial(because long channel close)", info.LAddr, info.UUID, info.RPort, seq)
				_ = backwardConn.Close()
				return
			}
		}
	}()
	// proxy S2C
	dataHeader := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    info.UUID,
		MessageType: protocol.MessageTypeBackwardData,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	buffer := make([]byte, 131072)

	for {
		length, err := backwardConn.Read(buffer)
		if err != nil {
			b.log.BackWardInfof("the backward(listen on admin %v agent(%v) %v(seq:%v))initial closed(admin active close)", info.LAddr, info.UUID, info.RPort, seq)
			_ = backwardConn.Close()
			return
		}

		backwardDataMess := &protocol.BackwardData{
			Seq:     seq,
			DataLen: uint64(length),
			Data:    buffer[:length],
		}

		protocol.ConstructMessage(sMessage, dataHeader, backwardDataMess, false)
		sMessage.SendMessage()
	}
}

func (b *backwardManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-b.BackwardMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						b.log.GeneralErrorf("backward handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.BackwardReady:
					if channel, ok := b.BackwardReady.Load(packet.ReqHeader.TaskID); ok {
						channel.(chan string) <- mess.Err
					} else {
						b.log.GeneralErrorf("backward %v", ChannelClose)
					}
				case *protocol.BackwardStart:
					// get the start messageData from node,so just start a backward
					go b.startBackWard(mess)
				case *protocol.BackwardData:
					mgrTask := &BackwardTask{
						Mode: BWGetDataChanWithoutUUID,
						Seq:  mess.Seq,
					}
					b.TaskChan <- mgrTask
					result := <-b.ResultChan
					if result.OK {
						result.DataChan <- mess.Data
					} else {
						b.log.BackWardWarnf("the backward data channel %v is closed,this part of the data will be lost:'%v'", mess.Seq, string(mess.Data))
					}
					b.Done <- true
				case *protocol.BackWardFin:
					mgrTask := &BackwardTask{
						Mode: BWCloseTCP,
						Seq:  mess.Seq,
					}
					b.TaskChan <- mgrTask
				case *protocol.BackwardStopDone:
					if mess.All == 1 {
						backwardTask := &BackwardTask{
							Mode: BWCloseAll,
							UUID: mess.UUID,
						}
						b.TaskChan <- backwardTask
					} else {
						backwardTask := &BackwardTask{
							Mode:  BWCloseSingle,
							UUID:  mess.UUID,
							RPort: mess.RPort,
						}
						b.TaskChan <- backwardTask
					}
					<-b.ResultChan
				}
			}(messageData)
		}
	}
}
