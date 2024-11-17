package manager

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"strings"
	"time"
)

type RemoteLoad struct {
	taskID     string //任务id
	moduleName string //模块名字

	blockSize      int               //发送的分块大小
	endBlockSize   int               //最后一段分块的大小
	moduleData     map[uint64][]byte //经过分段处理的模块信息
	maxUploadCount int
	loadDir        string

	mgr *remoteLoadManager
	t   *topology.Topology

	args   string //参数或者参数服务器的地址
	ctx    context.Context
	cancel context.CancelFunc
}

func (r *RemoteLoad) GetTaskID() string {
	return r.taskID
}

// LoadExec 远程加载功能函数入口,要使用协程在后台运行，不然运行时间太长阻塞操作,返回执行结果字节数组
func (r *RemoteLoad) LoadExec(node, loadDir string) ([]byte, error) {
	route, ok := r.t.GetRoute(node)
	if !ok {
		return nil, newNodeNotFoundError(node)
	}

	re, err := r.loadExec(route, node, loadDir)
	if err != nil {
		return nil, err
	}
	r.mgr.log.LoadModuleInfof("[load handle taskID:%v]Module output result%v", r.taskID, string(re))
	return re, nil
}

func (r *RemoteLoad) loadExec(route, node, loadDir string) ([]byte, error) {
	r.loadDir = loadDir
	//后台新建一个load记录,将cancel和新派生的ctx存到admin的load manager中
	_ = r.mgr.NewLoad(node, r.taskID, r.moduleName, r.args, &r.cancel, &r.ctx)
	defer func() {
		//如果ctx关掉了的话什么都不用做，没关掉的话关掉ctx
		select {
		case <-r.ctx.Done():
		default:
			r.cancel()
		}
		//运行结束将内存中的记录删除,并通知agent结束模块运行
		r.mgr.DeleteRecord(r.taskID)
		r.noticeTaskEnd(r.taskID, route, node)
	}()
	//将参数任务id，module发送给agent
	missBlock, err := r.sendTask(route, node)
	if err != nil {
		r.mgr.log.LoadModuleErrorf("[load handle taskID:%v]module information failed to be uploaded:%v", r.taskID, err)
		if strings.Contains(err.Error(), TerminationErr.Error()) {
			return nil, err
		}
		r.loadError(err)
		return nil, err
	}
	r.mgr.log.LoadModuleInfof("[load handle taskID:%v]module information is sent successfully", r.taskID)
	//创建接收结果信息的channel，提前创建。
	r.mgr.ModuleComplete.Store(r.taskID, make(chan *protocol.ModuleResultInfo, 1))
	defer func() {
		v, ok := r.mgr.ModuleComplete.Load(r.taskID)
		if ok {
			close(v.(chan *protocol.ModuleResultInfo))
		}
		r.mgr.ModuleComplete.Delete(r.taskID)
	}()
	r.mgr.StateUpload(r.taskID) //开始上传模块
	//循环发送module到agent，并等待agent的响应
	err = r.uploadModule(route, node, missBlock)
	if err != nil {
		r.mgr.log.LoadModuleErrorf("[load handle taskID:%v]module data failed to be uploaded:%v", r.taskID, err)
		if strings.Contains(err.Error(), TerminationErr.Error()) {
			return nil, err
		}
		r.loadError(err)
		return nil, err
	}
	r.mgr.log.LoadModuleInfof("[load handle taskID:%v]module data is sent successfully", r.taskID)
	//接收没有问题则已经开始运行远程模块，在此更新模块状态
	r.mgr.StateRunning(r.taskID)
	channel, ok := r.mgr.ModuleComplete.Load(r.taskID)
	if !ok {
		return nil, errors.New("not find module running complete channel")
	}
	select {
	case i := <-channel.(chan *protocol.ModuleResultInfo):
		if i.Err != "" {
			err = errors.New(i.Err)
			r.loadError(err)
			r.mgr.log.LoadModuleErrorf("[load handle taskID:%v]module running error:%v", r.taskID, err)
			return nil, err
		}
		if i.StdErr != "" {
			r.mgr.log.LoadModuleErrorf("[load handle taskID:%v]module running stderr:%v", r.taskID, i.StdErr)
		}
		re, err := r.receiveResult(route, node, i)
		if err != nil {
			r.mgr.log.LoadModuleErrorf("[load handle taskID:%v]module received the result error:%v", r.taskID, err)
			if strings.Contains(err.Error(), TerminationErr.Error()) {
				return nil, err
			}
			r.loadError(err)
			return nil, err
		}
		return re, nil
	case <-r.ctx.Done():
		//发送结束标志
		r.noticeCtxCancel(r.taskID, "waiting module running")
		return nil, errors.New("waiting module result " + TerminationErr.Error())
	}
}

// 循环上传并获取检查结果
func (r *RemoteLoad) uploadModule(route, uuid string, missBlock protocol.MissingSegment) error {
	var err error
	for i := 0; i < r.maxUploadCount; i++ {
		err = r.sendModuleData(route, uuid, missBlock)
		if err != nil {
			return err
		}
		missBlock, err = r.sendDataEnd(route, uuid)
		if err != nil {
			return err
		}
		if len(missBlock) == 0 {
			return nil
		}
		r.mgr.log.LoadModuleDebugf("[load handle taskID:%v] agent Number of module segments missing: %v,Number of attempts to send missing segments: %v", r.taskID, missBlock, i+1)
	}
	//多次上传还是失败通知agent删除模块
	//noticeTaskEnd(r.taskID, route, uuid)
	return errors.New("failed to upload module, segmented upload several times, but still lack of segmentation failed to upload. Check the network connection or reconnect the agent")
}

// 读取并检查返回的结果
func (r *RemoteLoad) receiveResult(route, uuid string, blockInfo *protocol.ModuleResultInfo) ([]byte, error) {
	//新建分段信息
	err := r.mgr.SetResultBlockInfo(uuid, r.taskID, blockInfo.BlockLength, blockInfo.BlockSize, blockInfo.EndBlockSize)
	if err != nil {
		return nil, err
	}
	//开启后台转发。接收数据的时候每次都会检查manager中的ctx，如果ctx结束了就停止接收
	r.mgr.Transfer(r.taskID)
	var miss protocol.MissingSegment
	for i := uint64(0); i < blockInfo.BlockLength; i++ {
		miss = append(miss, i)
	}
	for i := 0; i < r.maxUploadCount; i++ {
		miss, err = r.readyReceive(route, uuid, miss)
		if err != nil {
			return nil, err
		}
		if len(miss) == 0 {
			break
		} else if i == r.maxUploadCount {
			r.mgr.log.LoadModuleErrorf("remote loading task %v Download results Missing segmentation %v after %v attempts", r.taskID, r.maxUploadCount, miss)
		}
	}
	//关闭接收channel
	r.mgr.ReceiveModule(r.taskID, nil, 0, true)

	//任务结束计算时间并设置状态
	r.mgr.SetComplete(r.taskID)
	loadInfo := r.mgr.GetSingleInfo(r.taskID)
	return loadInfo.ModuleRes, nil
}

// 通知manager准备接收，并返回响应给agent告知其可以发送
func (r *RemoteLoad) readyReceive(route, uuid string, missInfo protocol.MissingSegment) (protocol.MissingSegment, error) {
	//启动准备读取发送过来的数据
	//创建接收结果的channel
	r.mgr.DownLoadResult.Store(r.taskID, make(chan string))
	defer func() {
		v, ok := r.mgr.DownLoadResult.Load(r.taskID)
		if ok {
			close(v.(chan string))
		}
		r.mgr.DownLoadResult.Delete(r.taskID)
	}()
	//发送准备接收完成响应
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeReadyReceive,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	//将所需要的分段信息发给agent
	mess := &protocol.ReadyReceive{TaskIDLen: uint16(len(r.taskID)), TaskID: r.taskID}
	missBytes, err := json.Marshal(&missInfo)
	if err != nil {
		//noticeTaskEnd(r.taskID, route, uuid)
		return nil, err
	}
	missStr := string(missBytes)
	mess.MissSegmentLen = uint32(len(missStr))
	mess.MissSegment = missStr
	protocol.ConstructMessage(sMessage, header, mess, false)
	sMessage.SendMessage()
	r.mgr.StateDownload(r.taskID) //下载结果中
	channel, ok := r.mgr.DownLoadResult.Load(r.taskID)
	if !ok {
		return nil, errors.New("not find download result channel")
	}
	select {
	//manager将结果下载完成
	case errStr := <-channel.(chan string): //成功读取到目标信息，此时为字节
		if errStr != "" {
			//agent会自己停掉不需要我们给停
			return nil, err
		}
		//agent发送完成，我们需要检查结果完整性
		miss, err := r.mgr.CheckResultSize(uuid, r.taskID)
		if err != nil {
			//noticeTaskEnd(r.taskID, route, uuid)
			return nil, err
		}
		return miss, nil
	case <-r.ctx.Done():
		//等待下载结果的时候被结束
		r.noticeCtxCancel(r.taskID, "(download)wait download result")
		return nil, errors.New("download result " + TerminationErr.Error())
	}
}

// 发送任务信息给agent
func (r *RemoteLoad) sendTask(route, uuid string) (protocol.MissingSegment, error) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeSendTaskInfoReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	sendTask := &protocol.SendTaskInfoReq{
		ArgsLen:      uint16(len(r.args)),
		Args:         r.args,
		TaskIDLen:    uint16(len(r.taskID)),
		TaskID:       r.taskID,
		LoadDirLen:   uint16(len(r.loadDir)),
		LoadDir:      r.loadDir,
		BlockLength:  uint64(len(r.moduleData)),
		BlockSize:    uint32(r.blockSize),
		EndBlockSize: uint32(r.endBlockSize),
	}

	r.mgr.SendTaskInfoRes.Store(r.taskID, make(chan *public.MissingBlockWithError, 1))
	defer func() {
		v, ok := r.mgr.SendTaskInfoRes.Load(r.taskID)
		if ok {
			close(v.(chan *public.MissingBlockWithError))
		}
		r.mgr.SendTaskInfoRes.Delete(r.taskID)
	}()

	protocol.ConstructMessage(sMessage, header, sendTask, false)
	sMessage.SendMessage()
	channel, ok := r.mgr.SendTaskInfoRes.Load(r.taskID)
	if !ok {
		return nil, errors.New("not find send task channel")
	}
	select {
	case missBlock := <-channel.(chan *public.MissingBlockWithError):
		if missBlock.Error != "" {
			return nil, errors.New(missBlock.Error)
		} else {
			return missBlock.Block, nil
		}
	case <-r.ctx.Done():
		//发送停止接收转发当前任务，返回终止信息
		r.noticeCtxCancel(r.taskID, "send task information")
		return nil, errors.New("send task information " + TerminationErr.Error())
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return nil, errors.New("load module send task info error:" + ChannelTimeOut.Error())
	}
}

// 将module发送给目标agent
func (r *RemoteLoad) sendModuleData(route, uuid string, missBlock protocol.MissingSegment) error {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeSendModuleData,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	for _, i := range missBlock {
		//检查是否被终止
		select {
		case <-r.ctx.Done():
			r.noticeCtxCancel(r.taskID, "(upload)upload module")
			return errors.New("upload module " + TerminationErr.Error())
		default:
		}
		dataMess := &protocol.FileManagerFileData{
			TaskIDLen:  uint16(len(r.taskID)),
			TaskID:     r.taskID,
			DataLen:    uint64(len(r.moduleData[i])),
			Data:       r.moduleData[i],
			SegmentNum: i,
		}
		protocol.ConstructMessage(sMessage, header, dataMess, false)
		sMessage.SendMessage()
	}

	return nil
}

// 发送结束上传包，如果有异常就是通知agent停止，并不接收返回包
func (r *RemoteLoad) sendDataEnd(route, uuid string) (protocol.MissingSegment, error) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeSendModuleEnd,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	endMess := &protocol.FileManagerTransferEnd{TaskIDLen: uint16(len(r.taskID)), TaskID: r.taskID}

	r.mgr.SendDataEndRes.Store(r.taskID, make(chan *public.MissingBlockWithError, 1))
	defer func() {
		v, ok := r.mgr.SendDataEndRes.Load(r.taskID)
		if ok {
			close(v.(chan *public.MissingBlockWithError))
		}
		r.mgr.SendDataEndRes.Delete(r.taskID)
	}()

	////等一会在发是为了保证agent能将最后一个包保存（异步）
	//time.Sleep(time.Second*2)
	protocol.ConstructMessage(sMessage, header, endMess, false)
	sMessage.SendMessage()
	channel, ok := r.mgr.SendDataEndRes.Load(r.taskID)
	if !ok {
		return nil, errors.New("load module not find send end flag channel")
	}
	select {
	case info := <-channel.(chan *public.MissingBlockWithError):
		if info.Error == "" {
			return info.Block, nil
		} else {
			return nil, errors.New(info.Error)
		}
	case <-r.ctx.Done():
		r.noticeCtxCancel(r.taskID, "(upload)waiting for check size response")
		return nil, errors.New("waiting for check size" + TerminationErr.Error())
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return nil, errors.New("agent check module size error:" + ChannelTimeOut.Error())
	}
}

// 模块加载失败，用于设置模块加载状态
func (r *RemoteLoad) loadError(err error) {
	r.mgr.StateFail(r.taskID, err.Error())
}

// 通知agent任务被终止，agent会结束当前任务后续所有操作
func (r *RemoteLoad) noticeCtxCancel(taskID, action string) {
	//设置任务状态终止
	r.mgr.log.LoadModuleWarnf("[load handle taskID:%v]the module is terminated by ctx", taskID)
	r.mgr.StateAbort(taskID, action)
}

// 通知agent任务被终止，agent会结束当前任务后续所有操作
func (r *RemoteLoad) noticeTaskEnd(taskID, route, node string) {
	//todo 检查节点是否在线，不在线就不发了
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    node,
		MessageType: protocol.MessageTypeCancelRemoteLoadReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	cancelMess := &protocol.CancelCtx{TaskIDLen: uint16(len(taskID)), TaskID: taskID}
	protocol.ConstructMessage(sMessage, header, cancelMess, false)
	sMessage.SendMessage()
}
