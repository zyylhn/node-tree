package manager

import (
	"context"
	"fmt"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"os"
	"strings"
	"time"
)

//todo 详细日志待补充

type FileUpload struct {
	//文件信息
	fileData *os.File
	fileInfo os.FileInfo
	savePath string //保存到agent的路径
	srcPath  string
	fileHash string

	fMgr *fileUploadManager

	blockSize      uint32 //发送的分块大小
	endBlockSize   uint32 //最后一段分块的大小
	totalBlockNum  uint64 //分块数量
	maxUploadCount int    //最多尝试次数
	isCover        bool   //如果文件存在是否覆盖，true就是将之前文件删除，现在文件覆盖上去，false就是将新文件重命名

	state chan *public.Status //用来通知进度channel
}

// Upload 如果个给定的ctx结束了将会完全停止文件上传，并不会保留临时文件
func (f *FileUpload) Upload(savePath, nodeID, route string, state chan *public.Status, ctx context.Context) error {
	return f.upload(ctx, savePath, nodeID, route, state)
}

// 将创建的文件上传到指定的节点的指定路径中
func (f *FileUpload) upload(pCtx context.Context, savePath, nodeID, route string, state chan *public.Status) error {
	if strings.HasSuffix(savePath, "/") || strings.HasSuffix(savePath, "\\") {
		return fmt.Errorf("保存到目标上的文件路径不合法，需要完整文件路径及文件名而不仅仅是文件路径")
	}
	taskID := utils.GenerateUUID()
	if state == nil {
		state = make(chan *public.Status)
		go func() {
			for range state {
			}
		}()
	}
	defer func() {
		close(state)
	}()
	defer func() {
		_ = f.fileData.Close()
	}()

	ctx, cancel := context.WithCancel(pCtx)
	//如果ctx没取消的话在结束的时候取消（有可能被manager给取消）
	defer func() {
		select {
		case <-ctx.Done():
		default:
			cancel()
		}
	}()
	//发送上传文件信息给agent
	missBlock, err := f.sendUploadRequest(taskID, ctx, savePath, nodeID, route)
	if err != nil {
		if strings.Contains(err.Error(), TerminationErr.Error()) {
			return err
		}
		f.uploadError(taskID, err)
		return err
	}
	if l := uint64(len(missBlock)); l != f.totalBlockNum {
		f.fMgr.InitBlock(taskID, f.totalBlockNum-l)
	}
	err = f.startUploadFileData(taskID, ctx, nodeID, route, state, missBlock)
	if err != nil {
		if strings.Contains(err.Error(), TerminationErr.Error()) {
			return err
		}
		f.uploadError(taskID, err)
		return err
	}
	f.fMgr.SetStateSuccess(taskID)
	return nil
}

func (f *FileUpload) startUploadFileData(taskID string, pCtx context.Context, nodeID, route string, state chan *public.Status, miss protocol.MissingSegment) error {
	var err error
	f.fMgr.SetStateUploading(taskID)
	state <- &public.Status{
		Stat:   public.FileStart,
		TaskID: taskID,
	}
	f.fMgr.AgentWriteFileError.Store(taskID, make(chan error, 1))
	defer func() {
		if c, ok := f.fMgr.AgentWriteFileError.Load(taskID); ok {
			close(c.(chan error))
		}
		f.fMgr.AgentWriteFileError.Delete(taskID)
	}()
	channel, _ := f.fMgr.AgentWriteFileError.Load(taskID)
	for i := 0; i < f.maxUploadCount; i++ {
		err = f.sendFileData(channel.(chan error), taskID, pCtx, nodeID, route, state, miss)
		if err != nil {
			return err
		}
		miss, err = f.sendDataEnd(taskID, pCtx, nodeID, route)
		if err != nil {
			return err
		}
		if len(miss) == 0 { //不缺失分段，上传成功
			state <- &public.Status{
				Stat:   public.FileComplete,
				TaskID: taskID,
			}
			return nil
		} else {
			state <- &public.Status{
				Stat:   public.FileDelete,
				Scale:  int64(uint32(len(miss)) * f.blockSize),
				TaskID: taskID,
			}
			f.fMgr.DeleteComplete(taskID, uint64(len(miss)))
		}
		f.fMgr.log.FileUploadDebugf("FileUpload [nodeID:%v srcPath:%v targetPath:%v] agent missing segments: %v. Number of attempts to send missing segments: %v", nodeID, f.srcPath, f.savePath, miss, i+1)
	}
	return fmt.Errorf("file upload failure: The file integrity verification still fails after the %v segmented upload", f.maxUploadCount)
}

func (f *FileUpload) sendFileData(agentWriteError chan error, taskID string, pCtx context.Context, nodeID string, route string, state chan *public.Status, miss protocol.MissingSegment) error {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    nodeID,
		MessageType: protocol.MessageTypeUploadFileData,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}

	for _, i := range miss {
		select {
		case <-pCtx.Done():
			f.noticeCtxCancel(taskID, route, nodeID, "upload data")
			return fmt.Errorf("upload file data " + TerminationErr.Error())
		case err := <-agentWriteError:
			return err
		default:
		}
		blockData, err := f.fMgr.GetFileData(taskID, i)
		if err != nil {
			f.noticeUploadStop(taskID, route, nodeID, false)
			return fmt.Errorf("failed to upload the file, read the file segment from the local error:%v", err)
		}
		dataMess := &protocol.FileManagerFileData{
			TaskIDLen:  uint16(len(taskID)),
			TaskID:     taskID,
			DataLen:    uint64(len(blockData.Data)),
			Data:       blockData.Data,
			SegmentNum: blockData.Num,
		}
		protocol.ConstructMessage(sMessage, header, dataMess, false)
		sMessage.SendMessage()
		state <- &public.Status{
			Stat:   public.FileAdd,
			Scale:  int64(f.blockSize),
			TaskID: taskID,
		}
	}
	return nil
}

func (f *FileUpload) sendDataEnd(taskID string, pCtx context.Context, nodeID string, route string) (protocol.MissingSegment, error) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    nodeID,
		MessageType: protocol.MessageTypeUploadFileDataEnd,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	endMess := &protocol.FileManagerTransferEnd{TaskIDLen: uint16(len(taskID)), TaskID: taskID}
	f.fMgr.FileUploadEndRes.Store(taskID, make(chan *public.MissingBlockWithError, 1))
	defer func() {
		v, ok := f.fMgr.FileUploadEndRes.Load(taskID)
		if ok {
			close(v.(chan *public.MissingBlockWithError))
		}
		f.fMgr.FileUploadEndRes.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, endMess, false)
	sMessage.SendMessage()
	channel, ok := f.fMgr.FileUploadEndRes.Load(taskID)
	if !ok {
		return nil, fmt.Errorf("file upload not find send end flag channel")
	}
	select {
	case info := <-channel.(chan *public.MissingBlockWithError):
		if info.Error == "" {
			return info.Block, nil
		} else {
			return nil, fmt.Errorf(info.Error)
		}
	case <-pCtx.Done():
		f.noticeCtxCancel(taskID, route, nodeID, "waiting for check size response")
		return nil, fmt.Errorf("waiting for check size" + TerminationErr.Error())
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return nil, fmt.Errorf("upload file waiting for check size error:" + ChannelTimeOut.Error())
	}
}

// 向agent发送上传文件请求，agent会返回错误或者告诉admin自己缺少的文件分段
func (f *FileUpload) sendUploadRequest(taskID string, ctx context.Context, savePath, nodeID, route string) (protocol.MissingSegment, error) {
	//计算文件hash
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    nodeID,
		MessageType: protocol.MessageTypeFileUploadReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	mess := &protocol.FileManagerReq{
		TaskIDLen:     uint16(len(taskID)),
		TaskID:        taskID,
		FilePathLen:   uint16(len(savePath)),
		FilePath:      savePath,
		BlockLength:   f.totalBlockNum,
		BlockSize:     f.blockSize,
		EndBlockSize:  f.endBlockSize,
		FileSHA256Len: uint16(len(f.fileHash)),
		FileSHA256:    f.fileHash,
	}
	f.fMgr.FileUploadInfoRes.Store(taskID, make(chan *public.MissingBlockWithError, 1))
	defer func() {
		v, ok := f.fMgr.FileUploadInfoRes.Load(taskID)
		if ok {
			close(v.(chan *public.MissingBlockWithError))
		}
		f.fMgr.FileUploadInfoRes.Delete(taskID)
	}()
	protocol.ConstructMessage(sMessage, header, mess, false)
	sMessage.SendMessage()
	channel, ok := f.fMgr.FileUploadInfoRes.Load(taskID)
	if !ok {
		return nil, fmt.Errorf("uploadFile:not find send fileInfo channel")
	}
	select {
	case missBlock := <-channel.(chan *public.MissingBlockWithError):
		if missBlock.Error != "" {
			return nil, fmt.Errorf(missBlock.Error)
		} else {
			f.fMgr.UpdateSavePath(taskID, missBlock.FilePath)
			return missBlock.Block, nil
		}
	case <-ctx.Done():
		f.noticeCtxCancel(taskID, route, nodeID, "send upload information")
		return nil, fmt.Errorf("send upload information " + TerminationErr.Error())
	case <-time.After(time.Second * protocol.ChanTimeOut):
		return nil, fmt.Errorf("upload file send file info error:" + ChannelTimeOut.Error())
	}
}

// 通知agent任务被终止，agent会结束当前任务后续所有操作
func (f *FileUpload) noticeCtxCancel(taskID, route, uuid, action string) {
	//设置任务状态终止
	f.fMgr.log.FileUploadWarnf("FileUpload [nodeID:%v srcPath:%v targetPath:%v] task %v the module is terminated by ctx", uuid, f.srcPath, f.savePath, taskID)
	f.fMgr.SetStateTerminate(taskID, action+" Be actively cancelled")
	//被取消了去读取manager是否要保留或者删除agent上的临时文件
	f.noticeUploadStop(taskID, route, uuid, f.fMgr.GetDeleteTmpFileState(taskID))
}

// 通知子节点停止文件上传相关操作
func (f *FileUpload) noticeUploadStop(taskID, route, uuid string, deleteTmpFile bool) {
	//todo 发送前检查节点是否还在线，不在线就不发了
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)
	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeCancelUpload,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	cancelMess := &protocol.StopFileTransfer{TaskIDLen: uint16(len(taskID)), TaskID: taskID}
	if deleteTmpFile {
		cancelMess.IsDelete = 1
	}
	protocol.ConstructMessage(sMessage, header, cancelMess, false)
	sMessage.SendMessage()
}

// 上传失败，用来记录失败状态和通知
func (f *FileUpload) uploadError(taskID string, err error) {
	f.fMgr.SetStateFail(taskID, err.Error())
}
