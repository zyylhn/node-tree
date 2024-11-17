package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"os"
	"sort"
	"sync"
	"time"
)

const (
	SendBaseInfo    = "sendUploadInfo"
	UploadFileData  = "uploadFileData"
	UploadSuccess   = "uploadSuccess"
	UploadFail      = "uploadFail"
	UploadTerminate = "uploadTerminate"
)

const (
	UPLOADNew               = iota
	UPLOADGetFileData       //获取文件内容
	UPLOADSetState          //设置文件上传状态
	UPLOADCancelTask        //取消指定上传任务
	UPLOADCancelNodeTask    //取消指定节点上所有的文件上传任务
	UPLOADGetUploadInfo     //获取上传任务信息
	UPLOADGetNodeUploadInfo //获取指定节点的文件上传信息
	UPLOADGetAllUploadInfo  //获取所有文件上传信息
	UPLOADUpdateSavePath    //更新在agent上保存的文件名
	UPLOADDeleteCompleteNum //重传删除进度
	UPLOADGetDeleteTmpFile  //读取是否删除下载时的临时文件
	UPLOADPauseTask         //暂定任务（其实也是停止任务，但是不删除agent那面预留的临时文件）
	UPLOADAddBlock          //给定初始传输完成的分段
)

//todo 暂时不对上传下载记录进行保存数量的限制，先都存在内存中

// 用来保存文件加载记录
type fileUploadManager struct {
	FileUploadMessChan chan *protocol.CompletePacket //handel监听的消息channel

	uploadInfo  map[string]map[string]*UploadInfo //所有正在运行的文件上传信息map[uuid]map[taskID]上传信息
	uploadIndex map[string]string                 //map[taskID]uuid  记录指定任务归属于哪个节点   //todo 删除上面信息的时候也要考虑对索引的删除

	uploadTaskChan   chan *UploadTask
	uploadResultChan chan *UploadResult

	FileUploadInfoRes   sync.Map //map[taskID]chan public.MissingBlockWithError,返回error和缺失的分段
	FileUploadEndRes    sync.Map //map[taskID]chan public.MissingBlockWithError，返回error和缺失的分段
	AgentWriteFileError sync.Map //map[taskID]chan error

	log *public.NodeManagerLog
	t   *topology.Topology
}

type UploadInfo struct {
	public.UploadBaseInfo

	file          *os.File
	Cancel        context.CancelFunc
	Ctx           context.Context
	deleteTmpFile bool
}

type UploadTask struct {
	TaskID string
	NodeID string
	Mode   int //对应上面的操作
	UploadInfo
	blockNum          uint64
	ErrorReason       string
	TerminateReason   string
	State             string
	SavePath          string
	DeleteCompleteNum uint64
	AddBlock          uint64
}

type UploadResult struct {
	SingleUploadInfo *public.UploadBaseInfo
	Error            error
	Ok               bool
	fileData         *protocol.BlockWithNum
	allTaskInfo      map[string][]*public.UploadBaseInfo
}

func newFileUploadManager(log *public.NodeManagerLog, t *topology.Topology) *fileUploadManager {
	re := new(fileUploadManager)
	re.FileUploadMessChan = make(chan *protocol.CompletePacket, 5)
	re.uploadInfo = make(map[string]map[string]*UploadInfo)
	re.uploadIndex = make(map[string]string)
	re.uploadTaskChan = make(chan *UploadTask)
	re.uploadResultChan = make(chan *UploadResult)
	re.FileUploadInfoRes = sync.Map{}
	re.FileUploadEndRes = sync.Map{}
	re.AgentWriteFileError = sync.Map{}

	re.log = log
	re.t = t
	return re
}

func (f *fileUploadManager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-f.uploadTaskChan:
			switch task.Mode {
			case UPLOADNew:
				f.newUpload(task)
			case UPLOADGetFileData:
				f.getFileData(task)
			case UPLOADSetState:
				f.setState(task)
			case UPLOADGetNodeUploadInfo:
				f.getNodeUploadInfo(task)
			case UPLOADGetAllUploadInfo:
				f.getAllUploadInfo()
			case UPLOADGetUploadInfo:
				f.getTaskInfoWithTaskID(task)
			case UPLOADCancelTask:
				f.cancelCtx(task)
			case UPLOADCancelNodeTask:
				f.cancelNodeCtx(task)
			case UPLOADUpdateSavePath:
				f.updateSavePath(task)
			case UPLOADDeleteCompleteNum:
				f.deleteCompleteNum(task)
			case UPLOADGetDeleteTmpFile:
				f.getDeleteTmpFileState(task)
			case UPLOADPauseTask:
				f.pauseTask(task)
			case UPLOADAddBlock:
				f.addCompleteBlock(task)
			}
		}
	}
}

func (f *fileUploadManager) NewUpload(taskID, nodeID, srcPath, dstPath string, fileSize uint64, ctx context.Context, cancel context.CancelFunc, isCover bool, blockSize uint32, blockNum uint64, endBlockSize uint32, file *os.File) *public.UploadBaseInfo {
	task := &UploadTask{
		Mode:   UPLOADNew,
		TaskID: taskID,
		NodeID: nodeID,
		UploadInfo: UploadInfo{
			UploadBaseInfo: public.UploadBaseInfo{
				FileManagerInfo: &public.FileManagerInfo{
					TaskID:        taskID,
					State:         "",
					StartTime:     time.Time{},
					EndTime:       time.Time{},
					FileSize:      fileSize,
					NodeID:        nodeID,
					FailureReason: "",
					AbortReason:   "",
				},
				UploadToAgentPath: dstPath,
				LocalFilePath:     srcPath,
				IsCover:           isCover,
				BlockSize:         blockSize,
				EndBlockSize:      endBlockSize,
				TotalBlockNum:     blockNum,
			},
			file:          file,
			Cancel:        cancel,
			Ctx:           ctx,
			deleteTmpFile: true,
		},
	}
	f.uploadTaskChan <- task
	re := <-f.uploadResultChan
	return re.SingleUploadInfo
}

// GetFileData 读取指定任务的指定段文件内容
func (f *fileUploadManager) GetFileData(taskID string, blockNum uint64) (*protocol.BlockWithNum, error) {
	task := new(UploadTask)
	task.Mode = UPLOADGetFileData
	task.TaskID = taskID
	task.blockNum = blockNum
	f.uploadTaskChan <- task
	re := <-f.uploadResultChan
	if re.Error != nil {
		return nil, re.Error
	} else {
		return re.fileData, nil
	}
}

func (f *fileUploadManager) SetStateSuccess(taskID string) {
	task := new(UploadTask)
	task.Mode = UPLOADSetState
	task.TaskID = taskID
	task.State = UploadSuccess
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

func (f *fileUploadManager) SetStateUploading(taskID string) {
	task := new(UploadTask)
	task.Mode = UPLOADSetState
	task.TaskID = taskID
	task.State = UploadFileData
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

func (f *fileUploadManager) SetStateFail(taskID string, reason string) {
	task := new(UploadTask)
	task.Mode = UPLOADSetState
	task.TaskID = taskID
	task.State = UploadFail
	task.ErrorReason = reason
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

func (f *fileUploadManager) SetStateTerminate(taskID string, reason string) {
	task := new(UploadTask)
	task.Mode = UPLOADSetState
	task.TaskID = taskID
	task.State = UploadTerminate
	task.TerminateReason = reason
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

func (f *fileUploadManager) StopTask(taskID string) {
	task := new(UploadTask)
	task.Mode = UPLOADCancelTask
	task.TaskID = taskID
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

// StopNodeTask 停止指定节点上所有的文件上传任务，主要用在节点掉线场景
func (f *fileUploadManager) StopNodeTask(node string) {
	task := new(UploadTask)
	task.Mode = UPLOADCancelNodeTask
	task.NodeID = node
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

// GetTaskInfo 根据id获取文件上传信息，如果返回error了只可能是找不到指定id的任务的error
func (f *fileUploadManager) GetTaskInfo(taskID string) (*public.UploadBaseInfo, error) {
	task := new(UploadTask)
	task.Mode = UPLOADGetUploadInfo
	task.TaskID = taskID
	f.uploadTaskChan <- task
	re := <-f.uploadResultChan
	if re.Error != nil {
		return nil, re.Error
	} else {
		return re.SingleUploadInfo, nil
	}
}

func (f *fileUploadManager) InitBlock(taskID string, num uint64) {
	task := new(UploadTask)
	task.Mode = UPLOADAddBlock
	task.TaskID = taskID
	task.AddBlock = num
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

// GetNodeTaskInfo 获取指定节点的所有文件上传信息，如果返回error只能是没有找到对应节点的error
func (f *fileUploadManager) GetNodeTaskInfo(node string) ([]*public.UploadBaseInfo, error) {
	task := new(UploadTask)
	task.Mode = UPLOADGetNodeUploadInfo
	task.NodeID = node
	f.uploadTaskChan <- task
	re := <-f.uploadResultChan
	if re.Error != nil {
		return nil, re.Error
	} else {
		var sortInfo UploadInfoSort
		infoList := re.allTaskInfo[node]
		sortInfo = infoList
		sort.Sort(sortInfo)
		return sortInfo, nil
	}
}

// GetAllTaskInfo 获取所有文件上传任务记录
func (f *fileUploadManager) GetAllTaskInfo() map[string][]*public.UploadBaseInfo {
	task := new(UploadTask)
	task.Mode = UPLOADGetAllUploadInfo
	f.uploadTaskChan <- task
	re := <-f.uploadResultChan
	var sortComplete map[string][]*public.UploadBaseInfo
	for id, info := range re.allTaskInfo {
		var sortInfo UploadInfoSort
		sortInfo = info
		sort.Sort(sortInfo)
		sortComplete[id] = sortInfo
	}
	return sortComplete
}

// UpdateSavePath 更新实际保存文件路径
func (f *fileUploadManager) UpdateSavePath(taskID string, savePath string) {
	task := new(UploadTask)
	task.Mode = UPLOADUpdateSavePath
	task.TaskID = taskID
	task.SavePath = savePath
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

func (f *fileUploadManager) DeleteComplete(taskID string, num uint64) {
	task := new(UploadTask)
	task.Mode = UPLOADDeleteCompleteNum
	task.TaskID = taskID
	task.DeleteCompleteNum = num
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

func (f *fileUploadManager) GetDeleteTmpFileState(taskID string) bool {
	task := new(UploadTask)
	task.Mode = UPLOADGetDeleteTmpFile
	task.TaskID = taskID
	f.uploadTaskChan <- task
	re := <-f.uploadResultChan
	return re.Ok
}

func (f *fileUploadManager) PauseTask(taskID string) {
	task := new(UploadTask)
	task.Mode = UPLOADPauseTask
	task.TaskID = taskID
	f.uploadTaskChan <- task
	<-f.uploadResultChan
}

func (f *fileUploadManager) newUpload(task *UploadTask) {
	if _, ok := f.uploadInfo[task.NodeID]; !ok {
		f.uploadInfo[task.NodeID] = make(map[string]*UploadInfo)
	}
	f.uploadInfo[task.NodeID][task.TaskID] = &task.UploadInfo
	f.uploadInfo[task.NodeID][task.TaskID].State = SendBaseInfo
	f.uploadInfo[task.NodeID][task.TaskID].StartTime = time.Now()
	f.uploadIndex[task.TaskID] = task.NodeID
	f.uploadResultChan <- &UploadResult{SingleUploadInfo: &f.uploadInfo[task.NodeID][task.TaskID].UploadBaseInfo}
}

func (f *fileUploadManager) getTaskInfo(taskID string) (*UploadInfo, error) {
	if node, ok := f.uploadInfo[f.uploadIndex[taskID]]; ok {
		if info, ok2 := node[taskID]; ok2 {
			return info, nil
		} else {
			return nil, fmt.Errorf("upload task %v not exist", taskID)
		}
	} else {
		return nil, fmt.Errorf("upload task %v not exist", taskID)
	}
}

func (f *fileUploadManager) getFileData(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		var data []byte
		data, err = public.ReadFileBlock(info.file, info.BlockSize, info.EndBlockSize, info.TotalBlockNum, task.blockNum)
		if err != nil {
			f.uploadResultChan <- &UploadResult{Error: fmt.Errorf("get file block error:%v", err)}
		} else {
			info.CompleteBlock += 1
			f.uploadResultChan <- &UploadResult{fileData: &protocol.BlockWithNum{
				Num:  task.blockNum,
				Data: data,
			}}
		}
	} else {
		f.uploadResultChan <- &UploadResult{Error: err}
	}
}

func (f *fileUploadManager) addCompleteBlock(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		info.CompleteBlock = task.AddBlock
	}
	f.uploadResultChan <- &UploadResult{Ok: true}
}

func (f *fileUploadManager) deleteCompleteNum(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		info.CompleteBlock -= task.DeleteCompleteNum
	}
	f.uploadResultChan <- &UploadResult{Ok: true}
}

func (f *fileUploadManager) setState(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		info.State = task.State

		if task.State == UploadFail {
			info.FailureReason = task.ErrorReason
			info.EndTime = time.Now()
		}
		if task.State == UploadTerminate {
			info.AbortReason = task.TerminateReason
			info.EndTime = time.Now()
		}
		if task.State == UploadSuccess {
			info.EndTime = time.Now()
		}
	}
	f.uploadResultChan <- &UploadResult{Ok: true}
}

func (f *fileUploadManager) cancelCtx(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		info.Cancel()
	}
	f.uploadResultChan <- &UploadResult{Ok: true}
}

func (f *fileUploadManager) cancelNodeCtx(task *UploadTask) {
	if info, ok := f.uploadInfo[task.NodeID]; ok {
		for _, v := range info {
			v.Cancel()
		}
	}
	f.uploadResultChan <- &UploadResult{Ok: true}
}

func (f *fileUploadManager) getTaskInfoWithTaskID(task *UploadTask) {
	info, err := f.getTaskInfo(task.TaskID)
	if err != nil {
		f.uploadResultChan <- &UploadResult{Error: err}
	} else {
		f.uploadResultChan <- &UploadResult{SingleUploadInfo: &info.UploadBaseInfo}
	}
}

func (f *fileUploadManager) getNodeUploadInfo(task *UploadTask) {
	if node, ok := f.uploadInfo[task.NodeID]; ok {
		var re []*public.UploadBaseInfo
		for _, i := range node {
			re = append(re, &i.UploadBaseInfo)
		}
		reMap := make(map[string][]*public.UploadBaseInfo)
		reMap[task.NodeID] = re
		f.uploadResultChan <- &UploadResult{allTaskInfo: reMap}
	} else {
		f.uploadResultChan <- &UploadResult{Error: fmt.Errorf("node %v don't have upload task", task.NodeID)}
	}
}

func (f *fileUploadManager) updateSavePath(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		info.UploadToAgentPath = task.SavePath
	}
	f.uploadResultChan <- &UploadResult{Ok: true}
}

func (f *fileUploadManager) getDeleteTmpFileState(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		f.uploadResultChan <- &UploadResult{Ok: info.deleteTmpFile}
	} else {
		//任务都不存在了，就直接给删了
		f.uploadResultChan <- &UploadResult{Ok: true}
	}
}

func (f *fileUploadManager) pauseTask(task *UploadTask) {
	if info, err := f.getTaskInfo(task.TaskID); err == nil {
		info.deleteTmpFile = false
		info.Cancel()
	}
	f.uploadResultChan <- &UploadResult{Ok: true}
}

func (f *fileUploadManager) getAllUploadInfo() {
	var re map[string][]*public.UploadBaseInfo
	re = make(map[string][]*public.UploadBaseInfo)
	for nodeID, nodeInfo := range f.uploadInfo {
		for _, info := range nodeInfo {
			re[nodeID] = append(re[nodeID], &info.UploadBaseInfo)
		}
	}
	f.uploadResultChan <- &UploadResult{allTaskInfo: re}
}

func (f *fileUploadManager) NewUploadFormLocalFile(path string, isCover bool) (*FileUpload, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	re, err := f.newUploadTask(path, file, 0, isCover)
	return re, nil
}

func (f *fileUploadManager) NewUploadTask(path string, fileData *os.File, blockSize uint32, isCover bool) (*FileUpload, error) {
	return f.newUploadTask(path, fileData, blockSize, isCover)
}

func (f *fileUploadManager) newUploadTask(path string, fileData *os.File, blockSize uint32, isCover bool) (*FileUpload, error) {
	//根据文件信息计算分块大小、数量、最后一个分块大小
	fileInfo, err := fileData.Stat()
	if err != nil {
		return nil, err
	}
	if blockSize == 0 {
		blockSize = 30720
	}
	re := &FileUpload{
		fileData:       fileData,
		fileInfo:       fileInfo,
		fMgr:           f,
		blockSize:      blockSize,
		endBlockSize:   uint32(fileInfo.Size()) % blockSize,
		totalBlockNum:  uint64(fileInfo.Size())/uint64(blockSize) + 1,
		maxUploadCount: 50,
		isCover:        isCover,
		srcPath:        path,
	}
	re.fileHash, err = SHA256(path)
	return re, err
}

func (f *fileUploadManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case messageData := <-f.FileUploadMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						f.log.GeneralErrorf(" handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.FileMissSegmentInfo:
					var miss protocol.MissingSegment
					err := json.Unmarshal([]byte(mess.MissSegment), &miss)
					if err != nil {
						if channel, ok := f.FileUploadInfoRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block: nil,
								Error: err.Error(),
							}
						} else {
							f.log.GeneralErrorf("upload info res %v", ChannelClose)
						}
					} else {
						if channel, ok := f.FileUploadInfoRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block:    miss,
								Error:    mess.Err,
								FilePath: mess.FilePath,
							}
						} else {
							f.log.GeneralErrorf("upload info res %v", ChannelClose)
						}
					}
				case *protocol.CheckIntegrityRes:
					var miss protocol.MissingSegment
					err := json.Unmarshal([]byte(mess.MissSegment), &miss)
					if err != nil {
						if channel, ok := f.FileUploadEndRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block: nil,
								Error: err.Error(),
							}
						} else {
							f.log.GeneralErrorf("upload check size res %v", ChannelClose)
						}
					} else {
						if channel, ok := f.FileUploadEndRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block: miss,
								Error: mess.Err,
							}
						} else {
							f.log.GeneralErrorf("upload check size res %v", ChannelClose)
						}
					}
				case *protocol.ErrorMessage:
					if channel, ok := f.AgentWriteFileError.Load(mess.TaskID); ok {
						channel.(chan error) <- fmt.Errorf(mess.Err)
					} else {
						f.log.GeneralErrorf("upload agent write file error:%v %v", mess.Err, ChannelClose)
					}
				}
			}(messageData)
		}
	}
}

// UploadInfoSort 根据时间对返回的文件上传记录进行排序
type UploadInfoSort []*public.UploadBaseInfo

func (u UploadInfoSort) Len() int {
	return len(u)
}

func (u UploadInfoSort) Less(i, j int) bool {
	return u[i].StartTime.Before(u[j].StartTime)
}

func (u UploadInfoSort) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}
