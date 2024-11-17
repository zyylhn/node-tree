package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"io"
	"sync"
	"time"
)

const (
	LOADNewLoad = iota
	LOADUpdateState
	LOADReceive
	LOADTransfer
	LOADSetComplete       //执行成功并获取到了执行结果
	LOADGetSingleExecInfo //根据taskID获取当前节点指定任务的信息
	LOADCancel            //取消当前任务的ctx，也就是结束任务
	LOADDelete            //删除指定任务id的执行记录
	LOADSetBlockInfo      //设置返回结果的分段信息
	LOADCheckResult       //检查返回结果是否缺失分段
	LOADNodeAllTask       //停止指定指定节点上的所有任务
)

const (
	StateStart          = "start"           //开始（模块还没运行，正在准备阶段）
	StateUploadModule   = "upload module"   //上传模块
	StateRunning        = "running"         //正在运行
	StateDownLoadResult = "download result" //下载结果
	StateComplete       = "complete"        //完成
	StateFail           = "fail"            //失败
	StateAbort          = "abort"           //中止（断连等其他因素）

)

type LoadModuleBaseInfo struct {
	TaskId        string        `json:"task_id"`        //当前加载任务id
	NodeID        string        `json:"node_id"`        //节点id
	ModuleName    string        `json:"module_name"`    //模块名字，在plugin中定义
	State         string        `json:"state"`          //模块状态
	FailureReason string        `json:"failure_reason"` //失败原因
	AbortReason   string        `json:"abort_reason"`   //终止原因
	ModuleArgs    string        `json:"module_args"`    //模块的请求参数，也就是从远程地址读取到的，这里主动去读一次,存为json
	ModuleRes     []byte        `json:"module_res"`     //模块执行的结果，由远程模块返回
	StartTime     int64         `json:"start_time"`     //任务的创建时间
	EndTime       int64         `json:"end_time"`       //模块得到结果时间
	TimeConsuming time.Duration `json:"time_consuming"` //耗时
}

type LoadModuleBaseInfoStart struct {
	*LoadModuleBaseInfo
}

type LoadModuleBaseInfoEnd struct {
	*LoadModuleBaseInfo
}

type remoteLoadManager struct {
	LoadModuleMessChan chan *protocol.CompletePacket
	log                *public.NodeManagerLog
	t                  *topology.Topology

	loadModule  map[string]map[string]*LoadModuleInfo //所有运行过的远程模块的信息map[uuid][taskID]information
	moduleIndex map[string]string                     //map[taskID]uuid  记录这个任务在哪个节点

	LoadTaskChan chan *ModuleTask
	LoadResult   chan *ModuleResult

	SendTaskInfoRes  sync.Map //map[taskId]chan public.MissingBlockWithError 用来接收发送任务信息的响应
	SendDataEndRes   sync.Map //map[taskId]chan public.MissingBlockWithError 用来接收发送结束标志的响应包
	ModuleComplete   sync.Map //map[taskID]chan interface 用来接收模块执行的结果，interface里面的值为uint64（结果长度）或者string（错误原因）
	ReceiveModuleMap sync.Map //map[string]chan *protocol.BlockWithNum //map[taskID]chan result 	用来接收不同任务返回的结果
	DownLoadResult   sync.Map //返回执行的结果给handle
}

// LoadModuleInfo 远程模块的加载记录
type LoadModuleInfo struct {
	LoadModuleBaseInfo
	Cancel          context.CancelFunc //结束context远程加载整体流程
	Ctx             context.Context
	SyncResultBlock sync.Map //所有的分段信息
	BlockLength     uint64
	BlockSize       uint32 //分段的大小
	EndBlockSize    uint32 //最后一个分段的大小
	lock            sync.RWMutex
}

type ModuleTask struct {
	Mode          int    //操作类型
	UUID          string //目标节点的uuid
	TaskID        string //任务id
	ModuleName    string //模块名字
	State         string //状态，用于更新状态
	FailureReason string //失败原因
	End           bool   //标识传输的结束
	Data          []byte //分段传输回来的结果
	Error         string //回传结果的时候异常中断的原因
	Cancel        context.CancelFunc
	Ctx           context.Context
	BlockLen      uint64 //总计分了多少段
	BlockSize     uint32 //分段的大小
	EndBlockSize  uint32 //最后一个分段的大小
	BlockNum      uint64
	Args          string //模块请求参数
}

type ModuleResult struct {
	OK           bool   //是否执行成功
	Error        string //错误原因
	ModuleResult []byte
	SingleInfo   *LoadModuleBaseInfo
	TaskIDs      []string
	MissBlock    []uint64
}

func newLoadModuleManager(logInfo *public.NodeManagerLog, t *topology.Topology) *remoteLoadManager {
	manager := new(remoteLoadManager)
	manager.log = logInfo
	manager.t = t
	manager.LoadModuleMessChan = make(chan *protocol.CompletePacket, 5)
	manager.loadModule = make(map[string]map[string]*LoadModuleInfo)
	manager.LoadResult = make(chan *ModuleResult)
	manager.LoadTaskChan = make(chan *ModuleTask)
	manager.moduleIndex = make(map[string]string)
	manager.SendDataEndRes = sync.Map{}
	manager.ModuleComplete = sync.Map{}
	manager.ReceiveModuleMap = sync.Map{}
	manager.SendTaskInfoRes = sync.Map{}
	manager.DownLoadResult = sync.Map{}
	return manager
}

func (l *remoteLoadManager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-l.LoadTaskChan:
			switch task.Mode {
			case LOADNewLoad:
				l.newLoad(task)
			case LOADUpdateState:
				l.updateState(task)
			case LOADTransfer:
				go l.transferModule(task)
			case LOADReceive:
				l.receiveModule(task)
			case LOADSetComplete:
				l.setComplete(task)
			case LOADGetSingleExecInfo:
				l.getSingleInfo(task)
			case LOADDelete:
				l.deleteRecord(task)
			case LOADCancel:
				l.cancelCtx(task)
			case LOADSetBlockInfo:
				l.setResultBlockInfo(task)
			case LOADCheckResult:
				l.checkResult(task)
			case LOADNodeAllTask:
				l.stopNodeAllTask(task)
			}
		}
	}
}

func (l *remoteLoadManager) NewLoad(uuid, taskID string, moduleType string, args string, cancel *context.CancelFunc, ctx *context.Context) *LoadModuleBaseInfo {
	task := new(ModuleTask)
	task.Mode = LOADNewLoad
	task.UUID = uuid
	task.TaskID = taskID
	task.ModuleName = moduleType
	task.Args = args
	task.Cancel = *cancel
	task.Ctx = *ctx
	l.LoadTaskChan <- task
	re := <-l.LoadResult
	return re.SingleInfo
}

func (l *remoteLoadManager) StateRunning(taskID string) {
	l.setState(taskID, StateRunning, "")
}
func (l *remoteLoadManager) StateAbort(taskID string, reason string) {
	l.setState(taskID, StateAbort, reason)
}
func (l *remoteLoadManager) StateFail(taskID, reason string) {
	l.setState(taskID, StateFail, reason)
}

func (l *remoteLoadManager) StateUpload(taskID string) {
	l.setState(taskID, StateUploadModule, "")
}

func (l *remoteLoadManager) StateDownload(taskID string) {
	l.setState(taskID, StateDownLoadResult, "")
}

// Transfer 在协程中不断读取handle发来的模块数据
func (l *remoteLoadManager) Transfer(taskID string) {
	task := new(ModuleTask)
	task.Mode = LOADTransfer
	task.TaskID = taskID
	task.UUID = l.moduleIndex[taskID]
	l.LoadTaskChan <- task
}

// ReceiveModule 不断接收数据写入对应的channel
func (l *remoteLoadManager) ReceiveModule(taskID string, data []byte, blockNum uint64, end bool) {
	task := &ModuleTask{TaskID: taskID, Data: data, UUID: l.moduleIndex[taskID], End: end, Mode: LOADReceive, BlockNum: blockNum}
	l.LoadTaskChan <- task
	<-l.LoadResult
}

// SetComplete 设置结束时间，并计算出花费时间，并状态为完成
func (l *remoteLoadManager) SetComplete(taskID string) {
	task := new(ModuleTask)
	task.Mode = LOADSetComplete
	task.UUID = l.moduleIndex[taskID]
	task.TaskID = taskID
	l.LoadTaskChan <- task
	<-l.LoadResult
}

func (l *remoteLoadManager) GetSingleInfo(taskID string) *LoadModuleBaseInfo {
	task := new(ModuleTask)
	task.Mode = LOADGetSingleExecInfo
	task.UUID = l.moduleIndex[taskID]
	task.TaskID = taskID
	l.LoadTaskChan <- task
	result := <-l.LoadResult
	if result.OK {
		return result.SingleInfo
	} else {
		return nil
	}
}

func (l *remoteLoadManager) SetResultBlockInfo(uuid, taskID string, len uint64, blockSize, endBlockSize uint32) error {
	task := new(ModuleTask)
	task.Mode = LOADSetBlockInfo
	task.UUID = uuid
	task.TaskID = taskID
	task.BlockLen = len
	task.BlockSize = blockSize
	task.EndBlockSize = endBlockSize
	l.LoadTaskChan <- task
	result := <-l.LoadResult
	if result.OK {
		return nil
	} else {
		return errors.New(result.Error)
	}
}

func (l *remoteLoadManager) CheckResultSize(uuid, taskID string) (protocol.MissingSegment, error) {
	task := new(ModuleTask)
	task.Mode = LOADCheckResult
	task.UUID = uuid
	task.TaskID = taskID
	l.LoadTaskChan <- task
	result := <-l.LoadResult
	if result.OK {
		return result.MissBlock, nil
	} else {
		return nil, errors.New(result.Error)
	}
}

func (l *remoteLoadManager) newLoad(task *ModuleTask) {
	if _, ok := l.loadModule[task.UUID]; !ok {
		l.loadModule[task.UUID] = make(map[string]*LoadModuleInfo)
	}
	startTime := time.Now()
	l.loadModule[task.UUID][task.TaskID] = new(LoadModuleInfo)
	l.loadModule[task.UUID][task.TaskID].ModuleName = task.ModuleName
	l.loadModule[task.UUID][task.TaskID].State = StateStart
	l.loadModule[task.UUID][task.TaskID].StartTime = startTime.Unix()
	l.moduleIndex[task.TaskID] = task.UUID //记录索引
	l.loadModule[task.UUID][task.TaskID].TaskId = task.TaskID
	l.loadModule[task.UUID][task.TaskID].NodeID = task.UUID
	l.loadModule[task.UUID][task.TaskID].Cancel = task.Cancel
	l.loadModule[task.UUID][task.TaskID].ModuleArgs = task.Args
	l.loadModule[task.UUID][task.TaskID].Ctx = task.Ctx
	l.loadModule[task.UUID][task.TaskID].lock = sync.RWMutex{}
	l.loadModule[task.UUID][task.TaskID].SyncResultBlock = sync.Map{}

	l.log.LoadModuleDebugf("[load manager taskID:%v]Create a new remote load module %v on node %v", task.TaskID, task.UUID, task.ModuleName)

	l.LoadResult <- &ModuleResult{SingleInfo: &l.loadModule[task.UUID][task.TaskID].LoadModuleBaseInfo}
}

func (l *remoteLoadManager) updateState(task *ModuleTask) {
	if node, ok := l.loadModule[task.UUID]; ok {
		if module, ok2 := node[task.TaskID]; ok2 {
			module.State = task.State
			if task.State == StateFail {
				module.FailureReason = task.FailureReason
				module.State = StateFail
				module.EndTime = time.Now().Unix()
				module.TimeConsuming = time.Duration(module.EndTime-module.StartTime) * time.Second
				l.log.LoadModuleDebugf("[load manager taskID:%v]set task state fail:%v", task.TaskID, task.FailureReason)
			} else if task.State == StateAbort {
				module.AbortReason = task.FailureReason
				module.State = StateAbort
				module.EndTime = time.Now().Unix()
				module.TimeConsuming = time.Duration(module.EndTime-module.StartTime) * time.Second
				l.log.LoadModuleDebugf("[load manager taskID:%v]set task state abort:%v", task.TaskID, task.FailureReason)
			} else {
				module.State = task.State
				l.log.LoadModuleInfof("[load manager taskID:%v]set task state %v", task.TaskID, task.State)

			}
		}
	}
	l.LoadResult <- &ModuleResult{OK: true}
}

func (l *remoteLoadManager) setResultBlockInfo(task *ModuleTask) {
	if node, ok := l.loadModule[task.UUID]; ok {
		if _, ok2 := node[task.TaskID]; ok2 {
			l.loadModule[task.UUID][task.TaskID].BlockSize = task.BlockSize
			l.loadModule[task.UUID][task.TaskID].EndBlockSize = task.EndBlockSize
			l.loadModule[task.UUID][task.TaskID].BlockLength = task.BlockLen
			for i := uint64(0); i < task.BlockLen; i++ {
				l.loadModule[task.UUID][task.TaskID].SyncResultBlock.Store(i, nil)
			}
			l.LoadResult <- &ModuleResult{OK: true}
		} else {
			l.LoadResult <- &ModuleResult{OK: false, Error: fmt.Sprintf("task %v is not loaded on node %v", task.TaskID, task.UUID)}
		}
	} else {
		l.LoadResult <- &ModuleResult{OK: false, Error: fmt.Sprintf("no loading task exists on node %v", task.UUID)}
	}
}

func (l *remoteLoadManager) checkResult(task *ModuleTask) {
	l.loadModule[task.UUID][task.TaskID].lock.RLock()
	if node, ok := l.loadModule[task.UUID]; ok {
		if module, ok2 := node[task.TaskID]; ok2 {
			var miss protocol.MissingSegment
			module.SyncResultBlock.Range(func(key, value interface{}) bool {
				if value == nil {
					miss = append(miss, key.(uint64))
				} else if uint32(len(value.([]byte))) != module.BlockSize {
					if uint32(len(value.([]byte))) != module.EndBlockSize {
						miss = append(miss, key.(uint64))
					} else if key.(uint64) != module.BlockLength-1 {
						miss = append(miss, key.(uint64))
					}
				}
				return true
			})
			if len(miss) > 0 {
				l.log.LoadModuleDebugf("[load manager taskID:%v]Check module run data result Warning: Module run result return total number of segments: %v, length of each segment: %v, length of the last segment: %v, now missing segment number: %v", task.TaskID, module.BlockLength, module.BlockSize, module.EndBlockSize, miss)
			} else {
				l.log.LoadModuleDebugf("[load manager taskID:%v]Check module running result data successfully: module running result returned total number of segments: %v, length of each segment: %v, length of the last segment: %v", task.TaskID, module.BlockLength, module.BlockSize, module.EndBlockSize)
			}
			l.LoadResult <- &ModuleResult{OK: true, MissBlock: miss}
		} else {
			l.LoadResult <- &ModuleResult{OK: false, Error: fmt.Sprintf("task %v is not loaded on node %v", task.TaskID, task.UUID)}
		}
	} else {
		l.LoadResult <- &ModuleResult{OK: false, Error: fmt.Sprintf("no loading task exists on node %v", task.UUID)}
	}
	l.loadModule[task.UUID][task.TaskID].lock.RUnlock()
}

func (l *remoteLoadManager) setState(taskID, state, reason string) {
	task := new(ModuleTask)
	task.Mode = LOADUpdateState
	task.UUID = l.moduleIndex[taskID]
	task.TaskID = taskID
	task.State = state
	task.FailureReason = reason
	l.LoadTaskChan <- task
	<-l.LoadResult
}

// 以协程的方式运行，将receiveModule中接收到的数据写到对应的远程模块中
func (l *remoteLoadManager) transferModule(task *ModuleTask) {
	l.ReceiveModuleMap.Store(task.TaskID, make(chan *protocol.BlockWithNum, 10))

	defer func() {
		l.ReceiveModuleMap.Delete(task.TaskID)
	}()
	channel, ok1 := l.ReceiveModuleMap.Load(task.TaskID)
	if !ok1 {
		return
	}
	for {
		select {
		case data, ok := <-channel.(chan *protocol.BlockWithNum):
			if ok {
				l.loadModule[task.UUID][task.TaskID].SyncResultBlock.Store(data.Num, data.Data)
			} else {
				//说明此时接收结束
				return
			}
		case <-l.loadModule[l.moduleIndex[task.TaskID]][task.TaskID].Ctx.Done():
			return
		}
	}
}

// 从handle中接收data写到ReceiveModule中
func (l *remoteLoadManager) receiveModule(task *ModuleTask) {
	if channel, ok := l.ReceiveModuleMap.Load(task.TaskID); ok {
		if task.End {
			close(channel.(chan *protocol.BlockWithNum))
		} else {
			channel.(chan *protocol.BlockWithNum) <- &protocol.BlockWithNum{
				Num:  task.BlockNum,
				Data: task.Data,
			}
		}
	}
	l.LoadResult <- &ModuleResult{OK: true}
}

func (l *remoteLoadManager) setComplete(task *ModuleTask) {
	if node, ok := l.loadModule[task.UUID]; ok {
		if module, ok2 := node[task.TaskID]; ok2 {
			module.EndTime = time.Now().Unix()
			module.TimeConsuming = time.Duration(module.EndTime-module.StartTime) * time.Second
			module.State = StateComplete
			for i := uint64(0); i < l.loadModule[task.UUID][task.TaskID].BlockLength; i++ {
				d, _ := l.loadModule[task.UUID][task.TaskID].SyncResultBlock.Load(i)
				l.loadModule[task.UUID][task.TaskID].ModuleRes = append(l.loadModule[task.UUID][task.TaskID].ModuleRes, d.([]byte)...)
				l.log.LoadModuleDebugf("[load manager taskID:%v]Read from the result segment and concatenate the full result, %v segment, data length :%v", task.TaskID, i+1, len(d.([]byte)))
			}
			l.log.LoadModuleDebugf("[load manager taskID:%v]module load result:%v", task.TaskID, string(l.loadModule[task.UUID][task.TaskID].ModuleRes))
			module.EndTime = time.Now().Unix()
			module.TimeConsuming = time.Duration(module.EndTime-module.StartTime) * time.Second
			module.State = StateComplete
		}
	}
	l.LoadResult <- &ModuleResult{OK: true}
}

// 获取指定节点指定任务的任务信息
func (l *remoteLoadManager) getSingleInfo(task *ModuleTask) {
	if node, ok := l.loadModule[l.moduleIndex[task.TaskID]]; ok {
		if module, ok2 := node[task.TaskID]; ok2 {
			l.LoadResult <- &ModuleResult{OK: true, SingleInfo: &module.LoadModuleBaseInfo}
		} else {
			l.LoadResult <- &ModuleResult{OK: false}
		}
	} else {
		l.LoadResult <- &ModuleResult{OK: false}
	}
}

func (l *remoteLoadManager) cancelCtx(task *ModuleTask) {
	if node, ok := l.loadModule[task.UUID]; ok {
		if module, ok2 := node[task.TaskID]; ok2 {
			module.Cancel()
		}
	}
	l.LoadResult <- &ModuleResult{OK: true}
}

// CancelCtx 终止当前任务，给外部调用的接口
func (l *remoteLoadManager) CancelCtx(taskID string) {
	task := new(ModuleTask)
	task.Mode = LOADCancel
	task.UUID = l.moduleIndex[taskID]
	task.TaskID = taskID
	l.LoadTaskChan <- task
	<-l.LoadResult
}

func (l *remoteLoadManager) stopNodeAllTask(task *ModuleTask) {
	if _, ok := l.loadModule[task.UUID]; ok {
		for _, module := range l.loadModule[task.UUID] {
			module.Cancel()
		}
	}
	l.LoadResult <- &ModuleResult{OK: true}
}

func (l *remoteLoadManager) StopNodeAllTask(uuid string) {
	task := new(ModuleTask)
	task.Mode = LOADNodeAllTask
	task.UUID = uuid
	l.LoadTaskChan <- task
	<-l.LoadResult
}

func (l *remoteLoadManager) deleteRecord(task *ModuleTask) {
	if node, ok := l.loadModule[task.UUID]; ok {
		if module, ok2 := node[task.TaskID]; ok2 {
			//判断状态，如果没有完成需要告知需要先终止任务在删除记录
			select {
			//ctx结束说明任务结束
			case <-module.Ctx.Done():
				//删除指定任务记录
				delete(node, task.TaskID)
				//删除索引记录
				delete(l.moduleIndex, task.TaskID)
				//当前节点为没有远程加载的任务
				if len(node) == 0 {
					delete(l.loadModule, task.UUID)
				}
				l.LoadResult <- &ModuleResult{OK: true}
			default:
				//没结束就返回警告
				l.LoadResult <- &ModuleResult{OK: false, Error: "the module is running, wait for the run to end or manually terminate before deleting it:" + module.State}
			}
		} else {
			l.LoadResult <- &ModuleResult{OK: false, Error: "non-existent task id:" + task.TaskID}
		}
	} else {
		l.LoadResult <- &ModuleResult{OK: false, Error: "non-existent node id:" + task.UUID}
	}
}

// DeleteRecord 删除当前任务Id的执行记录，进行中的任务无法删除，只能删除已经完成的：终止、异常、完成状态的任务
func (l *remoteLoadManager) DeleteRecord(taskID string) {
	task := new(ModuleTask)
	task.Mode = LOADDelete
	task.UUID = l.moduleIndex[taskID]
	task.TaskID = taskID
	l.LoadTaskChan <- task
	result := <-l.LoadResult
	if result.OK {
		l.log.LoadModuleDebug(fmt.Sprintf("[load manager taskID:%v]Removes the module load record from memory", taskID))
	} else {
		l.log.LoadModuleError(fmt.Sprintf("[load manager taskID:%v]Failed to delete module load record from memory :%v", taskID, result.Error))
	}
}

//todo 把cancel去掉！！！

func (l *remoteLoadManager) NewRemoteLoad(module io.ReadCloser, args string, moduleName string, ctx *context.Context, c *context.CancelFunc) (*RemoteLoad, error) {
	load := new(RemoteLoad)
	load.args = args
	load.moduleName = moduleName
	load.taskID = utils.GenerateUUID()
	load.moduleData = make(map[uint64][]byte)
	load.blockSize = 30720
	load.maxUploadCount = 50
	load.t = l.t
	load.mgr = l

	i := uint64(0)
	defer func(module io.ReadCloser) {
		_ = module.Close()
	}(module)
	for {
		buf := make([]byte, load.blockSize)
		n, err := module.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		load.moduleData[i] = buf[:n]
		i++
	}
	if i != 0 {
		load.endBlockSize = len(load.moduleData[i-1])
	}
	load.ctx = *ctx
	load.cancel = *c
	return load, nil
}

func (l *remoteLoadManager) Dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-l.LoadModuleMessChan:
			func(packet *protocol.CompletePacket) {
				defer func() {
					if r := recover(); r != nil {
						l.log.GeneralErrorf("load handle panic:%v", r)
					}
				}()
				switch mess := packet.ReqBody.(type) {
				case *protocol.FileMissSegmentInfo:
					var miss protocol.MissingSegment
					err := json.Unmarshal([]byte(mess.MissSegment), &miss)
					if err != nil {
						if channel, ok := l.SendTaskInfoRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block: nil,
								Error: err.Error(),
							}
						} else {
							l.log.GeneralErrorf("get module info res %v", ChannelClose)
						}
					} else {
						if channel, ok := l.SendTaskInfoRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block: miss,
								Error: mess.Err,
							}
						} else {
							l.log.GeneralErrorf("get module info res %v", ChannelClose)
						}
					}
				case *protocol.CheckIntegrityRes:
					var miss protocol.MissingSegment
					err := json.Unmarshal([]byte(mess.MissSegment), &miss)
					if err != nil {
						if channel, ok := l.SendDataEndRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block: nil,
								Error: err.Error(),
							}
						} else {
							l.log.GeneralErrorf("load module check module size %v", ChannelClose)
						}
					} else {
						if channel, ok := l.SendDataEndRes.Load(mess.TaskID); ok {
							channel.(chan *public.MissingBlockWithError) <- &public.MissingBlockWithError{
								Block: miss,
								Error: mess.Err,
							}
						} else {
							l.log.GeneralErrorf("load module check module size %v", ChannelClose)
						}
					}
				case *protocol.ModuleResultInfo:
					//执行结果回来了，准备接收回来的结果,将结果写入到channel中，给handle处理
					if channel, ok := l.ModuleComplete.Load(mess.TaskID); ok {
						channel.(chan *protocol.ModuleResultInfo) <- mess
					} else {
						l.log.GeneralErrorf("load module receive result info %v", ChannelClose)
					}
				case *protocol.FileManagerFileData:
					l.ReceiveModule(mess.TaskID, mess.Data, mess.SegmentNum, false)
				case *protocol.FileManagerTransferEnd:
					channel, ok := l.DownLoadResult.Load(mess.TaskID)
					if ok {
						channel.(chan string) <- mess.Err
					} else {
						l.log.GeneralErrorf("load module return result %v", ChannelClose)
					}
				}
			}(msg)
		}
	}
}
