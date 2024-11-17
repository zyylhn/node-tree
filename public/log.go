package public

import (
	"github.com/kataras/golog"
	"github.com/zyylhn/node-tree/utils"
	"sync"
	"time"
)

const (
	General = iota + 1
	LoadModule
	BackWard
	Forward
	FileUpload
	FileDownload
)

type NodeManagerLog struct {
	GeneralLogger      *golog.Logger
	LoadModuleLogger   *golog.Logger
	BackWardLogger     *golog.Logger
	ForwardLogger      *golog.Logger
	FileUploadLogger   *golog.Logger
	FileDownloadLogger *golog.Logger

	writeToken *utils.TokenBucket //可以排队写入的数量：当写入时间较长可以缓存一定数量的日志准备写入。
	logLock    sync.Mutex         //同时在写的只能有一个，其他通过协程阻塞
}

func NewNodeManagerLog(log *golog.Logger, writeQueueNum int) *NodeManagerLog {
	re := new(NodeManagerLog)
	re.GeneralLogger = log.Clone().SetPrefix("<general> ")
	re.LoadModuleLogger = log.Clone().SetPrefix("<load_module> ")
	re.BackWardLogger = log.Clone().SetPrefix("<backward> ")
	re.ForwardLogger = log.Clone().SetPrefix("<forward> ")
	re.FileUploadLogger = log.Clone().SetPrefix("<file_upload> ")
	re.FileDownloadLogger = log.Clone().SetPrefix("<file_download> ")
	re.writeToken = utils.NewTokenBucket(writeQueueNum, time.Minute)
	re.logLock = sync.Mutex{}
	return re
}

func (n *NodeManagerLog) GeneralLogf(level golog.Level, format string, args ...interface{}) {
	n.Logf(General, level, format, args...)
}

func (n *NodeManagerLog) GeneralDebugf(format string, args ...interface{}) {
	n.GeneralLogf(golog.DebugLevel, format, args...)
}

func (n *NodeManagerLog) GeneralInfof(format string, args ...interface{}) {
	n.GeneralLogf(golog.InfoLevel, format, args...)
}

func (n *NodeManagerLog) GeneralWarnf(format string, args ...interface{}) {
	n.GeneralLogf(golog.WarnLevel, format, args...)
}

func (n *NodeManagerLog) GeneralErrorf(format string, args ...interface{}) {
	n.GeneralLogf(golog.ErrorLevel, format, args...)
}

func (n *NodeManagerLog) GeneralLog(level golog.Level, args ...interface{}) {
	n.Log(General, level, args...)
}

func (n *NodeManagerLog) GeneralDebug(args ...interface{}) {
	n.GeneralLog(golog.DebugLevel, args...)
}

func (n *NodeManagerLog) GeneralInfo(args ...interface{}) {
	n.GeneralLog(golog.InfoLevel, args...)
}

func (n *NodeManagerLog) GeneralWarn(args ...interface{}) {
	n.GeneralLog(golog.WarnLevel, args...)
}

func (n *NodeManagerLog) GeneralError(args ...interface{}) {
	n.GeneralLog(golog.ErrorLevel, args...)
}

func (n *NodeManagerLog) LoadModuleLogf(level golog.Level, format string, args ...interface{}) {
	n.Logf(LoadModule, level, format, args...)
}

func (n *NodeManagerLog) LoadModuleDebugf(format string, args ...interface{}) {
	n.LoadModuleLogf(golog.DebugLevel, format, args...)
}

func (n *NodeManagerLog) LoadModuleInfof(format string, args ...interface{}) {
	n.LoadModuleLogf(golog.InfoLevel, format, args...)
}

func (n *NodeManagerLog) LoadModuleWarnf(format string, args ...interface{}) {
	n.LoadModuleLogf(golog.WarnLevel, format, args...)
}

func (n *NodeManagerLog) LoadModuleErrorf(format string, args ...interface{}) {
	n.LoadModuleLogf(golog.ErrorLevel, format, args...)
}

func (n *NodeManagerLog) LoadModuleLog(level golog.Level, args ...interface{}) {
	n.Log(LoadModule, level, args...)
}

func (n *NodeManagerLog) LoadModuleDebug(args ...interface{}) {
	n.LoadModuleLog(golog.DebugLevel, args...)
}

func (n *NodeManagerLog) LoadModuleInfo(args ...interface{}) {
	n.LoadModuleLog(golog.InfoLevel, args...)
}

func (n *NodeManagerLog) LoadModuleWarn(args ...interface{}) {
	n.LoadModuleLog(golog.WarnLevel, args...)
}

func (n *NodeManagerLog) LoadModuleError(args ...interface{}) {
	n.LoadModuleLog(golog.ErrorLevel, args...)
}

func (n *NodeManagerLog) BackWardLogf(level golog.Level, format string, args ...interface{}) {
	n.Logf(BackWard, level, format, args...)
}

func (n *NodeManagerLog) BackWardDebugf(format string, args ...interface{}) {
	n.BackWardLogf(golog.DebugLevel, format, args...)
}

func (n *NodeManagerLog) BackWardInfof(format string, args ...interface{}) {
	n.BackWardLogf(golog.InfoLevel, format, args...)
}

func (n *NodeManagerLog) BackWardWarnf(format string, args ...interface{}) {
	n.BackWardLogf(golog.WarnLevel, format, args...)
}

func (n *NodeManagerLog) BackWardErrorf(format string, args ...interface{}) {
	n.BackWardLogf(golog.ErrorLevel, format, args...)
}

func (n *NodeManagerLog) BackWardLog(level golog.Level, args ...interface{}) {
	n.Log(BackWard, level, args...)
}

func (n *NodeManagerLog) BackWardDebug(args ...interface{}) {
	n.BackWardLog(golog.DebugLevel, args...)
}

func (n *NodeManagerLog) BackWardInfo(args ...interface{}) {
	n.BackWardLog(golog.InfoLevel, args...)
}

func (n *NodeManagerLog) BackWardWarn(args ...interface{}) {
	n.BackWardLog(golog.WarnLevel, args...)
}

func (n *NodeManagerLog) BackWardError(args ...interface{}) {
	n.BackWardLog(golog.ErrorLevel, args...)
}

func (n *NodeManagerLog) ForwardLogf(level golog.Level, format string, args ...interface{}) {
	n.Logf(Forward, level, format, args...)
}

func (n *NodeManagerLog) ForWardDebugf(format string, args ...interface{}) {
	n.ForwardLogf(golog.DebugLevel, format, args...)
}

func (n *NodeManagerLog) ForWardInfof(format string, args ...interface{}) {
	n.ForwardLogf(golog.InfoLevel, format, args...)
}

func (n *NodeManagerLog) ForWardWarnf(format string, args ...interface{}) {
	n.ForwardLogf(golog.WarnLevel, format, args...)
}

func (n *NodeManagerLog) ForWardErrorf(format string, args ...interface{}) {
	n.ForwardLogf(golog.ErrorLevel, format, args...)
}

func (n *NodeManagerLog) ForwardLog(level golog.Level, args ...interface{}) {
	n.Log(Forward, level, args...)
}

func (n *NodeManagerLog) ForWardDebug(args ...interface{}) {
	n.ForwardLog(golog.DebugLevel, args...)
}

func (n *NodeManagerLog) ForWardInfo(args ...interface{}) {
	n.ForwardLog(golog.InfoLevel, args...)
}

func (n *NodeManagerLog) ForWardWarn(args ...interface{}) {
	n.ForwardLog(golog.WarnLevel, args...)
}

func (n *NodeManagerLog) ForWardError(args ...interface{}) {
	n.ForwardLog(golog.ErrorLevel, args...)
}

func (n *NodeManagerLog) FileUploadLogf(level golog.Level, format string, args ...interface{}) {
	n.Logf(FileUpload, level, format, args...)
}

func (n *NodeManagerLog) FileUploadDebugf(format string, args ...interface{}) {
	n.FileUploadLogf(golog.DebugLevel, format, args...)
}

func (n *NodeManagerLog) FileUploadInfof(format string, args ...interface{}) {
	n.FileUploadLogf(golog.InfoLevel, format, args...)
}

func (n *NodeManagerLog) FileUploadWarnf(format string, args ...interface{}) {
	n.FileUploadLogf(golog.WarnLevel, format, args...)
}

func (n *NodeManagerLog) FileUploadErrorf(format string, args ...interface{}) {
	n.FileUploadLogf(golog.ErrorLevel, format, args...)
}

func (n *NodeManagerLog) FileUploadLog(level golog.Level, args ...interface{}) {
	n.Log(FileUpload, level, args...)
}

func (n *NodeManagerLog) FileUploadDebug(args ...interface{}) {
	n.FileUploadLog(golog.DebugLevel, args...)
}

func (n *NodeManagerLog) FileUploadInfo(args ...interface{}) {
	n.FileUploadLog(golog.InfoLevel, args...)
}

func (n *NodeManagerLog) FileUploadWarn(args ...interface{}) {
	n.FileUploadLog(golog.WarnLevel, args...)
}

func (n *NodeManagerLog) FileUploadError(args ...interface{}) {
	n.FileUploadLog(golog.ErrorLevel, args...)
}

func (n *NodeManagerLog) FileDownloadLogf(level golog.Level, format string, args ...interface{}) {
	n.Logf(FileDownload, level, format, args...)
}

func (n *NodeManagerLog) FileDownloadDebugf(format string, args ...interface{}) {
	n.FileDownloadLogf(golog.DebugLevel, format, args...)
}

func (n *NodeManagerLog) FileDownloadInfof(format string, args ...interface{}) {
	n.FileDownloadLogf(golog.InfoLevel, format, args...)
}

func (n *NodeManagerLog) FileDownloadWarnf(format string, args ...interface{}) {
	n.FileDownloadLogf(golog.WarnLevel, format, args...)
}

func (n *NodeManagerLog) FileDownloadErrorf(format string, args ...interface{}) {
	n.FileDownloadLogf(golog.ErrorLevel, format, args...)
}

func (n *NodeManagerLog) FileDownloadLog(level golog.Level, args ...interface{}) {
	n.Log(FileDownload, level, args...)
}

func (n *NodeManagerLog) FileDownloadDebug(args ...interface{}) {
	n.FileDownloadLog(golog.DebugLevel, args...)
}

func (n *NodeManagerLog) FileDownloadInfo(args ...interface{}) {
	n.FileDownloadLog(golog.InfoLevel, args...)
}

func (n *NodeManagerLog) FileDownloadWarn(args ...interface{}) {
	n.FileDownloadLog(golog.WarnLevel, args...)
}

func (n *NodeManagerLog) FileDownloadError(args ...interface{}) {
	n.FileDownloadLog(golog.ErrorLevel, args...)
}

func (n *NodeManagerLog) Logf(module int, level golog.Level, format string, args ...interface{}) {
	n.logf(module, level, format, args...)
}

func (n *NodeManagerLog) Log(module int, level golog.Level, args ...interface{}) {
	n.log(module, level, args...)
}

func (n *NodeManagerLog) logf(module int, level golog.Level, format string, args ...interface{}) {
	err := n.writeToken.Take()
	if err != nil {
		golog.Errorf("输出日志出现阻塞，导致这种情况的原因是日志保存速度过慢或者阻塞，请联系开发者定位问题，此条日志将跳过不进行保存，详细报错:%v。备份日志内容:level:%v,data:%s", err, level, args)
		return
	}
	defer n.writeToken.Release()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		n.logLock.Lock()
		defer n.logLock.Unlock()
		switch module {
		case General:
			n.GeneralLogger.Logf(level, format, args...)
		case LoadModule:
			n.LoadModuleLogger.Logf(level, format, args...)
		case BackWard:
			n.BackWardLogger.Logf(level, format, args...)
		case Forward:
			n.ForwardLogger.Logf(level, format, args...)
		case FileUpload:
			n.FileUploadLogger.Logf(level, format, args...)
		case FileDownload:
			n.FileDownloadLogger.Logf(level, format, args...)
		}
	}()
	wg.Wait()
}

func (n *NodeManagerLog) log(module int, level golog.Level, args ...interface{}) {
	err := n.writeToken.Take()
	if err != nil {
		golog.Errorf("输出日志出现阻塞，导致这种情况的原因是日志保存速度过慢或者阻塞，请联系开发者定位问题，此条日志将跳过不进行保存，详细报错:%v。备份日志内容:level:%v,data:%s", err, level, args)
		return
	}
	defer n.writeToken.Release()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		n.logLock.Lock()
		defer n.logLock.Unlock()
		switch module {
		case General:
			n.GeneralLogger.Log(level, args...)
		case LoadModule:
			n.LoadModuleLogger.Log(level, args...)
		case BackWard:
			n.BackWardLogger.Log(level, args...)
		case Forward:
			n.ForwardLogger.Log(level, args...)
		case FileUpload:
			n.FileUploadLogger.Log(level, args...)
		case FileDownload:
			n.FileDownloadLogger.Log(level, args...)
		}
	}()
	wg.Wait()
}

// Close 关闭令牌桶中channel
func (n *NodeManagerLog) Close() {
	n.writeToken.Close()
}
