package public

import (
	"fmt"
	"github.com/zyylhn/node-tree/protocol"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

//文件上传下载agent和admin共用的函数

const (
	FileStart    = iota //文件传输开始
	FileAdd             //完成传输一段长度的文件
	FileDelete          //为什么要有这个删除呢，因为admin在传输过成功将文件发送出去就判定为传输成功，但是最后agent在检查的时候可能会有缺失，所以这时候就会重新传输，所以要减去一部分
	FileComplete        //传输完成
)

// Status 文件传输状态的标记
type Status struct {
	Stat   int
	Scale  int64
	TaskID string
}

// UploadFileStart 节点控制系统的消息通知
type UploadFileStart struct {
	*UploadBaseInfo
}

// UploadFileEnd 节点控制系统的消息通知
type UploadFileEnd struct {
	*UploadBaseInfo
}

type UploadBaseInfo struct {
	*FileManagerInfo
	//IsLocalFile       bool   //文件来源，是本地文件还是通过io.readerCloser直接传输进来的
	UploadToAgentPath string //上传到agent的保存路径
	LocalFilePath     string //当IsLocalFile为true的时候为admin本地的文件路径，false的话为空
	IsCover           bool
	BlockSize         uint32 //发送的分块大小
	EndBlockSize      uint32 //最后一段分块的大小
	TotalBlockNum     uint64 //分块数量
	CompleteBlock     uint64
}

// FileManagerInfo 文件上传下载的基本信息
type FileManagerInfo struct {
	TaskID        string    `json:"task_id"`
	State         string    `json:"state"` //上传下载的状态（不是上面的文件传输状态标记，是manager中的）
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	FileSize      uint64    `json:"file_size"`
	NodeID        string    `json:"node_id"`        //上传到哪个节点或者从哪个节点下载
	FailureReason string    `json:"failure_reason"` //失败原因
	AbortReason   string    `json:"abort_reason"`   //终止原因
	//RateOfProgress uint32   `json:"rate_of_progress"` //已完成的分段个数
}

type MissingBlockWithError struct {
	Block    protocol.MissingSegment
	FilePath string
	Error    string
}

// CreateFile 检查给你定的路径是否合法，不存在的话是否可创建。如果上传的文件要保存的路径已经有文件了，是覆盖还是重命名
func CreateFile(filePath string, isCover bool) (string, error) {
	//todo Windows上传了/test/test的路径会怎么样,给不同的正斜线反斜线都能正常创建吗，除此之外其他路径传递基本满足，等写完在测试各种情况吧
	var baseDir string
	filePath, _ = filepath.Abs(filePath)
	if regexp.MustCompile("^[a-zA-Z]:").MatchString(filePath) || strings.Contains(filePath, "\\") {
		filePath = strings.ReplaceAll(filePath, "\\", "/")
		baseDir = strings.ReplaceAll(filepath.Dir(filePath), "/", "\\")
		//fileName = filepath.Base(filePath)
	} else {
		baseDir = filepath.Dir(filePath)
	}
	if baseDir != "." {
		//不是在当前目录，需要创建新目录，直接使用createAll创建
		err := os.MkdirAll(baseDir, 0666)
		if err != nil {
			return "", err
		}
	}
	basePath := filePath
	//判断文件是否存在
	for i := 0; i < 100; i++ {
		if i != 0 {
			if strings.Contains(basePath, ".") {
				lastDot := strings.LastIndex(basePath, ".")
				filePath = basePath[:lastDot] + fmt.Sprintf("(%v).", i) + basePath[lastDot+1:]
			} else {
				filePath = basePath + fmt.Sprintf("(%v)", i)
			}
		}

		_, err := os.Stat(filePath)
		if err == nil && isCover == false { //文件存在，并且不能强行覆盖
			if i == 99 {
				return "", fmt.Errorf("file already exists %v and files with the suffix 1-99 also exist", filePath)
			}
			continue
		} else if os.IsNotExist(err) || isCover == true { //文件不存在或者可以强行覆盖
			break
		} else { //其他error并且覆盖不能强行覆盖文件
			return "", fmt.Errorf("check file %v info error:%v", filePath, err)
		}
	}
	//在指定目录创建临时文件
	f, err := os.OpenFile(filePath+".downloading", os.O_RDWR|os.O_CREATE, 0666)
	//f, err := os.Create(filePath + ".downloading")
	if err != nil {
		return "", err
	}
	_ = f.Close()
	return filePath + ".downloading", nil
}

// ReadFileBlock 读取文件指定位置的数据
func ReadFileBlock(file *os.File, blockSize uint32, endBlockSize uint32, totalBlockNum uint64, readBlockNum uint64) ([]byte, error) {
	var data []byte
	var err error
	if totalBlockNum == readBlockNum {
		data = make([]byte, endBlockSize)
	} else {
		data = make([]byte, blockSize)
	}
	_, err = file.Seek(int64(readBlockNum-1)*int64(blockSize), 0)
	if err != nil {
		return nil, err
	}
	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WriteFileBlock 写入内容到文件指定位置，并在文件尾标记分段写入完成
func WriteFileBlock(file *os.File, blockSize uint32, endBlockSize uint32, totalBlockNum uint64, writeBlockNum uint64, data []byte, hash string) error {
	var err error
	if writeBlockNum == totalBlockNum {
		data = data[:endBlockSize]
	} else {
		data = data[:blockSize]
	}
	_, err = file.WriteAt(data, int64(writeBlockNum-1)*int64(blockSize))
	if err != nil {
		return err
	}
	//第一次写入的时候写入文件hash
	if writeBlockNum == 1 {
		_, err = file.WriteAt([]byte(hash), int64(blockSize)*int64(totalBlockNum-1)+int64(endBlockSize))
	}
	_, err = file.WriteAt([]byte("1"), int64(blockSize)*int64(totalBlockNum-1)+int64(endBlockSize)+64+int64(writeBlockNum)-1)
	if err != nil {
		return err
	}
	return nil
}

// GetMissBlock 检查文文件是否缺失某个分段
func GetMissBlock(file *os.File, blockSize uint32, endBlockSize uint32, totalBlockNum uint64, hash string) (protocol.MissingSegment, error) {
	//todo 文件md5检查，第一次文件写入的时候将文件md5写入临时文件尾，检查缺少分段时优先检查md5，确认md5之后在进行分段检查。这样做是为了断点续传时确定当前的临时文件与要上传过来的文件是同一个文件，防止了同大小同名文件的干扰
	var err error
	var re []uint64
	var readHash []byte
	readHash = make([]byte, 64)
	_, err = file.Seek(int64(blockSize)*int64(totalBlockNum-1)+int64(endBlockSize), 0)
	if err != nil {
		return nil, err
	}
	_, err = file.Read(readHash)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if string(readHash) != hash {
		//不是一个文件，返回缺失所有分段，后面会重新覆盖
		for i := uint64(1); i < totalBlockNum+1; i++ {
			re = append(re, i)
		}
		return re, nil
	}
	var data []byte
	data = make([]byte, totalBlockNum)
	_, err = file.Seek(int64(blockSize)*int64(totalBlockNum-1)+int64(endBlockSize)+64, 0)
	if err != nil {
		return nil, err
	}
	_, err = file.Read(data)
	if err != nil {
		return nil, err
	}
	for n, s := range string(data) {
		if string(s) != "1" {
			re = append(re, uint64(n+1))
		}
	}
	return re, nil
}

// DeleteBlockFlag  将文件尾的后缀删除掉
func DeleteBlockFlag(file *os.File, blockSize uint32, endBlockSize uint32, totalBlockNum uint64) error {
	err := file.Truncate(int64(blockSize)*int64(totalBlockNum-1) + int64(endBlockSize))
	if err != nil {
		return err
	}
	return nil
}
