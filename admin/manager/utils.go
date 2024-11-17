package manager

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"io"
	"os"
)

func newNodeNotFoundError(node string) error {
	return fmt.Errorf("no rout information was found for the %v,it may not be online, please check and try again", node)
}

var ChannelTimeOut = errors.New("read response from client timeout")

var ChannelClose = errors.New("response return but channel already stop")

const (
	UPMethod  = iota //用户名密码
	CERMethod        //证书
)

type SSH struct {
	Method          int
	Addr            string
	Username        string
	Password        string
	CertificatePath string
	Certificate     []byte
}

func (ssh *SSH) getCertificate() (err error) {
	ssh.Certificate, err = os.ReadFile(ssh.CertificatePath)
	if err != nil {
		return
	}
	return
}

var TerminationErr = errors.New("be terminated")

// SHA256 计算文件hash
func SHA256(filePath string) (string, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = file.Close()
	}()

	// 创建 SHA-256 哈希对象
	hash := sha256.New()

	// 将文件内容写入哈希对象
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}

	// 计算哈希值并返回
	hashInBytes := hash.Sum(nil)
	hashString := hex.EncodeToString(hashInBytes)

	return hashString, nil
}

// NodeUpAndDownStreamInfo 节点断线消息
type NodeUpAndDownStreamInfo struct {
	ParentNode       *topology.Node     `json:"parent_node"`        //断线节点的上级节点，可以用来重连使用
	ChildrenNodeTree *topology.NodeTree `json:"children_node_tree"` //断线节点作为头节点的节点树
}
