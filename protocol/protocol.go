package protocol

import (
	"fmt"
	"net"
	"time"

	"github.com/zyylhn/node-tree/crypto"
)

var Upstream string   //上游协议
var Downstream string //下游协议

var ChanTimeOut = time.Duration(120) //channel 等待任务返回结果的超时时间，目的是防止任务阻塞

const (
	MessageTypeHI = iota
	MessageTypeUUID
	MessageTypeChildUuidReq
	MessageTypeChildUuidRes
	MessageTypeMyInfo
	MessageTypeMyMemo
	MessageTypeShellReq
	MessageTypeShellRes
	MessageTypeShellCmd
	MessageTypeShellResult
	MessageTypeShellExit
	MessageTypeListenReq
	MessageTypeListenRes
	MessageTypeSshReq
	MessageTypeSshRes
	MessageTypeSshCmd
	MessageTypeSshResult
	MessageTypeSshExit
	MessageTypeSshTunnelReq
	MessageTypeSshTunnelRes
	MessageTypeSocksStart
	MessageTypeSocksTcpData
	MessageTypeSocksUdpData
	MessageTypeUdpAssReq
	MessageTypeUdpAssRes
	MessageTypeSocksTcpFin
	MessageTypeSocksReady
	MessageTypeForwardTest
	MessageTypeForwardStart
	MessageTypeForwardReady
	MessageTypeForwardData
	MessageTypeForwardFin
	MessageTypeBackwardTest
	MessageTypeBackwardStart
	MessageTypeBackwardSeq
	MessageTypeBackwardReady
	MessageTypeBackwardData
	MessageTypeBackwardFin
	MessageTypeBackwardStop
	MessageTypeBackwardStopDone
	MessageTypeConnectStart
	MessageTypeConnectDone
	MessageTypeNodeOffLine
	MessageTypeNodeReOnline
	MessageTypeUpStreamOffLine
	MessageTypeUpStreamReOnline
	MessageTypeUpStreamReOnlineShutdown
	MessageTypeListenerCloseReq
	MessageTypeListenerCloseRes
	MessageTypeGetListenerReq
	MessageTypeGetListenerRes
	MessageTypeReSetAgentOptionReq
	MessageTypeReSetAgentOptionRes
	MessageTypeGetAgentOptionsReq
	MessageTypeGetAgentOptionRes
	MessageTypeGetAgentInfoReq
	MessageTypeActiveOffLine
	MessageTypeShutdownAll
	MessageTypeSendTaskInfoReq
	MessageTypeSendTaskInfoRes
	MessageTypeSendModuleData
	MessageTypeSendModuleEnd
	MessageTypeCheckSizeResult
	MessageTypeLoadResultInfo
	MessageTypeReadyReceive
	MessageTypeCancelRemoteLoadReq
	MessageTypeReverseNodeUuid
	MessageTypeNodeReOnlineRes
	MessageTypeFileUploadReq
	MessageTypeFileUploadRes
	MessageTypeWriteFileError
	MessageTypeUploadFileData
	MessageTypeUploadFileDataEnd
	MessageTypeCancelUpload
	MessageTypeUploadFileChecksSizeRes
)

const AdminUuid = "bBpjJe8Hsv"
const TempUuid = "ZIxLjfCSvJ"
const TempRoute = "Z3nZvSQHEnYzFO"

const Active = "Active connection"
const Passive = "Passive Monitor"

const (
	NormalActive          = iota + 1 //主动连接上级节点
	NormalPassive                    //等待上级节点反连
	ProxyActive                      //使用代理主动连接上级节点
	ProxyReconnectActive             //带有代理的和重连机制主动连接上级节点
	NormalReconnectActive            //没有代理的但是有重连机制的连接上级节点
)

// Options 客户端启动参数
type Options struct {
	Mode                int    //连接模式
	Secret              string //连接密码
	Listen              string //监听模式
	Reconnect           uint64 //重连
	Connect             string //主动连接模式
	Proxy               string
	ProxyU              string
	ProxyP              string
	Upstream            string
	Downstream          string
	Charset             string
	DeleteSelf          bool     //程序结束之后是否删除自己
	Block               bool     //阻塞命令行启动,仅在linux上生效
	Test                bool     //测试联通性功能，指定此参数之后不进行bot身份验证，仅测试网络联通性
	PermanentID         bool     //是否启用永久id（会在目标的目录下生成一个文件）
	PermanentIDFilePath string   //存放永久id的文件
	Memo                []string //备注信息
}

// PermanentInfo 持久化的信息
type PermanentInfo struct {
	ConnectInfo ConnectMethod `json:"connect_info"`
	Uuid        string        `json:"uuid"`
	Memo        []string      `json:"memo"`
	Charset     string        `json:"charset"`
}

// Message 发送和接受消息的接口
type Message interface {
	ConstructHeader()
	// ConstructData 根据header和interface（data）的值构造数据包
	ConstructData(*Header, interface{}, bool)
	ConstructSuffix()
	DeconstructHeader()
	// DeconstructData 进行一次数据包读取并根据数据包解析返回header和data
	DeconstructData() (*Header, interface{}, error)
	DeconstructSuffix()
	SendMessage()
}

// ConstructMessage 生成响应包
func ConstructMessage(message Message, header *Header, mess interface{}, isPass bool) {
	message.ConstructData(header, mess, isPass)
	message.ConstructHeader()
	message.ConstructSuffix()
}

// DestructMessage 解析响应包
func DestructMessage(message Message) (*Header, interface{}, error) {
	message.DeconstructHeader()
	header, mess, err := message.DeconstructData()
	message.DeconstructSuffix()
	return header, mess, err
}

type Header struct {
	Sender      string // 发送者：10字节
	Acceptor    string //接受者：10字节
	MessageType uint16 //消息类型：2 bytes
	RouteLen    uint32 //4 bytes
	Route       string // 长度不一定根据route len来断定
	DataLen     uint64 //8 bytes
	TaskIDLen   uint16
	TaskID      string //区分消息id：10字节
}

type CompletePacket struct {
	ReqHeader *Header
	ReqBody   interface{}
}

// RestAgentOptionReq 设置agent参数请求
type RestAgentOptionReq struct {
	OpLen uint32
	Op    string
}

type ResetAgentOptionRes struct {
	ErrorLen uint64
	Error    string
}

type ReverseNodeUuid struct {
	UuidLen uint16
	Uuid    string
}

type GETAgentOptionReq struct {
}

type GETAgentOptionRes struct {
	OptionLen uint64
	Option    string
}

type ActiveOffline struct {
}

type ShutdownAll struct {
	DeleteSelf uint16
}

type ErrorMessage struct {
	ErrLen    uint16
	Err       string
	TaskIDLen uint16
	TaskID    string
}

type SendTaskInfoReq struct {
	ArgsLen      uint16
	Args         string
	TaskIDLen    uint16
	TaskID       string
	LoadDirLen   uint16
	LoadDir      string
	BlockLength  uint64
	BlockSize    uint32
	EndBlockSize uint32
}

type FileManagerReq struct {
	TaskIDLen     uint16
	TaskID        string
	FilePathLen   uint16
	FilePath      string
	BlockLength   uint64
	BlockSize     uint32
	EndBlockSize  uint32
	IsCover       uint16 //是否覆盖原有文件
	FileSHA256Len uint16
	FileSHA256    string
}

type MissingSegment []uint64

type FileMissSegmentInfo struct {
	TaskIDLen      uint16
	TaskID         string
	MissSegmentLen uint32
	MissSegment    string //序列化数据
	ErrLen         uint16
	Err            string
	FilePathLen    uint16
	FilePath       string //文件路径
}

type FileManagerFileData struct {
	TaskIDLen  uint16
	TaskID     string
	DataLen    uint64
	Data       []byte
	SegmentNum uint64
}

type FileManagerTransferEnd struct {
	TaskIDLen uint16
	TaskID    string
	ErrLen    uint64
	Err       string
}

type CheckIntegrityRes struct {
	TaskIDLen      uint16
	TaskID         string
	MissSegmentLen uint32
	MissSegment    string //序列化数据
	ErrLen         uint64
	Err            string
}

type ModuleResultInfo struct {
	TaskIDLen    uint16
	TaskID       string
	ErrLen       uint64
	Err          string
	StdErrLen    uint64
	StdErr       string
	BlockLength  uint64
	BlockSize    uint32
	EndBlockSize uint32
}

type BlockWithNum struct {
	Num  uint64 //分段的编号
	Data []byte //数据段
}

type ReadyReceive struct {
	TaskIDLen      uint16
	TaskID         string
	MissSegmentLen uint32
	MissSegment    string //序列化数据
}

// HIMess 握手包
type HIMess struct {
	GreetingLen uint16 //握手字符串的长度
	Greeting    string //握手字符串
	UUIDLen     uint16 //uuid的长度
	UUID        string //uuid的id
	IsAdmin     uint16 //是否是admin节点
	IsReconnect uint16 //是否是重连
}

type UUIDMess struct {
	UUIDLen uint16
	UUID    string
}

type ChildUUIDReq struct {
	ParentUUIDLen uint16
	ParentUUID    string
	AddrLen       uint16
	Addr          string
}

type ChildUUIDRes struct {
	UUIDLen uint16
	UUID    string
}

type NodeReOnlineRes struct {
	OK uint16
}

type StopListenerReq struct {
	All     uint64
	ADDRLen uint64
	ADDR    string
}

type StopListenerRes struct {
	OK uint16
}

type GetListenerReq struct {
	OK uint16
}

type GetListenerRes struct {
	ListenerLen uint64
	Listener    string
}

type CancelCtx struct {
	TaskIDLen uint16
	TaskID    string
}

type StopFileTransfer struct {
	TaskIDLen uint16
	TaskID    string
	IsDelete  uint16 //是否删除临时文件
}

type GetAgentInfo struct{}

type MyInfo struct {
	UUIDLen        uint16
	UUID           string
	SystemInfoLen  uint64
	SystemInfo     string
	MemoLen        uint64
	Memo           string
	ConnectInfoLen uint64
	ConnectInfo    string
}

type ConnectMethod struct {
	Mode            string `json:"mode"`   //连接模式
	Secret          string `json:"secret"` //连接密码
	*PassiveConnect        //父节点主动连接
	*ActiveConnect         //父节点被动连接
	Upstream        string `json:"upstream"`
	Downstream      string `json:"downstream"`
}

func (c *ConnectMethod) String() string {
	if c != nil {
		var result string

		if c.PassiveConnect != nil {
			result = fmt.Sprintf("node listenaddr on %v", c.Listen)
		} else {
			result = fmt.Sprintf("node initial to %v", c.Connect)
			if c.Reconnect != 0 {
				result += fmt.Sprintf(" has reconnect(sleep:%vs)", c.Reconnect)
			}
			if c.Proxy != "" {
				result += fmt.Sprintf(" use proxy %v", c.Proxy)
			}
			if c.Proxy != "" {
				result += fmt.Sprintf(" socks username:%v password:%v", c.ProxyU, c.ProxyP)
			}
		}
		if c.Upstream != "" {
			result += fmt.Sprintf(" upstream traffic plugintype %v,", c.Upstream)
		}
		if c.Downstream != "" {
			result += fmt.Sprintf("downstream traffic plugintype %v", c.Downstream)
		}
		return result
	} else {
		return "NULL"
	}
}

// PassiveConnect 子节点监听为被动
type PassiveConnect struct {
	Listen string `json:"listen"`
}

// ActiveConnect 子节点来连接父节点为主动
type ActiveConnect struct {
	Connect   string `json:"initial"`
	Reconnect uint64 `json:"reconnect"`
	Proxy     string `json:"proxy"`
	ProxyU    string `json:"proxy_u"`
	ProxyP    string `json:"proxy_p"`
}

func NewConnectMethod(c, l, pass, proxy, pu, pp, up, down string, reconnect uint64) *ConnectMethod {
	if c != "" {
		passive := &ActiveConnect{Connect: c, Reconnect: reconnect, Proxy: proxy, ProxyP: pp, ProxyU: pu}
		return &ConnectMethod{
			Mode:          Active,
			Secret:        pass,
			ActiveConnect: passive,
			Upstream:      up,
			Downstream:    down,
		}
	} else {
		return &ConnectMethod{
			Mode:           Passive,
			Secret:         pass,
			PassiveConnect: &PassiveConnect{Listen: l},
			Upstream:       up,
			Downstream:     down,
		}
	}
}

type MyMemo struct {
	MemoLen uint64
	Memo    string
}

type ShellReq struct {
	Start    uint16
	ShellLen uint64
	Shell    string
}

type ShellRes struct {
	ErrLen uint64
	Err    string
}

type ShellCommand struct {
	CommandLen uint64
	Command    string
}

type ShellResult struct {
	ResultLen uint64
	Result    string
}

type ShellExit struct {
	OK uint16
}

type ListenReq struct {
	Method       uint16
	AddrLen      uint64
	Addr         string
	StopListener uint16 //1代表接收到上线节点就停止监听器监听
	Wait         uint16 //1代表服务器正在等待监听器接收到的结果，只有在stop listener为true的时候才会使用
}

type ListenRes struct {
	ErrLen uint64
	Err    string
}

type SSHReq struct {
	Method         uint16
	AddrLen        uint16
	Addr           string
	UsernameLen    uint64
	Username       string
	PasswordLen    uint64
	Password       string
	CertificateLen uint64
	Certificate    []byte
}

type SSHRes struct {
	ErrLen uint64
	Err    string
}

type SSHCommand struct {
	CommandLen uint64
	Command    string
}

type SSHResult struct {
	ResultLen uint64
	Result    string
}

type SSHExit struct {
	OK uint16
}

type SSHTunnelReq struct {
	Method         uint16
	AddrLen        uint16
	Addr           string
	PortLen        uint16
	Port           string
	UsernameLen    uint64
	Username       string
	PasswordLen    uint64
	Password       string
	CertificateLen uint64
	Certificate    []byte
}

type SSHTunnelRes struct {
	ErrLen uint64
	Err    string
}

type FileStatReq struct {
	FilenameLen uint32
	Filename    string
	FileSize    uint64
	SliceNum    uint64
}

type FileStatRes struct {
	OK uint16
}

type FileData struct {
	DataLen uint64
	Data    []byte
}

type FileErr struct {
	Error uint16
}

type FileDownReq struct {
	FilePathLen uint32
	FilePath    string
	FilenameLen uint32
	Filename    string
}

type FileDownRes struct {
	OK uint16
}

type SocksStart struct {
	UsernameLen uint64
	Username    string
	PasswordLen uint64
	Password    string
}

type SocksTCPData struct {
	Seq     uint64
	DataLen uint64
	Data    []byte
}

type SocksUDPData struct {
	Seq     uint64
	DataLen uint64
	Data    []byte
}

type UDPAssStart struct {
	Seq           uint64
	SourceAddrLen uint16
	SourceAddr    string
}

type UDPAssRes struct {
	Seq     uint64
	OK      uint16
	AddrLen uint16
	Addr    string
}

type SocksTCPFin struct {
	Seq uint64
}

type SocksReady struct {
	ErrLen uint64
	Err    string
}

type ForwardTest struct {
	AddrLen uint16
	Addr    string
}

type ForwardStart struct {
	Seq     uint64
	AddrLen uint16
	Addr    string
}

type ForwardReady struct {
	ErrLen uint64
	Err    string
}

type ForwardData struct {
	Seq     uint64
	DataLen uint64
	Data    []byte
}

type ForwardFin struct {
	Seq uint64
}

type BackwardTest struct {
	LAddrLen uint16
	LAddr    string
	RPortLen uint16 //todo 后续升级成rAddr支持监听节点的特定地址端口
	RPort    string
}

type BackwardStart struct {
	UUIDLen  uint16
	UUID     string
	LAddrLen uint16
	LAddr    string
	RPortLen uint16
	RPort    string
}

type BackwardReady struct {
	ErrLen uint64
	Err    string
}

type BackwardSeq struct {
	Seq      uint64
	RPortLen uint16
	RPort    string
}

type BackwardData struct {
	Seq     uint64
	DataLen uint64
	Data    []byte
}

type BackWardFin struct {
	Seq uint64
}

type BackwardStop struct {
	All      uint16
	RPortLen uint16
	RPort    string
}

type BackwardStopDone struct {
	All      uint16
	UUIDLen  uint16
	UUID     string
	RPortLen uint16
	RPort    string
}

type ConnectStart struct {
	AddrLen   uint16
	Addr      string
	ProxyLen  uint16
	Proxy     string
	ProxyULen uint16
	ProxyU    string
	ProxyPLen uint16
	ProxyP    string
}

type ConnectRes struct {
	ErrLen  uint64
	Err     string
	UuidLen uint16
	Uuid    string
}

type NodeOffline struct {
	UUIDLen uint16
	UUID    string
}

type NodeReOnline struct {
	ParentUUIDLen uint16
	ParentUUID    string
	UUIDLen       uint16
	UUID          string
	IPLen         uint16
	IP            string
	Backlink      uint16
	ConnectPort   uint16
}

type UpstreamOffline struct {
	OK uint16
}

type UpstreamReOnline struct {
	OK uint16
}

type Shutdown struct {
	OK         uint16
	DeleteSelf uint16
}

type MessageComponent struct {
	UUID   string   //当前节点id
	Conn   net.Conn //tcp连接
	Secret string   //连接密码
}

// DecideType 设置上下游协议
func DecideType(upstream, downstream string) {
	if upstream == "http" {
		Upstream = "http"
	} else {
		Upstream = "raw"
	}

	if downstream == "http" {
		Downstream = "http"
	} else {
		Downstream = "raw"
	}
}

// PrepareAndDecideWhichSProtoToUpper 使用上游流量类型来新建message，会与上级节点的下游协议对接（仅agent使用用来写入消息）
func PrepareAndDecideWhichSProtoToUpper(conn net.Conn, secret string, uuid string) Message {
	switch Upstream {
	case "raw":
		tMessage := new(RawMessage)
		tMessage.Conn = conn
		tMessage.UUID = uuid
		tMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	case "http":
		tMessage := new(HTTPMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	}
	return nil
}

// PrepareAndDecideWhichSProtoToLower 使用下游流量类型来新建message，会与agent的上游协议对接，用来写入消息
func PrepareAndDecideWhichSProtoToLower(conn net.Conn, secret string, uuid string) Message {
	switch Downstream {
	case "raw":
		tMessage := new(RawMessage)
		tMessage.Conn = conn
		tMessage.UUID = uuid
		tMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	case "http":
		tMessage := new(HTTPMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	}
	return nil
}

// PrepareAndDecideWhichRProtoFromUpper 使用上游流量类型来新建message，会与上级节点的下游协议对接（仅agent使用用来读取消息）
func PrepareAndDecideWhichRProtoFromUpper(conn net.Conn, secret string, uuid string) Message {
	switch Upstream {
	case "raw":
		tMessage := new(RawMessage)
		tMessage.Conn = conn
		tMessage.UUID = uuid
		tMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	case "http":
		tMessage := new(HTTPMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	}
	return nil
}

// PrepareAndDecideWhichRProtoFromLower 使用下游流量类型来新建message，会与agent的上游协议对接,用来读取消息
func PrepareAndDecideWhichRProtoFromLower(conn net.Conn, secret string, uuid string) Message {
	switch Downstream {
	case "raw":
		tMessage := new(RawMessage)
		tMessage.Conn = conn
		tMessage.UUID = uuid
		tMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	case "http":
		tMessage := new(HTTPMessage)
		tMessage.RawMessage = new(RawMessage)
		tMessage.RawMessage.Conn = conn
		tMessage.RawMessage.UUID = uuid
		tMessage.RawMessage.CryptoSecret = crypto.KeyPadding([]byte(secret))
		return tMessage
	}
	return nil
}
