package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"reflect"

	"github.com/zyylhn/node-tree/crypto"
)

type RawMessage struct {
	// Essential component to apply a Message
	UUID         string
	Conn         net.Conn
	CryptoSecret []byte
	// Prepared buffer
	HeaderBuffer []byte //请求头，包含了header相关信息
	DataBuffer   []byte //发送的数据
}

func (message *RawMessage) ConstructHeader() {}

// ConstructData 根据header和密码构造数据包到DataBuffer中
func (message *RawMessage) ConstructData(header *Header, mess interface{}, isPass bool) {
	var headerBuffer, dataBuffer bytes.Buffer
	// First, construct own header
	messageTypeBuf := make([]byte, 2)
	taskIDLenBuf := make([]byte, 2)
	routeLenBuf := make([]byte, 4)

	binary.BigEndian.PutUint16(messageTypeBuf, header.MessageType)
	binary.BigEndian.PutUint32(routeLenBuf, header.RouteLen)
	binary.BigEndian.PutUint16(taskIDLenBuf, header.TaskIDLen)

	// Write header into buffer(except for dataLen)
	headerBuffer.Write([]byte(header.Sender))
	headerBuffer.Write([]byte(header.Acceptor))
	headerBuffer.Write(messageTypeBuf)
	headerBuffer.Write(routeLenBuf)
	headerBuffer.Write([]byte(header.Route))
	headerBuffer.Write(taskIDLenBuf)
	headerBuffer.Write([]byte(header.TaskID))

	// Check if message's data is needed to encrypt
	if !isPass {
		// Use reflect to construct data,optimize the code,thx to the idea from @lz520520
		messType := reflect.TypeOf(mess).Elem()
		messValue := reflect.ValueOf(mess).Elem()

		messFieldNum := messType.NumField()

		for i := 0; i < messFieldNum; i++ {
			inter := messValue.Field(i).Interface()

			switch value := inter.(type) {
			case string:
				dataBuffer.Write([]byte(value))
			case uint16:
				buffer := make([]byte, 2)
				binary.BigEndian.PutUint16(buffer, value)
				dataBuffer.Write(buffer)
			case uint32:
				buffer := make([]byte, 4)
				binary.BigEndian.PutUint32(buffer, value)
				dataBuffer.Write(buffer)
			case uint64:
				buffer := make([]byte, 8)
				binary.BigEndian.PutUint64(buffer, value)
				dataBuffer.Write(buffer)
			case []byte:
				dataBuffer.Write(value)
			}
		}
	} else {
		mMess := mess.([]byte)
		dataBuffer.Write(mMess)
	}

	message.DataBuffer = dataBuffer.Bytes()
	// Encrypt&Compress data
	if !isPass {
		message.DataBuffer = crypto.GzipCompress(message.DataBuffer)
		message.DataBuffer = crypto.AESEncrypt(message.DataBuffer, message.CryptoSecret)
	}
	// Calculate the whole data's length
	dataLenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(dataLenBuf, uint64(len(message.DataBuffer)))
	headerBuffer.Write(dataLenBuf)
	message.HeaderBuffer = headerBuffer.Bytes()
}

func (message *RawMessage) ConstructSuffix() {}

func (message *RawMessage) DeconstructHeader() {}

func (message *RawMessage) DeconstructData() (*Header, interface{}, error) {
	var (
		header         = new(Header)
		senderBuf      = make([]byte, 10)
		acceptorBuf    = make([]byte, 10)
		messageTypeBuf = make([]byte, 2)
		routeLenBuf    = make([]byte, 4)
		dataLenBuf     = make([]byte, 8)
		taskIDLenBuf   = make([]byte, 2)
	)

	var err error
	// Read header's element one by one
	_, err = io.ReadFull(message.Conn, senderBuf)
	if err != nil {
		return header, nil, err
	}
	header.Sender = string(senderBuf)

	_, err = io.ReadFull(message.Conn, acceptorBuf)
	if err != nil {
		return header, nil, err
	}
	header.Acceptor = string(acceptorBuf)

	_, err = io.ReadFull(message.Conn, messageTypeBuf)
	if err != nil {
		return header, nil, err
	}
	header.MessageType = binary.BigEndian.Uint16(messageTypeBuf)

	_, err = io.ReadFull(message.Conn, routeLenBuf)
	if err != nil {
		return header, nil, err
	}
	header.RouteLen = binary.BigEndian.Uint32(routeLenBuf)

	routeBuf := make([]byte, header.RouteLen)
	_, err = io.ReadFull(message.Conn, routeBuf)
	if err != nil {
		return header, nil, err
	}
	header.Route = string(routeBuf)

	_, err = io.ReadFull(message.Conn, taskIDLenBuf)
	if err != nil {
		return header, nil, err
	}
	header.TaskIDLen = binary.BigEndian.Uint16(taskIDLenBuf)

	taskIDBuf := make([]byte, header.TaskIDLen)
	_, err = io.ReadFull(message.Conn, taskIDBuf)
	if err != nil {
		return header, nil, err
	}
	header.TaskID = string(taskIDBuf)

	_, err = io.ReadFull(message.Conn, dataLenBuf)
	if err != nil {
		return header, nil, err
	}
	header.DataLen = binary.BigEndian.Uint64(dataLenBuf)

	dataBuf := make([]byte, header.DataLen)
	_, err = io.ReadFull(message.Conn, dataBuf)
	if err != nil {
		return header, nil, err
	}

	//如果接收者为temp、或者接受者是当前节点、或者当前节点为admin，就将数据解密
	if header.Acceptor == TempUuid || message.UUID == AdminUuid || message.UUID == header.Acceptor {
		dataBuf = crypto.AESDecrypt(dataBuf, message.CryptoSecret) // use dataBuf directly to save the memory
	} else { //否则直接返回
		return header, dataBuf, nil
	}
	// Decompress the data
	dataBuf = crypto.GzipDecompress(dataBuf)
	// Use reflect to deconstruct data
	var mess interface{}
	switch header.MessageType {
	case MessageTypeHI:
		mess = new(HIMess)
	case MessageTypeUUID:
		mess = new(UUIDMess)
	case MessageTypeChildUuidReq:
		mess = new(ChildUUIDReq)
	case MessageTypeChildUuidRes:
		mess = new(ChildUUIDRes)
	case MessageTypeMyInfo:
		mess = new(MyInfo)
	case MessageTypeMyMemo:
		mess = new(MyMemo)
	case MessageTypeShellReq:
		mess = new(ShellReq)
	case MessageTypeShellRes:
		mess = new(ShellRes)
	case MessageTypeShellCmd:
		mess = new(ShellCommand)
	case MessageTypeShellResult:
		mess = new(ShellResult)
	case MessageTypeShellExit:
		mess = new(ShellExit)
	case MessageTypeListenReq:
		mess = new(ListenReq)
	case MessageTypeListenRes:
		mess = new(ListenRes)
	case MessageTypeSshReq:
		mess = new(SSHReq)
	case MessageTypeSshRes:
		mess = new(SSHRes)
	case MessageTypeSshCmd:
		mess = new(SSHCommand)
	case MessageTypeSshResult:
		mess = new(SSHResult)
	case MessageTypeSshExit:
		mess = new(SSHExit)
	case MessageTypeSshTunnelReq:
		mess = new(SSHTunnelReq)
	case MessageTypeSshTunnelRes:
		mess = new(SSHTunnelRes)
	case MessageTypeSocksStart:
		mess = new(SocksStart)
	case MessageTypeSocksTcpData:
		mess = new(SocksTCPData)
	case MessageTypeSocksUdpData:
		mess = new(SocksUDPData)
	case MessageTypeUdpAssReq:
		mess = new(UDPAssStart)
	case MessageTypeUdpAssRes:
		mess = new(UDPAssRes)
	case MessageTypeSocksTcpFin:
		mess = new(SocksTCPFin)
	case MessageTypeSocksReady:
		mess = new(SocksReady)
	case MessageTypeForwardTest:
		mess = new(ForwardTest)
	case MessageTypeForwardStart:
		mess = new(ForwardStart)
	case MessageTypeForwardReady:
		mess = new(ForwardReady)
	case MessageTypeForwardData:
		mess = new(ForwardData)
	case MessageTypeForwardFin:
		mess = new(ForwardFin)
	case MessageTypeBackwardTest:
		mess = new(BackwardTest)
	case MessageTypeBackwardReady:
		mess = new(BackwardReady)
	case MessageTypeBackwardStart:
		mess = new(BackwardStart)
	case MessageTypeBackwardSeq:
		mess = new(BackwardSeq)
	case MessageTypeBackwardData:
		mess = new(BackwardData)
	case MessageTypeBackwardFin:
		mess = new(BackWardFin)
	case MessageTypeBackwardStop:
		mess = new(BackwardStop)
	case MessageTypeBackwardStopDone:
		mess = new(BackwardStopDone)
	case MessageTypeConnectStart:
		mess = new(ConnectStart)
	case MessageTypeConnectDone:
		mess = new(ConnectRes)
	case MessageTypeNodeOffLine:
		mess = new(NodeOffline)
	case MessageTypeNodeReOnline:
		mess = new(NodeReOnline)
	case MessageTypeUpStreamOffLine:
		mess = new(UpstreamOffline)
	case MessageTypeUpStreamReOnline:
		mess = new(UpstreamReOnline)
	case MessageTypeUpStreamReOnlineShutdown:
		mess = new(Shutdown)
	case MessageTypeListenerCloseReq:
		mess = new(StopListenerReq)
	case MessageTypeListenerCloseRes:
		mess = new(StopListenerRes)
	case MessageTypeGetListenerReq:
		mess = new(GetListenerReq)
	case MessageTypeGetListenerRes:
		mess = new(GetListenerRes)
	case MessageTypeReSetAgentOptionRes:
		mess = new(ResetAgentOptionRes)
	case MessageTypeReSetAgentOptionReq:
		mess = new(RestAgentOptionReq)
	case MessageTypeGetAgentOptionsReq:
		mess = new(GETAgentOptionReq)
	case MessageTypeGetAgentOptionRes:
		mess = new(GETAgentOptionRes)
	case MessageTypeGetAgentInfoReq:
		mess = new(GetAgentInfo)
	case MessageTypeActiveOffLine:
		mess = new(ActiveOffline)
	case MessageTypeShutdownAll:
		mess = new(ShutdownAll)
	case MessageTypeSendTaskInfoReq:
		mess = new(SendTaskInfoReq)
	case MessageTypeSendTaskInfoRes: //返回的是模块缺失的分段
		mess = new(FileMissSegmentInfo)
	case MessageTypeSendModuleData:
		mess = new(FileManagerFileData)
	case MessageTypeSendModuleEnd:
		mess = new(FileManagerTransferEnd)
	case MessageTypeCheckSizeResult:
		mess = new(CheckIntegrityRes)
	case MessageTypeLoadResultInfo:
		mess = new(ModuleResultInfo)
	case MessageTypeReadyReceive:
		mess = new(ReadyReceive)
	case MessageTypeCancelRemoteLoadReq:
		mess = new(CancelCtx)
	case MessageTypeReverseNodeUuid:
		mess = new(ReverseNodeUuid)
	case MessageTypeNodeReOnlineRes:
		mess = new(NodeReOnlineRes)
	case MessageTypeFileUploadReq:
		mess = new(FileManagerReq)
	case MessageTypeFileUploadRes:
		mess = new(FileMissSegmentInfo)
	case MessageTypeWriteFileError:
		mess = new(ErrorMessage)
	case MessageTypeUploadFileData:
		mess = new(FileManagerFileData)
	case MessageTypeUploadFileDataEnd:
		mess = new(FileManagerTransferEnd)
	case MessageTypeCancelUpload:
		mess = new(StopFileTransfer)
	case MessageTypeUploadFileChecksSizeRes:
		mess = new(CheckIntegrityRes)
	}

	messType := reflect.TypeOf(mess).Elem()
	messValue := reflect.ValueOf(mess).Elem()
	messFieldNum := messType.NumField()

	var ptr uint64
	for i := 0; i < messFieldNum; i++ {
		inter := messValue.Field(i).Interface()
		field := messValue.FieldByName(messType.Field(i).Name)

		switch inter.(type) {
		case string:
			tmp := messValue.FieldByName(messType.Field(i).Name + "Len")
			// 全转为uint64
			var stringLen uint64
			switch stringLenTmp := tmp.Interface().(type) {
			case uint16:
				stringLen = uint64(stringLenTmp)
			case uint32:
				stringLen = uint64(stringLenTmp)
			case uint64:
				stringLen = stringLenTmp
			}
			field.SetString(string(dataBuf[ptr : ptr+stringLen]))
			ptr += stringLen
		case uint16:
			field.SetUint(uint64(binary.BigEndian.Uint16(dataBuf[ptr : ptr+2])))
			ptr += 2
		case uint32:
			field.SetUint(uint64(binary.BigEndian.Uint32(dataBuf[ptr : ptr+4])))
			ptr += 4
		case uint64:
			field.SetUint(binary.BigEndian.Uint64(dataBuf[ptr : ptr+8]))
			ptr += 8
		case []byte:
			tmp := messValue.FieldByName(messType.Field(i).Name + "Len")
			var byteLen uint64
			switch byteLenTmp := tmp.Interface().(type) {
			case uint16:
				byteLen = uint64(byteLenTmp)
			case uint32:
				byteLen = uint64(byteLenTmp)
			case uint64:
				byteLen = byteLenTmp
			}
			field.SetBytes(dataBuf[ptr : ptr+byteLen])
			ptr += byteLen
		default:
			return header, nil, errors.New("unknown error")
		}
	}

	return header, mess, nil
}

func (message *RawMessage) DeconstructSuffix() {}

func (message *RawMessage) SendMessage() {
	finalBuffer := append(message.HeaderBuffer, message.DataBuffer...)
	_, _ = message.Conn.Write(finalBuffer)
	// Don't forget to set both Buffer to nil!!!
	message.HeaderBuffer = nil
	message.DataBuffer = nil
}
