package initial

import (
	"errors"
	"fmt"
	"github.com/zyylhn/node-tree/admin/topology"
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
	"github.com/zyylhn/node-tree/utils"
	"net"
)

const (
	MethodNormalActive  = iota //admin节点为主动连接模式没有使用代理
	MethodProxyActive          //admin节点为主动连接模式，并使用了代理
	MethodNormalPassive        //admin节点为监听模式
	MethodStartTogether        //agent和admin同时启动
)

type Options struct {
	Mode       uint8  //运行模式
	Secret     string //密码
	Listen     string
	Connect    string
	Proxy      string
	ProxyU     string
	ProxyP     string
	Downstream string //下游协议
}

// 生成uuid并发送给连接到的agent
func dispatchUUID(conn net.Conn, secret string) string {
	var sMessage protocol.Message

	uuid := utils.GenerateUUID()
	uuidMess := &protocol.UUIDMess{
		UUIDLen: uint16(len(uuid)),
		UUID:    uuid,
	}

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    protocol.TempUuid,
		MessageType: protocol.MessageTypeUUID,
		RouteLen:    uint32(len([]byte(protocol.TempRoute))),
		Route:       protocol.TempRoute,
	}

	sMessage = protocol.PrepareAndDecideWhichSProtoToLower(conn, secret, protocol.AdminUuid)

	protocol.ConstructMessage(sMessage, header, uuidMess, false)
	sMessage.SendMessage()

	return uuid
}

func GetConnWithAgent(op *Options, t *topology.Topology) (net.Conn, error) {
	var conn net.Conn
	var err error
	switch op.Mode {
	case MethodNormalActive: //主动连接
		conn, err = NormalActive(op, t, nil)
	case MethodNormalPassive: //被动连接
		conn, err = NormalPassive(op, t)
	case MethodProxyActive: //代理主动连接
		proxy := public.NewProxy(op.Connect, op.Proxy, op.ProxyU, op.ProxyP)
		conn, err = NormalActive(op, t, proxy)
	//case MethodStartTogether:
	//	conn, err = StartAgentAndAdmin(op, t) //todo 这块后面agent重构完应该需要进行一下调整。之前是不是没考虑全局admin和agent共用的情况
	default:
		return nil, fmt.Errorf("unknown start mode type:%v", op.Mode)
	}
	return conn, err
}

//// StartAgentAndAdmin 同时启动admin和agent0，agent监听端口，等待admin来连接
//func StartAgentAndAdmin(userOptions *Options, t *topology.Topology) (net.Conn, error) {
//	//agent监听端口
//	listenPort, err := getLocalAddr.GetFreePortWithError(nil)
//	if err != nil {
//		return nil, err
//	}
//	addr := net.JoinHostPort("127.0.0.1", fmt.Sprintf("%v", listenPort))
//	agentOption := new(protocol.Options)
//	agentOption.Mode = protocol.NormalActive
//	agentOption.Secret = userOptions.Secret
//	agentOption.Upstream = userOptions.Downstream
//	agentOption.Downstream = userOptions.Downstream
//	agentOption.Connect = addr
//	agentOption.Reconnect = 1
//	agentOption.PermanentID = true
//	agentOption.Charset = "utf-8"
//	agentOption.Memo = []string{"初始节点"}
//	go func() {
//		err = run.StartAgent(agentOption, 0)
//		if err != nil {
//			panic(err)
//		}
//	}()
//	time.Sleep(time.Second * 3)
//	userOptions.Listen = addr
//	return NormalPassive(userOptions, t)
//}

func NormalActive(userOptions *Options, t *topology.Topology, proxy *public.Proxy) (net.Conn, error) {

	var sMessage, rMessage protocol.Message

	hiMess := &protocol.HIMess{
		GreetingLen: uint16(len(public.HandshakeReq)),
		Greeting:    public.HandshakeReq,
		UUIDLen:     uint16(len(protocol.AdminUuid)),
		UUID:        protocol.AdminUuid,
		IsAdmin:     1,
		IsReconnect: 0,
	}

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    protocol.TempUuid,
		MessageType: protocol.MessageTypeHI,
		RouteLen:    uint32(len([]byte(protocol.TempRoute))), //没有路由
		Route:       protocol.TempRoute,
	}

	var (
		conn net.Conn
		err  error
	)

	if proxy == nil {
		conn, err = net.Dial("tcp", userOptions.Connect)
	} else {
		conn, err = proxy.Dial()
	}

	if err != nil {
		return nil, err
	}
	//连接成功之后进行身份验证
	if err = public.ActivePreAuth(conn, userOptions.Secret); err != nil {
		_ = conn.Close()
		return nil, err
	}

	//断定下游使用的协议，创建message
	sMessage = protocol.PrepareAndDecideWhichSProtoToLower(conn, userOptions.Secret, protocol.AdminUuid)

	//将上面构造的header和hello信息发送给目标
	protocol.ConstructMessage(sMessage, header, hiMess, false)
	sMessage.SendMessage()

	//根据下游协议创建message
	rMessage = protocol.PrepareAndDecideWhichRProtoFromLower(conn, userOptions.Secret, protocol.AdminUuid)
	//接收并解析连接目标返回的信息
	fHeader, fMessage, err := protocol.DestructMessage(rMessage)

	if err != nil {
		_ = conn.Close()
		return nil, errors.New(fmt.Sprintf("can't initial node %s, error: %s", conn.RemoteAddr().String(), err.Error()))
	}

	if fHeader.MessageType == protocol.MessageTypeHI { //判断目标返回的消息类型是不是也是Hello
		mess := fMessage.(*protocol.HIMess)
		if mess.Greeting == public.HandshakeRes && mess.IsAdmin == 0 { //返回信息是保持连接并且不是主节点
			if mess.IsReconnect == 0 { //如果不是重连就需要生成新的id来连接我们
				node := topology.NewNode(dispatchUUID(conn, userOptions.Secret), conn.RemoteAddr().String())
				t.AddNode(true, node, protocol.TempUuid)

				return conn, nil
			} else { //如果是重连的话子节点会带着自己的id连回来
				node := topology.NewNode(mess.UUID, conn.RemoteAddr().String())
				t.AddNode(true, node, protocol.TempUuid)

				return conn, nil
			}
		}
	}
	_ = conn.Close()
	return nil, errors.New("target node seems illegal")
}

func NormalPassive(userOptions *Options, t *topology.Topology) (net.Conn, error) {
	var err error
	var conn net.Conn
	listenAddr, _, err := utils.CheckIPPort(userOptions.Listen)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = listener.Close() // don't forget close the listener
	}()

	var sMessage, rMessage protocol.Message

	// just say hi!
	hiMess := &protocol.HIMess{
		GreetingLen: uint16(len(public.HandshakeRes)),
		Greeting:    public.HandshakeRes,
		UUIDLen:     uint16(len(protocol.AdminUuid)),
		UUID:        protocol.AdminUuid,
		IsAdmin:     1,
		IsReconnect: 0,
	}

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    protocol.TempUuid,
		MessageType: protocol.MessageTypeHI,
		RouteLen:    uint32(len([]byte(protocol.TempRoute))),
		Route:       protocol.TempRoute,
	}

	for {
		conn, err = listener.Accept()
		if err != nil {
			continue
		}

		if err = public.PassivePreAuth(conn, userOptions.Secret); err != nil {
			_ = conn.Close()
			continue
		}

		//成功接收到连接之后判断下游协议，构造message对象
		rMessage = protocol.PrepareAndDecideWhichRProtoFromLower(conn, userOptions.Secret, protocol.AdminUuid)
		//从conn中读取信息，然后获取头和message
		fHeader, fMessage, err := protocol.DestructMessage(rMessage)

		if err != nil {
			_ = conn.Close()
			continue
		}

		if fHeader.MessageType == protocol.MessageTypeHI {
			mess := fMessage.(*protocol.HIMess)
			if mess.Greeting == public.HandshakeReq && mess.IsAdmin == 0 { //hello正确，我们返回保持连接
				sMessage = protocol.PrepareAndDecideWhichSProtoToLower(conn, userOptions.Secret, protocol.AdminUuid)
				protocol.ConstructMessage(sMessage, header, hiMess, false)
				sMessage.SendMessage()
				if mess.IsReconnect == 0 { //如果不是重连，生成并通知子节点id
					node := topology.NewNode(dispatchUUID(conn, userOptions.Secret), conn.RemoteAddr().String())
					t.AddNode(true, node, protocol.TempUuid)
				} else {
					node := topology.NewNode(mess.UUID, conn.RemoteAddr().String())
					t.AddNode(true, node, protocol.TempUuid)
				}
				return conn, nil
			}
		}
		_ = conn.Close()
	}
}
