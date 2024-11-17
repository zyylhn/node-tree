package public

import (
	"net"

	"github.com/zyylhn/node-tree/protocol"
)

// DownstreamConnection 子节点与上级节点的连接和密码
var DownstreamConnection *protocol.MessageComponent

var UpstreamConnection *protocol.MessageComponent

func InitUpstreamConnection(conn net.Conn, secret, uuid string) {
	UpstreamConnection = &protocol.MessageComponent{
		Secret: secret,
		Conn:   conn,
		UUID:   uuid,
	}
}

func UpdateUpstreamConnection(conn net.Conn) {
	UpstreamConnection.Conn = conn
}

var HandshakeReq = "initial"
var HandshakeRes = "ok"
