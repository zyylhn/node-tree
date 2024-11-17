package manager

import (
	"github.com/zyylhn/node-tree/protocol"
	"github.com/zyylhn/node-tree/public"
)

func RefreshAgentInfo(uuid, route string) {
	sMessage := protocol.PrepareAndDecideWhichSProtoToLower(public.DownstreamConnection.Conn, public.DownstreamConnection.Secret, public.DownstreamConnection.UUID)

	header := &protocol.Header{
		Sender:      protocol.AdminUuid,
		Acceptor:    uuid,
		MessageType: protocol.MessageTypeGetAgentInfoReq,
		RouteLen:    uint32(len([]byte(route))),
		Route:       route,
	}
	myMemoMess := &protocol.GetAgentInfo{}
	protocol.ConstructMessage(sMessage, header, myMemoMess, false)
	sMessage.SendMessage()
}
