package digest

import (
	isaacnetwork "github.com/ProtoconNet/mitum2/isaac/network"
	"github.com/ProtoconNet/mitum2/network/quicmemberlist"
	"github.com/ProtoconNet/mitum2/network/quicstream"
)

func (hd *Handlers) SetNetworkClient(
	client *isaacnetwork.BaseClient,
	memberList *quicmemberlist.Memberlist,
	nodeList []quicstream.ConnInfo,
) *Handlers {
	hd.baseClient = client
	hd.memberList = memberList
	hd.staticNodeList = nodeList

	return hd
}
