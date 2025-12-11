package cmds

import (
	"context"
	"crypto/tls"

	"github.com/ProtoconNet/mitum-currency/v3/digest"
	isaacnetwork "github.com/ProtoconNet/mitum2/isaac/network"
	"github.com/ProtoconNet/mitum2/launch"
	"github.com/ProtoconNet/mitum2/network/quicmemberlist"
	"github.com/ProtoconNet/mitum2/util"
	"github.com/ProtoconNet/mitum2/util/logging"
)

func ProcessStartAPI(ctx context.Context) (context.Context, error) {
	var nt *digest.HTTP2Server
	if err := util.LoadFromContext(ctx, digest.ContextValueDigestNetwork, &nt); err != nil {
		return ctx, err
	}
	if nt == nil {
		return ctx, nil
	}

	return ctx, nt.Start(ctx)
}

func ProcessAPI(ctx context.Context) (context.Context, error) {
	var nodeDesign launch.NodeDesign
	var design digest.YamlDigestDesign
	var log *logging.Logging

	if err := util.LoadFromContext(ctx,
		launch.DesignContextKey, &nodeDesign,
		digest.ContextValueDigestDesign, &design,
		launch.LoggingContextKey, &log,
	); err != nil {
		return ctx, err
	}

	if design.Equal(digest.YamlDigestDesign{}) {
		log.Log().Debug().Msg("digest api disabled; empty network")

		return ctx, nil
	}

	//var st *digest.Database
	//if err := util.LoadFromContextOK(ctx, digest.ContextValueDigestDatabase, &st); err != nil {
	//	log.Log().Debug().Err(err).Msg("digest api disabled; empty database")
	//
	//	return ctx, nil
	//} else if st == nil {
	//	log.Log().Debug().Msg("digest api disabled; empty database")
	//
	//	return ctx, nil
	//}

	log.Log().Info().
		Str("bind", design.Network().Bind().String()).
		Str("publish", design.Network().ConnInfo().String()).
		Msg("trying to start http2 server for digest API")

	var params *launch.LocalParams
	var memberList *quicmemberlist.Memberlist
	var nodeList = design.ConnInfo
	if err := util.LoadFromContextOK(ctx,
		launch.LocalParamsContextKey, &params,
		launch.MemberlistContextKey, &memberList,
	); err != nil {
		return nil, err
	}

	connectionPool, err := launch.NewConnectionPool(
		1<<9,
		params.ISAAC.NetworkID(),
		nil,
	)
	if err != nil {
		return nil, err
	}

	client := isaacnetwork.NewBaseClient( //nolint:gomnd //...
		encs, enc,
		connectionPool.Dial,
		connectionPool.CloseAll,
	)

	var nt *digest.HTTP2Server
	var certs []tls.Certificate
	if design.Network().Bind().Scheme == "https" {
		certs = design.Network().Certs()
	}

	if sv, err := digest.NewHTTP2Server(
		design.Network().Bind().Host,
		design.Network().ConnInfo().URL().Host,
		certs,
		encs,
		params.ISAAC.NetworkID(),
	); err != nil {
		return ctx, err
	} else if err := sv.Initialize(); err != nil {
		return ctx, err
	} else {
		nt = sv
	}

	nt.SetNetworkClient(client, memberList, nodeList)

	return context.WithValue(ctx, digest.ContextValueDigestNetwork, nt), nil
}
