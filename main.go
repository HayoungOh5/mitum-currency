package main

import (
	"context"
	"fmt"
	"os"

	launchcmd "github.com/ProtoconNet/mitum2/launch/cmd"

	"github.com/ProtoconNet/mitum-currency/v3/cmds"
	"github.com/ProtoconNet/mitum-currency/v3/utils"
	"github.com/ProtoconNet/mitum2/base"
	"github.com/ProtoconNet/mitum2/launch"
	"github.com/ProtoconNet/mitum2/util"
	"github.com/ProtoconNet/mitum2/util/logging"
	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
)

var (
	Version   = "v0.0.1"
	BuildTime = "-"
	GitBranch = "master"
	GitCommit = "-"
)

//revive:disable:nested-structs
var CLI struct { //nolint:govet //...
	launch.BaseFlags
	Init      cmds.INITCommand `cmd:"" help:"init node"`
	Run       cmds.RunCommand  `cmd:"" help:"run node"`
	Storage   cmds.Storage     `cmd:""`
	Operation struct {
		Currency cmds.CurrencyCommand `cmd:"" help:"currency operation"`
		Suffrage cmds.SuffrageCommand `cmd:"" help:"suffrage operation"`
		DID      cmds.DIDCommand      `cmd:"" help:"did-registry operation"`
	} `cmd:"" help:"create operation"`
	Network struct {
		Client cmds.NetworkClientCommand `cmd:"" help:"network client"`
	} `cmd:"" help:"network"`
	Key struct {
		New     cmds.KeyNewCommand     `cmd:"" help:"generate new key"`
		Address cmds.KeyAddressCommand `cmd:"" help:"generate address from key"`
		Load    cmds.KeyLoadCommand    `cmd:"" help:"load key"`
		Sign    cmds.KeySignCommand    `cmd:"" help:"sign"`
	} `cmd:"" help:"key"`
	Handover launchcmd.HandoverCommands `cmd:""`
	Version  struct{}                   `cmd:"" help:"version"`
}

//revive:enable:nested-structs

var flagDefaults = kong.Vars{
	"log_out":                           "stderr",
	"log_format":                        "terminal",
	"log_level":                         "debug",
	"log_force_color":                   "false",
	"design_uri":                        launch.DefaultDesignURI,
	"create_account_threshold":          "100",
	"create_contract_account_threshold": "100",
	"suffrage_candidate_limiter_limit":  "77",
	"max_operation_in_proposal":         "99",
	"suffrage candidate lifespan":       "33",
	"max suffrage size":                 "33",
	"suffrage expel lifespan":           "44",
	"safe_threshold":                    base.SafeThreshold.String(),
	"network_id":                        "mitum",
}

func main() {
	kctx := kong.Parse(&CLI, flagDefaults)

	bi, err := util.ParseBuildInfo(Version, GitBranch, GitCommit, BuildTime)
	if err != nil {
		kctx.FatalIfErrorf(err)
	}

	if kctx.Command() == "version" {
		_, _ = fmt.Fprintln(os.Stdout, bi.String())

		return
	}
	pctx := util.ContextWithValues(context.Background(), map[util.ContextKey]interface{}{
		launch.VersionContextKey:     bi.Version,
		launch.FlagsContextKey:       CLI.BaseFlags,
		launch.KongContextContextKey: kctx,
	})

	pss := launch.DefaultMainPS()

	switch i, err := pss.Run(pctx); {
	case err != nil:
		kctx.FatalIfErrorf(err)
	default:
		pctx = i

		kctx = kong.Parse(&CLI, kong.BindTo(pctx, (*context.Context)(nil)), flagDefaults)
	}

	var log *logging.Logging
	if err := util.LoadFromContextOK(pctx, launch.LoggingContextKey, &log); err != nil {
		kctx.FatalIfErrorf(err)
	}

	log.Log().Debug().Interface("flags", os.Args).Msg("flags")
	log.Log().Debug().Interface("main_process", pss.Verbose()).Msg("processed")

	go func() {
		// 만약 InitMetrics에 인자가 필요 없다면 ()
		// 만약 포트 번호 등을 넘겨야 한다면 (CLI.MetricsPort) 등으로 전달
		if err := utils.InitMetrics(); err != nil {
			log.Log().Error().Err(err).Msg("failed to initialize metrics server")
		}
	}()

	if err := func() error {
		defer log.Log().Debug().Msg("stopped")

		return errors.WithStack(kctx.Run(pctx))
	}(); err != nil {
		log.Log().Error().Err(err).Msg("stopped by error")

		kctx.FatalIfErrorf(err)
	}
}
