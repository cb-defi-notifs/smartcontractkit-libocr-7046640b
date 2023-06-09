package managed

import (
	"context"
	"fmt"

	"github.com/smartcontractkit/libocr/commontypes"
	"github.com/smartcontractkit/libocr/internal/loghelper"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/internal/config/ocr3config"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/internal/managed/limits"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/internal/mercuryshim"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/internal/ocr3/protocol"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/internal/ocr3/serialization"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/internal/shim"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/ocr3types"
	"github.com/smartcontractkit/libocr/offchainreporting2plus/types"
	"github.com/smartcontractkit/libocr/subprocesses"
	"go.uber.org/multierr"
)

// RunManagedMercuryOracle runs a "managed" version of protocol.RunOracle. It handles
// setting up telemetry, garbage collection, configuration updates, translating
// from commontypes.BinaryNetworkEndpoint to protocol.NetworkEndpoint, and
// creation/teardown of reporting plugins.
func RunManagedMercuryOracle(
	ctx context.Context,

	v2bootstrappers []commontypes.BootstrapperLocator,
	configTracker types.ContractConfigTracker,
	contractTransmitter types.ContractTransmitter,
	database ocr3types.Database,
	localConfig types.LocalConfig,
	logger loghelper.LoggerWithContext,
	monitoringEndpoint commontypes.MonitoringEndpoint,
	netEndpointFactory types.BinaryNetworkEndpointFactory,
	offchainConfigDigester types.OffchainConfigDigester,
	offchainKeyring types.OffchainKeyring,
	onchainKeyring types.OnchainKeyring,
	mercuryPluginFactory ocr3types.MercuryPluginFactory,
) {
	subs := subprocesses.Subprocesses{}
	defer subs.Wait()

	var chTelemetrySend chan<- *serialization.TelemetryWrapper
	{
		chTelemetry := make(chan *serialization.TelemetryWrapper, 100)
		chTelemetrySend = chTelemetry
		subs.Go(func() {
			forwardTelemetry(ctx, logger, monitoringEndpoint, chTelemetry)
		})
	}

	runWithContractConfig(
		ctx,

		configTracker,
		database,
		func(ctx context.Context, contractConfig types.ContractConfig, logger loghelper.LoggerWithContext) {
			skipResourceExhaustionChecks := localConfig.DevelopmentMode == types.EnableDangerousDevelopmentMode

			fromAccount, err := contractTransmitter.FromAccount()
			if err != nil {
				logger.Error("ManagedMercuryOracle: error getting FromAccount", commontypes.LogFields{
					"error": err,
				})
				return
			}

			sharedConfig, oid, err := ocr3config.SharedConfigFromContractConfig(
				skipResourceExhaustionChecks,
				contractConfig,
				offchainKeyring,
				onchainKeyring,
				netEndpointFactory.PeerID(),
				fromAccount,
			)
			if err != nil {
				logger.Error("ManagedMercuryOracle: error while updating config", commontypes.LogFields{
					"error": err,
				})
				return
			}

			// Run with new config
			peerIDs := []string{}
			for _, identity := range sharedConfig.OracleIdentities {
				peerIDs = append(peerIDs, identity.PeerID)
			}

			childLogger := logger.MakeChild(commontypes.LogFields{
				"oid": oid,
			})

			mercuryPlugin, mercuryPluginInfo, err := mercuryPluginFactory.NewMercuryPlugin(ocr3types.MercuryPluginConfig{
				sharedConfig.ConfigDigest,
				oid,
				sharedConfig.N(),
				sharedConfig.F,
				sharedConfig.OnchainConfig,
				sharedConfig.ReportingPluginConfig,
				sharedConfig.MinRoundInterval(),
				sharedConfig.MaxDurationObservation,
			})
			if err != nil {
				logger.Error("ManagedMercuryOracle: error during NewReportingPlugin()", commontypes.LogFields{
					"error": err,
				})
				return
			}
			defer loghelper.CloseLogError(
				mercuryPlugin,
				logger,
				"ManagedMercuryOracle: error during reportingPlugin.Close()",
			)

			if err := validateMercuryPluginLimits(mercuryPluginInfo.Limits); err != nil {
				logger.Error("ManagedMercuryOracle: invalid MercuryPluginInfo", commontypes.LogFields{
					"error":             err,
					"mercuryPluginInfo": mercuryPluginInfo,
				})
				return
			}

			ocr3PluginLimits := mercuryshim.OCR3PluginLimits(mercuryPluginInfo.Limits)

			lims, err := limits.OCR3Limits(sharedConfig.PublicConfig, ocr3PluginLimits, onchainKeyring.MaxSignatureLength())
			if err != nil {
				logger.Error("ManagedMercuryOracle: error during limits", commontypes.LogFields{
					"error":            err,
					"publicConfig":     sharedConfig.PublicConfig,
					"ocr3PluginLimits": ocr3PluginLimits,
					"maxSigLen":        onchainKeyring.MaxSignatureLength(),
				})
				return
			}
			binNetEndpoint, err := netEndpointFactory.NewEndpoint(
				sharedConfig.ConfigDigest,
				peerIDs,
				v2bootstrappers,
				sharedConfig.F,
				lims,
			)
			if err != nil {
				logger.Error("ManagedMercuryOracle: error during NewEndpoint", commontypes.LogFields{
					"error":           err,
					"peerIDs":         peerIDs,
					"v2bootstrappers": v2bootstrappers,
				})
				return
			}

			// No need to binNetEndpoint.Start/Close since netEndpoint will handle that for us

			netEndpoint := shim.NewOCR3SerializingEndpoint[mercuryshim.MercuryReportInfo](
				chTelemetrySend,
				sharedConfig.ConfigDigest,
				binNetEndpoint,
				onchainKeyring.MaxSignatureLength(),
				childLogger,
				ocr3PluginLimits,
				sharedConfig.N(),
				sharedConfig.F,
			)
			if err := netEndpoint.Start(); err != nil {
				logger.Error("ManagedMercuryOracle: error during netEndpoint.Start()", commontypes.LogFields{
					"error":        err,
					"configDigest": sharedConfig.ConfigDigest,
				})
				return
			}
			defer loghelper.CloseLogError(
				netEndpoint,
				logger,
				"ManagedMercuryOracle: error during netEndpoint.Close()",
			)

			ocr3PluginConfig := ocr3types.ReportingPluginConfig{
				sharedConfig.ConfigDigest,
				oid,
				sharedConfig.N(),
				sharedConfig.F,
				sharedConfig.OnchainConfig,
				sharedConfig.ReportingPluginConfig,
				sharedConfig.DeltaRound,
				sharedConfig.MaxDurationQuery,
				sharedConfig.MaxDurationObservation,
				sharedConfig.MaxDurationShouldAcceptAttestedReport,
				sharedConfig.MaxDurationShouldTransmitAcceptedReport,
			}
			ocr3Plugin := &mercuryshim.MercuryOCR3Plugin{
				ocr3PluginConfig,
				mercuryPlugin,
				mercuryPluginInfo.Limits,
			}

			protocol.RunOracle[mercuryshim.MercuryReportInfo](
				ctx,
				sharedConfig,
				mercuryshim.NewMercuryOCR3ContractTransmitter(contractTransmitter),
				&shim.SerializingOCR3Database{database},
				oid,
				localConfig,
				childLogger,
				netEndpoint,
				offchainKeyring,
				mercuryshim.NewMercuryOCR3OnchainKeyring(onchainKeyring),
				shim.LimitCheckOCR3Plugin[mercuryshim.MercuryReportInfo]{ocr3Plugin, ocr3PluginLimits},
				shim.MakeOCR3TelemetrySender(chTelemetrySend, childLogger),
			)
		},
		localConfig,
		logger,
		offchainConfigDigester,
	)
}

func validateMercuryPluginLimits(limits ocr3types.MercuryPluginLimits) error {
	var err error
	if !(0 <= limits.MaxObservationLength && limits.MaxObservationLength <= ocr3types.MaxMaxMercuryObservationLength) {
		err = multierr.Append(err, fmt.Errorf("MaxObservationLength (%v) out of range. Should be between 0 and %v", limits.MaxObservationLength, ocr3types.MaxMaxMercuryObservationLength))
	}
	if !(0 <= limits.MaxReportLength && limits.MaxReportLength <= ocr3types.MaxMaxMercuryReportLength) {
		err = multierr.Append(err, fmt.Errorf("MaxReportLength (%v) out of range. Should be between 0 and %v", limits.MaxReportLength, ocr3types.MaxMaxMercuryReportLength))
	}
	return err
}
