import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import { resolveSelfInvocation, type SelfInvocation } from "@t3tools/shared/nodeRuntime";
import {
  KiroSettings,
  ProviderDriverKind,
  type OrchestrationV2ProviderCapabilities,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import type * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import { ChildProcessSpawner } from "effect/unstable/process";
import * as EffectAcpErrors from "effect-acp/errors";

import * as ServerConfig from "../../config.ts";
import { makeAcpNativeLoggerFactory } from "../../provider/acp/AcpNativeLogging.ts";
import type * as AcpSessionRuntime from "../../provider/acp/AcpSessionRuntime.ts";
import {
  KIRO_AUTOPILOT_CONFIG_ID,
  kiroApprovalOptions,
  makeKiroAcpRuntime,
} from "../../provider/acp/KiroAcpSupport.ts";
import * as ProviderEventLoggers from "../../provider/Layers/ProviderEventLoggers.ts";
import { mergeProviderInstanceEnvironment } from "../../provider/ProviderInstanceEnvironment.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  ProviderAdapterDriverCreateError,
  type ProviderAdapterDriver,
  type ProviderAdapterDriverCreateInput,
} from "../ProviderAdapterDriver.ts";
import {
  AcpProviderCapabilitiesV2,
  makeAcpAdapterV2,
  type AcpAdapterV2Flavor,
  type AcpAdapterV2RuntimeInput,
} from "./AcpAdapterV2.ts";

export const KIRO_PROVIDER = ProviderDriverKind.make("kiro");
const DEFAULT_KIRO_SETTINGS = Schema.decodeSync(KiroSettings)({});

const KiroProviderCapabilitiesV2 = {
  ...AcpProviderCapabilitiesV2,
  sessions: {
    ...AcpProviderCapabilitiesV2.sessions,
    supportsModelSwitchInSession: true,
  },
  tools: {
    ...AcpProviderCapabilitiesV2.tools,
    supportsMcpTools: true,
  },
} satisfies OrchestrationV2ProviderCapabilities;

type KiroRuntime = AcpSessionRuntime.AcpSessionRuntime["Service"];

export interface KiroAdapterV2Options {
  readonly instanceId: Parameters<typeof makeAcpAdapterV2>[0]["instanceId"];
  readonly settings: KiroSettings;
  readonly environment: NodeJS.ProcessEnv;
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly crypto: Crypto.Crypto;
  readonly selfInvocation: SelfInvocation;
  readonly fileSystem: FileSystem.FileSystem;
  readonly idAllocator: IdAllocator.IdAllocatorV2["Service"];
  readonly serverConfig: ServerConfig.ServerConfig["Service"];
  readonly nativeLogging?: Parameters<typeof makeAcpAdapterV2>[0]["nativeLogging"];
  /** Replaces the `kiro-cli` launch (replay tests). Kiro's session setup still applies. */
  readonly makeRuntime?: (
    input: AcpAdapterV2RuntimeInput,
  ) => Effect.Effect<KiroRuntime, EffectAcpErrors.AcpError, Crypto.Crypto | Scope.Scope>;
  readonly assertComplete?: Effect.Effect<void, EffectAcpErrors.AcpError>;
}

/**
 * Kiro starts sessions on Autopilot, where it runs tools without asking. Turn
 * it off so every tool Kiro gates reaches T3, and the thread's runtime policy
 * answers it: Full access approves, Supervised asks the user. This holds for
 * any session the runtime opens, so a runtime serving several threads can
 * never run one of them with another thread's grants.
 */
const superviseKiroSession = (runtime: KiroRuntime) =>
  Effect.gen(function* () {
    const autopilot = (yield* runtime.getConfigOptions).find(
      (option) => option.id === KIRO_AUTOPILOT_CONFIG_ID,
    );
    if (autopilot?.type !== "select" || autopilot.currentValue === "off") return;
    yield* runtime.setConfigOption(KIRO_AUTOPILOT_CONFIG_ID, "off");
  });

export function superviseKiroRuntime(runtime: KiroRuntime): KiroRuntime {
  const supervise = <E>(
    started: Effect.Effect<AcpSessionRuntime.AcpSessionRuntimeStartResult, E>,
  ) => started.pipe(Effect.tap(() => superviseKiroSession(runtime)));
  return {
    ...runtime,
    start: () => supervise(runtime.start()),
    loadSession: (sessionId, options) => supervise(runtime.loadSession(sessionId, options)),
    resumeSession: (sessionId, options) => supervise(runtime.resumeSession(sessionId, options)),
  };
}

/**
 * Kiro V3 selects models through its `model` session config option, and
 * `session/set_model` does not exist. "default" keeps the session's model.
 * A model the session does not offer fails the turn instead of silently
 * running on another one.
 */
const applyKiroModelSelection: NonNullable<AcpAdapterV2Flavor["applyModelSelection"]> = ({
  runtime,
  modelSelection,
}) =>
  Effect.gen(function* () {
    const modelOption = (yield* runtime.getConfigOptions).find(
      (option) => option.id === "model" || option.category === "model",
    );
    const current = modelOption?.type === "select" ? modelOption.currentValue : undefined;
    const requested = modelSelection.model.trim();
    if (requested.length === 0 || requested === "default" || requested === current) {
      return current;
    }
    if (modelOption?.type !== "select") {
      return yield* EffectAcpErrors.AcpRequestError.invalidParams(
        `Kiro did not offer a model choice for this session, so '${requested}' cannot be selected.`,
      );
    }
    const offered = modelOption.options.flatMap((entry) =>
      "value" in entry ? [entry.value] : entry.options.map((choice) => choice.value),
    );
    if (!offered.includes(requested)) {
      return yield* EffectAcpErrors.AcpRequestError.invalidParams(
        `Kiro model '${requested}' is unavailable for this account. Select an available model.`,
      );
    }
    yield* runtime.setConfigOption(modelOption.id, requested);
    return requested;
  });

export function makeKiroAcpAdapterFlavor(options: KiroAdapterV2Options): AcpAdapterV2Flavor {
  const makeRuntime =
    options.makeRuntime ??
    ((input: AcpAdapterV2RuntimeInput) => {
      const { runtimePolicy: _runtimePolicy, processEnvironment, ...runtimeInput } = input;
      return makeKiroAcpRuntime({
        ...runtimeInput,
        childProcessSpawner: options.childProcessSpawner,
        settings: options.settings,
        environment: { ...options.environment, ...processEnvironment },
      });
    });
  return {
    driver: KIRO_PROVIDER,
    runtimeHarness: "Kiro",
    capabilities: KiroProviderCapabilitiesV2,
    makeRuntime: (input) => makeRuntime(input).pipe(Effect.map(superviseKiroRuntime)),
    applyModelSelection: applyKiroModelSelection,
    approvalOptions: kiroApprovalOptions,
    // Kiro V3 advertises `promptCapabilities.image`; the shared adapter reads it.
    ...(options.assertComplete === undefined ? {} : { assertComplete: options.assertComplete }),
  };
}

export function makeKiroAdapterV2(options: KiroAdapterV2Options) {
  return makeAcpAdapterV2({
    instanceId: options.instanceId,
    flavor: makeKiroAcpAdapterFlavor(options),
    crypto: options.crypto,
    fileSystem: options.fileSystem,
    idAllocator: options.idAllocator,
    serverConfig: options.serverConfig,
    selfInvocation: options.selfInvocation,
    ...(options.nativeLogging === undefined ? {} : { nativeLogging: options.nativeLogging }),
  });
}

export type KiroAdapterV2DriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | Path.Path
  | ProviderEventLoggers.ProviderEventLoggers
  | ServerConfig.ServerConfig;

export const KiroAdapterV2Driver: ProviderAdapterDriver<KiroSettings, KiroAdapterV2DriverEnv> = {
  driverKind: KIRO_PROVIDER,
  configSchema: KiroSettings,
  defaultConfig: (): KiroSettings => DEFAULT_KIRO_SETTINGS,
  create: Effect.fn("KiroAdapterV2Driver.create")(
    function* (input: ProviderAdapterDriverCreateInput<KiroSettings>) {
      const hostEnvironment = yield* HostProcessEnvironment;
      const providerEventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
      const makeNativeLogger = yield* makeAcpNativeLoggerFactory();
      return makeKiroAdapterV2({
        instanceId: input.instanceId,
        settings: { ...input.config, enabled: input.enabled },
        environment: mergeProviderInstanceEnvironment(input.environment, hostEnvironment),
        childProcessSpawner: yield* ChildProcessSpawner.ChildProcessSpawner,
        crypto: yield* Crypto.Crypto,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* ServerConfig.ServerConfig,
        selfInvocation: yield* resolveSelfInvocation(),
        nativeLogging: (threadId) =>
          makeNativeLogger({
            nativeEventLogger: providerEventLoggers.native,
            provider: KIRO_PROVIDER,
            threadId,
          }),
      });
    },
    (effect, input) =>
      effect.pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterDriverCreateError({
              driver: KIRO_PROVIDER,
              instanceId: input.instanceId,
              detail: "Failed to create Kiro ACP adapter.",
              cause,
            }),
        ),
      ),
  ),
};
