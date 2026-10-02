import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import {
  KiroSettings,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  type ProviderTurnId,
  RunAttemptId,
  RunId,
  type RuntimeMode,
  type RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import { resolveSelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  type ProviderAdapterV2Error,
  type ProviderAdapterV2Event,
  ProviderAdapterV2RuntimePolicy,
} from "../ProviderAdapter.ts";
import {
  decodeAcpReplayTranscript,
  makeAcpReplayCompletenessAssertion,
  makeAcpReplayRuntime,
} from "./AcpAdapterV2.testkit.ts";
import { KIRO_PROVIDER, makeKiroAdapterV2 } from "./KiroAdapterV2.ts";

const testLayer = Layer.mergeAll(
  NodeServices.layer,
  IdAllocator.layer,
  ServerConfig.layerTest(process.cwd(), { prefix: "t3-kiro-v2-adapter-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
);

const SESSION_ID = "sess_abb5e0cf-d4a2-4360-9f03-2f8f0889a707";
const ENABLED_KIRO_SETTINGS = Schema.decodeSync(KiroSettings)({ enabled: true });

type Frame = Record<string, unknown>;
const outbound = (method: string, params: unknown = "<any>", label = method): Frame => ({
  type: "expect_outbound",
  label,
  frame: { kind: "request", method, params },
});
const outboundNotification = (method: string, params: unknown): Frame => ({
  type: "expect_outbound",
  label: method,
  frame: { kind: "notification", method, params },
});
const outboundResponse = (method: string, result: unknown): Frame => ({
  type: "expect_outbound",
  label: `${method}.response`,
  frame: { kind: "response", method, result },
});
const answer = (method: string, result: unknown, label = `${method}.result`): Frame => ({
  type: "emit_inbound",
  label,
  frame: { kind: "response", method, result },
});
const agentRequest = (method: string, params: unknown): Frame => ({
  type: "emit_inbound",
  label: method,
  frame: { kind: "request", method, params },
});
const update = (sessionUpdate: Record<string, unknown>): Frame => ({
  type: "emit_inbound",
  label: `update.${String(sessionUpdate.sessionUpdate)}`,
  frame: {
    kind: "notification",
    method: "session/update",
    params: { sessionId: SESSION_ID, update: sessionUpdate },
  },
});
const kiroNotification = (method: string, params: unknown): Frame => ({
  type: "emit_inbound",
  label: method,
  frame: { kind: "notification", method, params },
});

/**
 * Kiro CLI 2.27.0 (KAS 0.66.22) `initialize` result, recorded with
 * `kiro-cli acp --agent-engine=v3 --auth-method=cli`. Only the extension
 * method list is shortened and the log paths dropped.
 */
const KIRO_V3_INITIALIZE = {
  protocolVersion: 1,
  agentCapabilities: {
    loadSession: true,
    promptCapabilities: { image: true, embeddedContext: true },
    mcpCapabilities: { http: true, sse: true },
    sessionCapabilities: {
      list: {},
      close: {},
      delete: {},
      fork: { _meta: { kiro: { messageId: true } } },
    },
    _meta: {
      kiro: {
        checkpoints: true,
        sessionList: true,
        policyNotifications: true,
        extensionMethods: ["_kiro/session/context", "_kiro/session/compact", "_kiro/knowledge"],
        sessionSources: ["local", "remote"],
        sessionListScopes: ["workspace", "user"],
        executionTargets: ["local", "cloud-sandbox"],
        replayMarking: true,
      },
    },
  },
  authMethods: [
    { id: "aws-builder-id", name: "AWS Builder ID" },
    { id: "aws-iam-identity-center", name: "AWS IAM Identity Center" },
  ],
};

/** A deployment that advertises no Kiro extensions at all. */
const KIRO_V3_INITIALIZE_WITHOUT_EXTENSIONS = {
  protocolVersion: 1,
  agentCapabilities: {
    loadSession: true,
    promptCapabilities: { image: true, embeddedContext: true },
    mcpCapabilities: { http: true, sse: true },
  },
  authMethods: [],
};

const option = (value: string, name = value) => ({ value, name });
const autopilotOption = (currentValue: "on" | "off") => ({
  type: "select",
  id: "autopilot",
  name: "Autopilot",
  currentValue,
  options: [option("on", "Autopilot"), option("off", "Supervised")],
});
const modeOption = {
  type: "select",
  id: "mode",
  name: "Mode",
  category: "mode",
  currentValue: "vibe",
  options: [option("vibe", "Default"), option("spec", "Spec"), option("plan", "Plan")],
};
// Recorded sessions without a signed-in account carry no `model` option; the
// migration guide documents it as `configId: "model"`.
const modelOption = (currentValue: string) => ({
  type: "select",
  id: "model",
  name: "Model",
  category: "model",
  currentValue,
  options: [option("auto", "Auto"), option("claude-sonnet", "Claude Sonnet")],
});

/** Kiro's `session/new` result, recorded from 2.27.0 and trimmed to what T3 reads. */
const sessionSetup = (configOptions: ReadonlyArray<unknown>) => ({
  _meta: { schemaVersion: "1.0.0", id: SESSION_ID, agentMode: "vibe", source: "local" },
  sessionId: SESSION_ID,
  modes: {
    currentModeId: "vibe",
    availableModes: [
      { id: "vibe", name: "Default" },
      { id: "spec", name: "Spec" },
      { id: "plan", name: "Plan" },
    ],
  },
  configOptions,
});

/** What Kiro sends right after `initialize` and around `session/new`, all unknown to T3. */
const kiroSessionNoise: ReadonlyArray<Frame> = [
  kiroNotification("_kiro/mcp/status", { sessionId: SESSION_ID, servers: [] }),
  kiroNotification("_kiro/sessions/changed", { upserted: [], deleted: [] }),
];

// Kiro advertises `sessionCapabilities.close`, so T3 closes the session on teardown.
const closeSession: ReadonlyArray<Frame> = [
  outbound("session/close", { sessionId: SESSION_ID }),
  answer("session/close", {}),
];

const turnPrompt = outbound("session/prompt", {
  sessionId: SESSION_ID,
  prompt: "<any>",
});

const openSessionFrames = (input: {
  readonly initialize: unknown;
  readonly configOptions: ReadonlyArray<unknown>;
}): ReadonlyArray<Frame> => [
  outbound("initialize"),
  answer("initialize", input.initialize),
  outbound("session/new", { cwd: "<workspace>", mcpServers: "<any>" }),
  ...kiroSessionNoise,
  answer("session/new", sessionSetup(input.configOptions)),
  // Supervised threads run Kiro with Autopilot off, its native ask-first posture.
  outbound("session/set_config_option", {
    sessionId: SESSION_ID,
    configId: "autopilot",
    value: "off",
  }),
  answer("session/set_config_option", {
    configOptions: input.configOptions.map((entry) =>
      (entry as { id: string }).id === "autopilot" ? autopilotOption("off") : entry,
    ),
  }),
];

const runKiroScript = Effect.fn("runKiroScript")(function* (input: {
  readonly scenario: string;
  readonly frames: ReadonlyArray<Frame>;
  readonly model?: string;
  readonly runtimeMode?: RuntimeMode;
  readonly drive: (session: {
    readonly events: Stream.Stream<ProviderAdapterV2Event, ProviderAdapterV2Error>;
    readonly startTurn: Effect.Effect<void, ProviderAdapterV2Error>;
    readonly interrupt: (
      providerTurnId: ProviderTurnId,
    ) => Effect.Effect<void, ProviderAdapterV2Error>;
    readonly respond: (
      requestId: RuntimeRequestId,
      decision: "accept" | "decline",
    ) => Effect.Effect<void, ProviderAdapterV2Error>;
  }) => Effect.Effect<void, ProviderAdapterV2Error>;
}) {
  const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const replayDir = yield* fileSystem.makeTempDirectoryScoped({
    prefix: `t3-kiro-${input.scenario}-`,
  });
  const statusPath = path.join(replayDir, "status.json");
  const transcript = yield* decodeAcpReplayTranscript(
    {
      provider: KIRO_PROVIDER,
      protocol: "acp.ndjson-jsonrpc",
      version: "1",
      scenario: input.scenario,
      entries: input.frames as never,
    },
    KIRO_PROVIDER,
  );
  const instanceId = ProviderInstanceId.make(`kiro-${input.scenario}`);
  const adapter = makeKiroAdapterV2({
    instanceId,
    settings: ENABLED_KIRO_SETTINGS,
    environment: {},
    childProcessSpawner,
    crypto: yield* Crypto.Crypto,
    fileSystem,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig: yield* ServerConfig.ServerConfig,
    selfInvocation: yield* resolveSelfInvocation(),
    makeRuntime: makeAcpReplayRuntime({
      transcript,
      statusPath,
      scriptPath: yield* path.fromFileUrl(
        new URL("../../../scripts/acp-replay-agent.ts", import.meta.url),
      ),
      childProcessSpawner,
      fileSystem,
    }),
  });
  const threadId = ThreadId.make(`thread-kiro-${input.scenario}`);
  const modelSelection = { instanceId, model: input.model ?? "default" };
  const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
    runtimeMode: input.runtimeMode ?? "approval-required",
    interactionMode: "default",
    cwd: replayDir,
  });
  yield* Effect.gen(function* () {
    const session = yield* adapter.openSession({
      threadId,
      providerSessionId: ProviderSessionId.make(`provider-session-kiro-${input.scenario}`),
      modelSelection,
      runtimePolicy,
    });
    const providerThread = yield* session.ensureThread({ threadId, modelSelection, runtimePolicy });
    const now = yield* DateTime.now;
    const suffix = `${threadId}:1`;
    yield* input.drive({
      events: session.events,
      startTurn: session.startTurn({
        appThread: {
          createdBy: "user",
          creationSource: "web",
          id: threadId,
          projectId: ProjectId.make(`project:${threadId}`),
          title: "Kiro adapter test",
          providerInstanceId: instanceId,
          modelSelection,
          runtimeMode: runtimePolicy.runtimeMode,
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          activeProviderThreadId: providerThread.id,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          settledOverride: null,
          settledAt: null,
          lastVisitedAt: null,
          deletedAt: null,
        },
        threadId,
        runId: RunId.make(`run:${suffix}`),
        runOrdinal: 1,
        providerTurnOrdinal: 1,
        attemptId: RunAttemptId.make(`attempt:${suffix}`),
        rootNodeId: NodeId.make(`node:${suffix}`),
        providerThread,
        message: {
          createdBy: "user",
          creationSource: "web",
          messageId: MessageId.make(`message:${suffix}`),
          text: "Say hello",
          attachments: [],
        },
        modelSelection,
        runtimePolicy,
      }),
      interrupt: (providerTurnId) => session.interruptTurn({ providerThread, providerTurnId }),
      respond: (requestId, decision) => session.respondToRuntimeRequest({ requestId, decision }),
    });
  }).pipe(Effect.scoped);
  // Closing the session stops the replay agent; every scripted frame must be used.
  yield* makeAcpReplayCompletenessAssertion(fileSystem, statusPath, transcript);
});

const collectTurn = (events: Stream.Stream<ProviderAdapterV2Event, ProviderAdapterV2Error>) =>
  events.pipe(
    Stream.takeUntil((event) => event.type === "turn.terminal"),
    Stream.runCollect,
    Effect.map((collected) => Array.from(collected)),
  );

const assistantText = (events: ReadonlyArray<ProviderAdapterV2Event>) =>
  events
    .flatMap((event) =>
      event.type === "turn_item.updated" && event.turnItem.type === "assistant_message"
        ? [event.turnItem]
        : [],
    )
    .at(-1);

const terminalStatus = (events: ReadonlyArray<ProviderAdapterV2Event>) => {
  const terminal = events.find((event) => event.type === "turn.terminal");
  return terminal?.type === "turn.terminal" ? terminal.status : undefined;
};

describe("KiroAdapterV2", () => {
  it.effect("settles a turn from the session/prompt response, not from Kiro's turn_end info", () =>
    runKiroScript({
      scenario: "prompt-settles",
      frames: [
        ...openSessionFrames({
          initialize: KIRO_V3_INITIALIZE,
          configOptions: [modeOption, autopilotOption("on")],
        }),
        turnPrompt,
        update({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Hello" },
        }),
        // V3 may report turn_end and keep working; only the response ends the turn.
        update({ sessionUpdate: "session_info_update", _meta: { kiro: { kind: "turn_end" } } }),
        update({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: " from Kiro." },
        }),
        update({
          sessionUpdate: "session_info_update",
          _meta: { kiro: { kind: "turn_completion", status: "completed" } },
        }),
        kiroNotification("_kiro/progressive_context/items_changed", {
          sessionId: SESSION_ID,
          status: "success",
          items: [],
        }),
        answer("session/prompt", { stopReason: "end_turn" }),
        ...closeSession,
      ],
      drive: ({ events, startTurn }) =>
        Effect.gen(function* () {
          yield* startTurn;
          const turn = yield* collectTurn(events);
          assert.equal(terminalStatus(turn), "completed");
          const message = assistantText(turn);
          assert.equal(
            message?.type === "assistant_message" ? message.text : undefined,
            "Hello from Kiro.",
          );
        }),
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("opens a session on a deployment that advertises no Kiro extensions", () =>
    runKiroScript({
      scenario: "no-extensions",
      frames: [
        outbound("initialize"),
        answer("initialize", KIRO_V3_INITIALIZE_WITHOUT_EXTENSIONS),
        outbound("session/new", { cwd: "<workspace>", mcpServers: "<any>" }),
        // No autopilot option advertised, so T3 sends no config write.
        answer("session/new", sessionSetup([modeOption])),
        turnPrompt,
        update({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "ok" },
        }),
        answer("session/prompt", { stopReason: "end_turn" }),
      ],
      drive: ({ events, startTurn }) =>
        Effect.gen(function* () {
          yield* startTurn;
          assert.equal(terminalStatus(yield* collectTurn(events)), "completed");
        }),
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("keeps Kiro's Autopilot on for a Full access thread", () =>
    runKiroScript({
      scenario: "full-access-autopilot",
      runtimeMode: "full-access",
      frames: [
        outbound("initialize"),
        answer("initialize", KIRO_V3_INITIALIZE),
        outbound("session/new", { cwd: "<workspace>", mcpServers: "<any>" }),
        // Kiro opens on Autopilot, which is already what Full access asks for, so no
        // session/set_config_option follows: the replay agent fails on any unscripted frame.
        answer("session/new", sessionSetup([modeOption, autopilotOption("on")])),
        turnPrompt,
        answer("session/prompt", { stopReason: "end_turn" }),
        ...closeSession,
      ],
      drive: ({ events, startTurn }) =>
        Effect.gen(function* () {
          yield* startTurn;
          assert.equal(terminalStatus(yield* collectTurn(events)), "completed");
        }),
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("switches models with session/set_config_option, never session/set_model", () =>
    runKiroScript({
      scenario: "model-config-option",
      model: "claude-sonnet",
      frames: [
        outbound("initialize"),
        answer("initialize", KIRO_V3_INITIALIZE),
        outbound("session/new", { cwd: "<workspace>", mcpServers: "<any>" }),
        answer(
          "session/new",
          sessionSetup([modeOption, modelOption("auto"), autopilotOption("on")]),
        ),
        outbound("session/set_config_option", {
          sessionId: SESSION_ID,
          configId: "model",
          value: "claude-sonnet",
        }),
        answer("session/set_config_option", {
          configOptions: [modeOption, modelOption("claude-sonnet"), autopilotOption("on")],
        }),
        outbound("session/set_config_option", {
          sessionId: SESSION_ID,
          configId: "autopilot",
          value: "off",
        }),
        answer("session/set_config_option", {
          configOptions: [modeOption, modelOption("claude-sonnet"), autopilotOption("off")],
        }),
        turnPrompt,
        answer("session/prompt", { stopReason: "end_turn" }),
        ...closeSession,
      ],
      drive: ({ events, startTurn }) =>
        Effect.gen(function* () {
          yield* startTurn;
          assert.equal(terminalStatus(yield* collectTurn(events)), "completed");
        }),
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("refuses a model the session does not offer instead of running on another", () =>
    Effect.gen(function* () {
      const exit = yield* runKiroScript({
        scenario: "unknown-model",
        model: "not-a-kiro-model",
        frames: [
          ...openSessionFrames({
            initialize: KIRO_V3_INITIALIZE,
            configOptions: [modeOption, modelOption("auto"), autopilotOption("on")],
          }),
          ...closeSession,
        ],
        drive: () => Effect.die("the session must not open"),
      }).pipe(Effect.exit);
      assert.isTrue(Exit.isFailure(exit));
      assert.include(String(Exit.isFailure(exit) ? exit.cause : ""), "not-a-kiro-model");
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("cancels a running turn with session/cancel and settles it as interrupted", () =>
    runKiroScript({
      scenario: "cancel",
      frames: [
        ...openSessionFrames({
          initialize: KIRO_V3_INITIALIZE,
          configOptions: [modeOption, autopilotOption("on")],
        }),
        turnPrompt,
        update({
          sessionUpdate: "agent_message_chunk",
          content: { type: "text", text: "Working" },
        }),
        outboundNotification("session/cancel", { sessionId: SESSION_ID }),
        answer("session/prompt", { stopReason: "cancelled" }),
        ...closeSession,
      ],
      drive: ({ events, startTurn, interrupt }) =>
        Effect.gen(function* () {
          yield* startTurn;
          const started = Option.getOrThrow(
            yield* events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "turn_item.updated" && event.turnItem.type === "assistant_message",
              ),
              Stream.runHead,
            ),
          );
          if (started.type !== "turn_item.updated" || started.turnItem.providerTurnId === null) {
            return yield* Effect.die("expected the turn's first assistant chunk");
          }
          yield* interrupt(started.turnItem.providerTurnId);
          assert.equal(terminalStatus(yield* collectTurn(events)), "interrupted");
        }),
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );

  it.effect("asks the user before a gated tool and answers with Kiro's own option", () =>
    runKiroScript({
      scenario: "permission",
      frames: [
        ...openSessionFrames({
          initialize: KIRO_V3_INITIALIZE,
          configOptions: [modeOption, autopilotOption("on")],
        }),
        turnPrompt,
        update({
          sessionUpdate: "tool_call",
          toolCallId: "call-1",
          title: "Run tests",
          kind: "execute",
          status: "pending",
        }),
        // From the V3 migration guide, including its consent metadata.
        agentRequest("session/request_permission", {
          sessionId: SESSION_ID,
          toolCall: { toolCallId: "call-1", title: "Run tests", status: "pending" },
          options: [
            { optionId: "accept", name: "Allow", kind: "allow_once" },
            { optionId: "always-accept", name: "Always allow", kind: "allow_always" },
            { optionId: "reject", name: "Deny", kind: "reject_once" },
            { optionId: "always-reject", name: "Always deny", kind: "reject_always" },
          ],
          _meta: {
            kiro: {
              consent: {
                capability: "shell",
                resource: "npm run test",
                triggeringResource: "npm run test",
                workspaceRoot: "<workspace>",
                persistableConsent: true,
              },
            },
          },
        }),
        outboundResponse("session/request_permission", {
          outcome: { outcome: "selected", optionId: "accept" },
        }),
        update({
          sessionUpdate: "tool_call_update",
          toolCallId: "call-1",
          status: "completed",
        }),
        answer("session/prompt", { stopReason: "end_turn" }),
        ...closeSession,
      ],
      drive: ({ events, startTurn, respond }) =>
        Effect.gen(function* () {
          yield* startTurn;
          const pending = Option.getOrThrow(
            yield* events.pipe(
              Stream.filter(
                (event) =>
                  event.type === "runtime_request.updated" &&
                  event.runtimeRequest.status === "pending",
              ),
              Stream.runHead,
            ),
          );
          if (pending.type !== "runtime_request.updated") {
            return yield* Effect.die("expected a pending Kiro permission request");
          }
          yield* respond(pending.runtimeRequest.id, "accept");
          assert.equal(terminalStatus(yield* collectTurn(events)), "completed");
        }),
    }).pipe(Effect.provide(testLayer), Effect.scoped),
  );
});
