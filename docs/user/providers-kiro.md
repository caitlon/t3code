# Kiro

T3 Code runs the [Kiro CLI](https://kiro.dev/docs/cli/) as an agent, using your own Kiro account,
custom agents, steering, and MCP configuration.

## Set Up Kiro

1. Install the Kiro CLI on the machine running the T3 Code server
   (`curl -fsSL https://cli.kiro.dev/install | bash`).
2. Run `kiro-cli login` once in a terminal. Kiro Pro and higher plans can instead set an API key as
   `KIRO_API_KEY` in the Kiro provider's environment variables.
3. Open **Settings → Providers**, enable Kiro, and refresh it.

If `kiro-cli` is not on the server's `PATH`, set its **Binary path**. The installer usually puts it
in `~/.local/bin`. T3 Code starts Kiro's CLI V3 engine, so a Kiro CLI that cannot run V3 will not
start.

## Models

The **Kiro default** model keeps the model Kiro picks for the session. T3 Code does not list Kiro's
models yet; to choose one, add its Kiro model ID as a custom model. A model your account does not
offer stops the turn with an error instead of running on another model.

## Permission Modes

Kiro runs in its own two postures:

- **Supervised** turns Kiro's Autopilot off. Kiro asks before changes, and its requests come to you
  for approval.
- **Full access** turns Autopilot on, so Kiro runs tools without asking.

Kiro offers no **Auto** or **Auto-accept edits**. Approvals offer only the choices Kiro sends. Kiro's
"always allow" saves a rule for the whole workspace, so T3 Code does not offer it as a
session-wide choice.
