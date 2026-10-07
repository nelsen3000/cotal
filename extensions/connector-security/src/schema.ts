/**
 * Agent security event schema — adapted from uber/ADR's Sensor schema
 * (Sensor/adr_sensor/schemas/agent_event_schema.py, Apache-2.0).
 *
 * ADR Sensor normalizes telemetry from Claude Code, Cursor, Cline, Codex,
 * Copilot CLI, DeepSeek Harness, Warp, opencode, Gemini CLI, Google Antigravity
 * and Claude Desktop into one AgentEvent shape for security analysis.
 * This file ports that shape to TypeScript and maps Cotal journal/run events
 * onto it, so the Arena Hub fleet's agent activity can be monitored with the
 * same normalized telemetry ADR uses for its two-tier Detector.
 *
 * Field-by-field correspondence with AgentEvent:
 *  - timestamp, source, session_id          → core fields
 *  - chat_history: ChatMessage[]            → role/content/tools[]
 *  - tool_name/tool_type/server_name/arguments/result/status/error → ToolUsage
 *  - session_context, raw_log_path, model, hostname, username, host_os,
 *    token_usage, chunking fields           → carried over verbatim
 * Cotal-specific additions are marked "cotal." and never collide with ADR names.
 */

/** A single tool/function call made by an agent. Mirrors ADR's ToolUsage. */
export interface AdrToolUsage {
  /** e.g. "read_file", "bash", "send_message" */
  tool_name: string;
  /** mcp_tool | function_call | tool_use | terminal_command | cotal_endpoint */
  tool_type: string;
  /** MCP server name, when the tool came from an MCP server */
  server_name?: string;
  arguments: Record<string, unknown>;
  result?: string;
  /** success | error | denied */
  status?: string;
  error?: string;
}

/** One message in a conversation turn. Mirrors ADR's ChatMessage. */
export interface AdrChatMessage {
  /** 'user' | 'assistant' */
  role: string;
  content: string;
  tools: AdrToolUsage[];
  sequence_id?: string;
}

/**
 * Unified agent event. Mirrors ADR's AgentEvent dataclass, plus cotal.* extras.
 */
export interface AdrAgentEvent {
  timestamp: string; // ISO-8601
  /**
   * Telemetry source. ADR uses agent keys (claude, cursor, codex, gemini, …).
   * For fleet traffic the source is "cotal" with the originating connector in
   * cotal.connector.
   */
  source: string;
  session_id: string;
  chat_history: AdrChatMessage[];

  user_id?: string;
  project_path?: string;
  model?: string;
  hostname?: string;
  username?: string;
  raw_log_path?: string;
  session_context?: Record<string, unknown>;
  token_usage?: Record<string, unknown>;
  host_os?: string;

  /** Cotal extensions — addressing and delivery metadata ADR doesn't carry. */
  cotal?: {
    space?: string;
    sender?: string; // owner.actor principal
    channel?: string;
    delivery_mode?: "unicast" | "multicast" | "anycast";
    connector?: string; // e.g. "connector-1minai", "connector-claude"
  };
}

/**
 * ADR's AgentEvent.has_meaningful_content() port: drop noise entries
 * (auth failures, rate limits, warmups) before they reach detection.
 */
export function hasMeaningfulContent(event: AdrAgentEvent): boolean {
  const msgs = event.chat_history;
  if (msgs.length === 0) return false;
  if (msgs.length >= 2) return true;
  const msg = msgs[0];
  if (msg.tools.length > 0) return true;
  const content = msg.content.trim();
  if (content.length <= 5) return false;
  const skip = [
    "invalid api key",
    "please run /login",
    "authentication failed",
    "unauthorized",
    "session expired",
    "rate limit",
    "api error",
    "warmup",
  ];
  const lower = content.toLowerCase();
  return !skip.some((p) => lower.includes(p));
}

/**
 * Minimal shape of a Cotal journal/run event this adapter understands.
 * (Mirrors packages/core/src/types.ts message shapes; kept structural so this
 * module doesn't hard-depend on core.)
 */
export interface CotalRunEventLike {
  ts: string;
  space: string;
  from: string; // owner.actor principal
  channel?: string;
  kind: "chat" | "dm" | "task" | "control";
  text: string;
  toolCalls?: Array<{
    name: string;
    type?: string;
    server?: string;
    args?: Record<string, unknown>;
    ok?: boolean;
    error?: string;
  }>;
  connector?: string;
  model?: string;
}

/** Map one Cotal run/journal event onto the ADR AgentEvent shape. */
export function fromCotalRunEvent(ev: CotalRunEventLike, sessionId: string): AdrAgentEvent {
  const tools: AdrToolUsage[] = (ev.toolCalls ?? []).map((t) => ({
    tool_name: t.name,
    tool_type: t.type ?? "cotal_endpoint",
    server_name: t.server,
    arguments: t.args ?? {},
    status: t.ok === false ? "error" : "success",
    error: t.error,
  }));
  return {
    timestamp: ev.ts,
    source: "cotal",
    session_id: sessionId,
    chat_history: [{ role: "assistant", content: ev.text, tools }],
    model: ev.model,
    cotal: {
      space: ev.space,
      sender: ev.from,
      channel: ev.channel,
      delivery_mode: ev.kind === "dm" ? "unicast" : ev.kind === "task" ? "anycast" : "multicast",
      connector: ev.connector,
    },
  };
}
