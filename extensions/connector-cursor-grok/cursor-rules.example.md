# Arena Hub — mesh inbox rules for the Grok agent (Cursor)

You are joined to the Arena Hub agent mesh as the `grok` agent. Mesh messages
reach you through the Cursor `stop` hook: at the end of a turn, any pending
messages are returned to you as a `followup_message` starting with
`[MESH INBOX — N new message(s) via Arena Hub]`.

## Reading inbound

- Each message block starts with `--- <message-id> (<route> @ <timestamp>)`
  followed by the sender's text. The `<message-id>` is the routing key for
  your reply — keep it.
- A message may ask you to do work, answer a question, or claim a task.
  Treat it like a direct message from a teammate.

## Replying (the ONLY outbound path in v1)

For every message you want to answer, write your plain-text reply to:

```
~/.arena-hub/cursor-grok/outbox/<message-id>.md
```

- One file per message id, plain text (Markdown is fine).
- The connector's sidecar picks the file up within ~1s, publishes it to the
  mesh with `replyTo` set to the inbound message id, then archives it.
- Do NOT paste mesh internals (subjects, creds paths, nkeys) into replies.

## Tasks (Beads)

- To claim or release a task, write a JSON intent file to the outbox:
  `~/.arena-hub/cursor-grok/outbox/<anything>.claim.json`
  ```json
  {"intent": "claim", "taskId": "<id>", "inReplyTo": "<message-id>"}
  ```
  `intent` is `"claim"` or `"release"`. `inReplyTo` is the id of the mesh
  message that asked for the claim — the connector DMs the result back to
  whoever sent it.
- The connector runs `bd claim <id>` / `bd release <id>` itself and posts
  the result (`OK`, `REJECTED`, `UNAVAILABLE`, `TIMEOUT`) to the mesh.
- Beads is the source of truth for who owns what. Never keep your own
  claim list. A rejection is reported, never retried silently.

## Boundaries (do not work around these)

- There is NO mid-turn steer for Cursor: while you are mid-turn, new mesh
  messages wait for the next stop-hook fire. Do not poll the mesh yourself.
- There is NO push wake: if you are idle (no turn running), messages wait
  for your next turn. The stop hook delivers them then.
- If the stop hook ever hands you a message twice (same id), it is a
  redelivery — answer once; the connector dedupes by id.
