'use strict';
/**
 * config.js — configuration for the Gemini connector.
 *
 * All secrets come from the environment (never from code, never from argv).
 * Identity (owner/actor/nkey/lifecycle uid) is DERIVED from the minted creds
 * file — the connector never invents its own identity.
 */
const fs = require('fs');

function decodeJwtPayload(jwt) {
  const seg = jwt.split('.')[1];
  const padded = seg + '='.repeat((-seg.length % 4 + 4) % 4);
  return JSON.parse(Buffer.from(padded, 'base64').toString('utf8'));
}

function parseCreds(credsText) {
  const m = credsText.match(/-----BEGIN NATS USER JWT-----\s*([\s\S]*?)\s*------END NATS USER JWT------/);
  if (!m) throw new Error('creds file has no NATS USER JWT block');
  const payload = decodeJwtPayload(m[1].replace(/\s+/g, ''));
  return { nkey: payload.sub, payload };
}

/**
 * Derive owner/actor from the forge-locked DM publish grant:
 *   cotal.<space>.inst.*.*.<owner>.<actor>
 * This is authoritative — it is what the broker enforces.
 */
function derivePrincipal(space, grants) {
  const prefix = `cotal.${space}.inst.*.*.`;
  for (const g of grants) {
    if (g.startsWith(prefix)) {
      const rest = g.slice(prefix.length).split('.');
      if (rest.length === 2 && rest[0] && rest[1]) {
        return { owner: rest[0], actor: rest[1] };
      }
    }
  }
  throw new Error(`could not derive owner.actor from JWT grants for space "${space}"`);
}

/** Derive the lifecycle uid from the pre-provisioned DM durable grant:
 *   $JS.API.CONSUMER.INFO.DM_<space>.dm_<owner>-<actor>-<uid>
 */
function deriveLifecycleUid(space, owner, actor, grants) {
  const needle = `DM_${space}.dm_${owner}-${actor}-`;
  for (const g of grants) {
    const i = g.indexOf(needle);
    if (i >= 0) {
      const uid = g.slice(i + needle.length).split('.')[0];
      if (uid) return uid;
    }
  }
  return null;
}

function loadConfig(env) {
  env = env || process.env;
  const space = env.COTAL_SPACE || 'main';
  const server = env.COTAL_SERVER || 'nats://127.0.0.1:4222';
  const credsPath = env.COTAL_CREDS;
  if (!credsPath) throw new Error('COTAL_CREDS is required (path to the 0600 minted creds file)');

  const credsText = fs.readFileSync(credsPath, 'utf8');
  const { nkey, payload } = parseCreds(credsText);
  const pubGrants = ((payload.nats || {}).pub || {}).allow || [];
  const subGrants = ((payload.nats || {}).sub || {}).allow || [];
  const grants = pubGrants.concat(subGrants);

  const derived = derivePrincipal(space, grants);
  const owner = env.COTAL_OWNER || derived.owner;
  const actor = env.COTAL_ACTOR || derived.actor;
  const lifecycleUid = env.COTAL_LIFECYCLE_UID || deriveLifecycleUid(space, owner, actor, grants);
  if (!lifecycleUid) {
    throw new Error('could not determine lifecycle uid (no COTAL_LIFECYCLE_UID and no pre-provisioned DM durable grant)');
  }

  // Channels this connector is allowed to READ (subscribe) and WRITE (publish),
  // taken from the requested COTAL_SUBSCRIBE list intersected with the JWT grants.
  const requested = (env.COTAL_SUBSCRIBE || '').split(',').map(s => s.trim()).filter(Boolean);
  const subChannels = requested.filter(ch =>
    subGrants.includes(`cotal.${space}.chat.*.*.${ch}`));
  const pubChannels = requested.filter(ch =>
    pubGrants.includes(`cotal.${space}.chat.${owner}.${actor}.${ch}`));
  const denied = requested.filter(ch => !subChannels.includes(ch));
  if (denied.length) {
    console.warn(`[config] WARNING: requested channels not in JWT grants (skipped): ${denied.join(', ')}`);
  }

  const pollMs = parseInt(env.POLL_INTERVAL_MS || '60000', 10);
  const presenceMs = parseInt(env.PRESENCE_INTERVAL_MS || '2000', 10);

  const clientKind = (env.GEMINI_CLIENT || 'mock').toLowerCase();
  if (clientKind !== 'mock' && clientKind !== 'real') {
    throw new Error(`GEMINI_CLIENT must be "mock" or "real", got "${env.GEMINI_CLIENT}"`);
  }
  if (clientKind === 'real' && !env.GEMINI_API_KEY) {
    throw new Error('GEMINI_CLIENT=real requires GEMINI_API_KEY in the environment');
  }

  return {
    space,
    server,
    credsPath,
    credsText,
    nkey,
    owner,
    actor,
    agentName: env.COTAL_AGENT_NAME || 'gemini',
    lifecycleUid,
    subChannels,
    pubChannels,
    pollMs,
    presenceMs,
    mode: (env.CONNECTOR_MODE || 'daemon').toLowerCase(), // daemon | once
    clientKind,
    geminiApiKey: env.GEMINI_API_KEY || null,
    geminiModel: env.GEMINI_MODEL || 'gemini-2.0-flash',
    mockLog: env.MOCK_LOG || null,
    presenceBucket: `cotal_presence_${space}`,
    presenceKey: `${owner}.${actor}`,
    dmStream: `DM_${space}`,
    dmDurable: `dm_${owner}-${actor}-${lifecycleUid}`,
    subjectPrefix: `cotal.${space}.`,
  };
}

module.exports = { loadConfig, parseCreds, derivePrincipal, deriveLifecycleUid };
