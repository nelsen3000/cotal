/**
 * Subject builders for the Arena Hub muse connector.
 *
 * These mirror `@cotal-ai/core` `packages/core/src/subjects.ts` from the
 * nelsen3000/cotal fork (branch arena-hub-v1). They are reimplemented here
 * because this connector is a minimal standalone sidecar that speaks the
 * Cotal wire protocol directly over NATS — it does not import the upstream
 * extension framework. The grammar is copied exactly, never re-derived:
 *   - Prefix: `cotal.<space>.`
 *   - Multicast: `chat.<owner>.<actor>.<channel…>`      (publisher principal first)
 *   - Unicast:   `inst.<recipOwner>.<recipActor>.<sndOwner>.<sndActor>` (4 tokens)
 *   - Anycast:   `svc.<service>.<owner>.<actor>`
 *   - DM receive filter: `inst.<owner>.<actor>.>`
 *   - Presence KV bucket: `cotal_presence_<space>`; key = instance nkey public key
 *   - DM stream: `DM_<space>`; DM durable: `dm_<owner>-<actor>-<uid>`
 *
 * Owner/actor tokens are fail-loud validated ([A-Za-z0-9_]) — never silently
 * rewritten — because a separator or wildcard in an id is lane breakout.
 */

const ROOT = "cotal";
const ILLEGAL = /[^A-Za-z0-9_-]/g;

/** Make a string safe to use as a single NATS subject token (upstream: token()). */
function token(s) {
  const t = String(s).trim().replace(ILLEGAL, "_");
  return t.length > 0 ? t : "_";
}

function spacePrefix(space) {
  return `${ROOT}.${token(space)}`;
}

/**
 * Owner/actor identity token (upstream: assertValidOwnerToken).
 * Fails LOUD — never rewrites — on dots, wildcards, or '-'
 * ('-' is reserved as the principal name-form separator).
 */
function assertValidOwnerToken(owner) {
  if (typeof owner !== "string" || !/^[A-Za-z0-9_]+$/.test(owner)) {
    throw new Error(
      `invalid owner/actor token "${owner}": must be [A-Za-z0-9_]; ` +
        `no dots, '*', '>', or '-'. A separator or wildcard in an id is ` +
        `lane breakout and is rejected rather than silently rewritten.`
    );
  }
  return owner;
}

function ownerToken(s) {
  return s === "*" ? "*" : assertValidOwnerToken(s);
}

/** A routing token for service/role slots (upstream: routeToken). */
function routeToken(s) {
  return s === "*" ? "*" : token(s);
}

/** The principal in its two canonical forms (upstream: principalKey). */
function principalKey(owner, actor) {
  assertValidOwnerToken(owner);
  assertValidOwnerToken(actor);
  return { key: `${owner}.${actor}`, name: `${owner}-${actor}` };
}

/** Lifecycle uid grammar (upstream: assertLifecycleToken) — dash-free [a-z0-9]. */
function assertLifecycleToken(uid) {
  if (typeof uid !== "string" || !/^[a-z0-9]+$/.test(uid)) {
    throw new Error(
      `invalid lifecycle uid "${uid}": must be dash-free [a-z0-9].`
    );
  }
  return uid;
}

/** JetStream-name form of a lifecycle principal (upstream: lifecycleNameKey). */
function lifecycleNameKey(owner, actor, lifecycleUid) {
  return `${principalKey(owner, actor).name}-${assertLifecycleToken(lifecycleUid)}`;
}

/**
 * Split a dotted channel into its path, preserving NATS wildcards
 * (upstream: channelPath).
 */
function channelPath(channel) {
  const segs = String(channel)
    .split(".")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
  if (segs.length === 0) return "_";
  return segs
    .map((s, i) => {
      if (s === ">") {
        if (i !== segs.length - 1) {
          throw new Error(
            `channel "${channel}": '>' is only valid as the last segment`
          );
        }
        return ">";
      }
      return s === "*" ? "*" : token(s);
    })
    .join(".");
}

/** True when a channel names a concrete sub-channel (no wildcards) — publishable. */
function isConcreteChannel(channel) {
  return !String(channel)
    .split(".")
    .some((s) => s.trim() === "*" || s.trim() === ">");
}

/** Multicast publish subject: `cotal.<space>.chat.<o>.<a>.<channel…>` */
function chatSubject(space, owner, actor, channel) {
  return `${spacePrefix(space)}.chat.${ownerToken(owner)}.${ownerToken(
    actor
  )}.${channelPath(channel)}`;
}

/** Channel subscribe filter: `cotal.<space>.chat.*.*.<channel>` */
function chatRecvFilter(space, channel) {
  return `${spacePrefix(space)}.chat.*.*.${channelPath(channel)}`;
}

/** DM publish subject (4-token unicast): `cotal.<space>.inst.<ro>.<ra>.<so>.<sa>` */
function unicastSubject(space, recipOwner, recipActor, sndOwner, sndActor) {
  return `${spacePrefix(space)}.inst.${ownerToken(recipOwner)}.${ownerToken(
    recipActor
  )}.${ownerToken(sndOwner)}.${ownerToken(sndActor)}`;
}

/** DM receive filter: `cotal.<space>.inst.<owner>.<actor>.>` */
function unicastRecvFilter(space, owner, actor) {
  return `${spacePrefix(space)}.inst.${ownerToken(owner)}.${ownerToken(actor)}.>`;
}

/** Anycast publish subject: `cotal.<space>.svc.<service>.<o>.<a>` */
function anycastSubject(space, service, owner, actor) {
  return `${spacePrefix(space)}.svc.${routeToken(service)}.${ownerToken(
    owner
  )}.${ownerToken(actor)}`;
}

/** Presence KV bucket: `cotal_presence_<space>` */
function presenceBucket(space) {
  return `cotal_presence_${token(space)}`;
}

/** DM stream: `DM_<space>` */
function dmStream(space) {
  return `DM_${token(space)}`;
}

/** DM durable (lifecycle-keyed): `dm_<owner>-<actor>-<uid>` */
function dmDurable(owner, actor, lifecycleUid) {
  return `dm_${lifecycleNameKey(owner, actor, lifecycleUid)}`;
}

module.exports = { token, spacePrefix, assertValidOwnerToken, ownerToken, routeToken, principalKey, assertLifecycleToken, lifecycleNameKey, channelPath, isConcreteChannel, chatSubject, chatRecvFilter, unicastSubject, unicastRecvFilter, anycastSubject, presenceBucket, dmStream, dmDurable };
