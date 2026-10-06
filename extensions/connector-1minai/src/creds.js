// Parse a `cotal mint`-ed creds file and derive everything the connector needs:
// owner, actor (nkey), space, and the pre-provisioned DM durable name.
// This avoids hardcoding any identity material in config or code.

"use strict";

const fs = require("fs");
const { credsAuthenticator } = require("nats");

function b64urlDecode(s) {
  return Buffer.from(s.replace(/-/g, "+").replace(/_/g, "/"), "base64").toString("utf8");
}

function parseCredsFile(path) {
  const text = fs.readFileSync(path, "utf8");
  const m = text.match(/-----BEGIN NATS USER JWT-----\s*([\s\S]*?)\s*-----END NATS USER JWT-----/);
  if (!m) throw new Error(`no NATS USER JWT found in ${path}`);
  const jwt = m[1].trim().split("\n").join("");
  const payload = JSON.parse(b64urlDecode(jwt.split(".")[1]));
  const nats = payload.nats || {};
  const sub = nats.sub || payload.sub;

  // nkey public key: take it from any subject containing a UB/UD-style key,
  // else from the `name` claim of a minted agent creds file.
  const allSubjects = [
    ...((nats.pub && nats.pub.allow) || []),
    ...((nats.sub && nats.sub.allow) || []),
  ].join(" ");

  // owner.actor pair: find "local.<NKEY>" in a DM publish allow like
  //   cotal.<space>.inst.*.*.local.<NKEY>
  let owner = null, actor = null, space = null;
  const dmPub = ((nats.pub && nats.pub.allow) || []).find((s) =>
    /\.inst\.\*\.\*\./.test(s)
  );
  if (dmPub) {
    const mm = dmPub.match(/^cotal\.([^.]+)\.inst\.\*\.\*\.(.+)\.(.+)$/);
    if (mm) {
      space = mm[1];
      owner = mm[2];
      actor = mm[3];
    }
  }
  if (!owner || !actor || !space) {
    throw new Error(`could not derive owner/actor/space from creds JWT (${path})`);
  }

  // Pre-provisioned DM durable: the $JS.API allow entries name it exactly,
  // e.g. $JS.API.CONSUMER.INFO.DM_<space>.dm_<owner>-<actor>-<uid>
  // (these live in pub.allow — $JS.API calls are request/reply publishes).
  const dmStream = `DM_${space}`;
  const apiAllows = [
    ...((nats.pub && nats.pub.allow) || []),
    ...((nats.sub && nats.sub.allow) || []),
  ];
  const infoAllow = apiAllows.find((s) =>
    s.startsWith(`$JS.API.CONSUMER.INFO.${dmStream}.`)
  );
  const durable = infoAllow ? infoAllow.slice(`$JS.API.CONSUMER.INFO.${dmStream}.`.length) : null;

  return { owner, actor, space, nkey: actor, durable, dmStream };
}

module.exports = { parseCredsFile, credsAuthFromFile };

// nats@2.x exposes credsAuthenticator (not credsFile): it accepts the full
// creds-file text and parses the JWT + nkey seed out of it.
function credsAuthFromFile(path) {
  return credsAuthenticator(fs.readFileSync(path)); // Buffer, not string
}
