require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const morgan = require("morgan");
const mqtt = require("mqtt");

// Node 18+ has global fetch. If you're on Node 16, pin Node 18 in Coolify.
if (typeof fetch !== "function") {
  console.error("ERROR: fetch() is not available. Use Node 18+ or add node-fetch.");
  process.exit(1);
}

const PORT = parseInt(process.env.PORT || "3000", 10);

const MQTT_URL = process.env.MQTT_URL;
const MQTT_SUB_TOPIC = process.env.MQTT_SUB_TOPIC || "aiface/+/sub";

// Stability settings (env overridable)
const MQTT_CLIENT_ID = process.env.MQTT_CLIENT_ID || "aiface-lovable-bridge"; // MUST be stable + unique per running instance
const MQTT_USERNAME = process.env.MQTT_USERNAME || undefined;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || undefined;
const MQTT_KEEPALIVE = Math.max(parseInt(process.env.MQTT_KEEPALIVE || "60", 10), 10); // seconds
const MQTT_RECONNECT_PERIOD = Math.max(parseInt(process.env.MQTT_RECONNECT_PERIOD || "5000", 10), 1000); // ms
const MQTT_CONNECT_TIMEOUT = Math.max(parseInt(process.env.MQTT_CONNECT_TIMEOUT || "20000", 10), 5000); // ms
const MQTT_CLEAN = (process.env.MQTT_CLEAN || "false").toLowerCase() === "true";
const MQTT_QOS = Number.isFinite(Number(process.env.MQTT_QOS)) ? Number(process.env.MQTT_QOS) : 1;

// Device protocol topic suffixes (defaults match Aiface convention)
const MQTT_ACK_TOPIC_SUFFIX = process.env.MQTT_ACK_TOPIC_SUFFIX || "/pub"; // device listens: aiface/<SN>/pub
const MQTT_SUB_TOPIC_SUFFIX = process.env.MQTT_SUB_TOPIC_SUFFIX || "/sub"; // device sends:   aiface/<SN>/sub

const LOVABLE_ENDPOINT =
  process.env.LOVABLE_ENDPOINT || "https://ndmytnnbirezyrqvejwz.supabase.co/functions/v1/attendance-api";
const LOVABLE_API_KEY = process.env.LOVABLE_API_KEY;

const DEDUPE_SECONDS = Math.max(parseInt(process.env.DEDUPE_SECONDS || "0", 10), 0);
const DROP_INCOMPLETE = (process.env.DROP_INCOMPLETE || "true").toLowerCase() === "true";

// Observability controls
const RECENT_EVENTS_MAX = Math.max(parseInt(process.env.RECENT_EVENTS_MAX || "200", 10), 20);
const LOVABLE_RESPONSE_MAX_CHARS = Math.max(parseInt(process.env.LOVABLE_RESPONSE_MAX_CHARS || "1200", 10), 100);

if (!MQTT_URL) {
  console.error("ERROR: MQTT_URL is required.");
  process.exit(1);
}
if (!LOVABLE_API_KEY) {
  console.error("ERROR: LOVABLE_API_KEY is required.");
  process.exit(1);
}

// -------------------- helpers --------------------

function safeParseJson(payload) {
  try {
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function formatCloudTime(d = new Date()) {
  // "YYYY-MM-DD HH:mm:ss"
  const yyyy = d.getFullYear();
  const mm = pad2(d.getMonth() + 1);
  const dd = pad2(d.getDate());
  const hh = pad2(d.getHours());
  const mi = pad2(d.getMinutes());
  const ss = pad2(d.getSeconds());
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function extractSnFromTopic(topic) {
  // Expected: aiface/<SN>/sub
  const parts = String(topic || "").split("/");
  if (parts.length >= 3 && parts[0] === "aiface") return parts[1] || null;
  return null;
}

function inferActionFromInout(inout) {
  if (inout === 0 || inout === "0") return "clock_in";
  if (inout === 1 || inout === "1") return "clock_out";
  return null;
}

function normalizeEnrollId(value) {
  const n = typeof value === "number" ? value : parseInt(String(value || "0"), 10);
  if (!n || n <= 0) return null;
  return n;
}

function normalizeTimestamp(value) {
  // device sends "YYYY-MM-DD HH:mm:ss"
  const t = value || null;
  if (!t) return new Date().toISOString();
  return t;
}

function truncate(str, maxLen) {
  const s = String(str ?? "");
  if (s.length <= maxLen) return s;
  return s.slice(0, maxLen) + `...(${s.length - maxLen} more chars)`;
}

// -------------------- in-memory dedupe --------------------

const seen = new Map();
function dedupe(key) {
  if (!DEDUPE_SECONDS) return false;
  const now = Date.now();

  for (const [k, ts] of seen.entries()) {
    if (now - ts > DEDUPE_SECONDS * 1000) seen.delete(k);
  }

  if (seen.has(key)) return true;
  seen.set(key, now);
  return false;
}

// -------------------- recent events ring buffer --------------------

/**
 * Recent “truth” events so you can answer:
 * - received?
 * - acked?
 * - sent to lovable?
 * - lovable response ok/error?
 */
const recentEvents = []; // newest last
function pushEvent(ev) {
  const e = { at: new Date().toISOString(), ...ev };
  recentEvents.push(e);
  while (recentEvents.length > RECENT_EVENTS_MAX) recentEvents.shift();
  return e;
}

function summarizeRecords(records) {
  const times = records
    .map((r) => r?.time)
    .filter(Boolean)
    .map(String)
    .sort();
  return {
    recordsCount: records.length,
    firstTime: times.length ? times[0] : null,
    lastTime: times.length ? times[times.length - 1] : null,
  };
}

// -------------------- Lovable POST with observability --------------------

async function postToLovable(payload, context = {}) {
  const startedAt = Date.now();

  // mark: we are sending (attempt)
  pushEvent({
    type: "lovable_send_attempt",
    ...context,
    lovableEndpoint: LOVABLE_ENDPOINT,
    payloadSummary: {
      action: payload?.action,
      records: Array.isArray(payload?.records) ? payload.records.length : null,
      sample: Array.isArray(payload?.records) ? payload.records.slice(0, 3).map((r) => ({
        device_sn: r.device_sn,
        enrollid: r.enrollid,
        timestamp: r.timestamp,
        action: r.action,
      })) : null,
    },
  });

  const res = await fetch(LOVABLE_ENDPOINT, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": LOVABLE_API_KEY,
    },
    body: JSON.stringify(payload),
  });

  const ms = Date.now() - startedAt;
  const text = await res.text();

  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }

  const responseSnippet = truncate(text, LOVABLE_RESPONSE_MAX_CHARS);

  // mark: response received
  pushEvent({
    type: "lovable_response",
    ...context,
    ok: res.ok,
    status: res.status,
    ms,
    responseSnippet,
  });

  if (!res.ok) {
    const err = `Lovable error ${res.status}: ${responseSnippet}`;
    throw new Error(err);
  }

  return parsed ?? { raw: text };
}

// -------------------- stats --------------------

let stats = {
  mqttConnected: false,
  mqttEverConnected: false,
  mqttLastConnectAt: null,
  mqttLastDisconnectAt: null,
  mqttLastCloseAt: null,
  mqttLastErrorAt: null,
  mqttLastError: null,

  receivedMessages: 0,
  receivedSendlog: 0,

  // ACK stats
  acksSent: 0,
  ackErrors: 0,
  lastAckAt: null,
  lastAck: null,
  lastAckErrorAt: null,
  lastAckError: null,

  // forward stats
  forwarded: 0,
  forwardedBulk: 0,
  droppedIncomplete: 0,
  deduped: 0,

  // lovable stats
  lovableOk: 0,
  lovableErr: 0,
  lovableLastAt: null,
  lovableLastOk: null,
  lovableLastStatus: null,
  lovableLastMs: null,
  lovableLastSnippet: null,
  lovableLastError: null,

  errors: 0,
  lastError: null,
  lastForwardAt: null,
};

// -------------------- MQTT ACK --------------------

function publishSendlogAck({ sn, count, logindex }) {
  if (!sn) return;

  const ackTopic = `aiface/${sn}${MQTT_ACK_TOPIC_SUFFIX}`;
  const payload = {
    ret: "sendlog",
    result: true,
    count: typeof count === "number" ? count : parseInt(String(count || "0"), 10) || 0,
    logindex: typeof logindex === "number" ? logindex : parseInt(String(logindex || "0"), 10) || 0,
    cloudtime: formatCloudTime(new Date()),
  };

  // record in recent events (attempt)
  pushEvent({
    type: "ack_send_attempt",
    sn,
    ackTopic,
    count: payload.count,
    logindex: payload.logindex,
  });

  try {
    mqttClient.publish(ackTopic, JSON.stringify(payload), { qos: 1, retain: false }, (err) => {
      if (err) {
        stats.ackErrors += 1;
        stats.lastAckError = err.message;
        stats.lastAckErrorAt = new Date().toISOString();
        console.error(`ACK publish error to ${ackTopic}:`, err.message);

        pushEvent({
          type: "ack_send_result",
          sn,
          ackTopic,
          ok: false,
          error: err.message,
          count: payload.count,
          logindex: payload.logindex,
        });
      } else {
        stats.acksSent += 1;
        stats.lastAckAt = new Date().toISOString();
        stats.lastAck = { sn, count: payload.count, logindex: payload.logindex };

        console.log(`ACK sent -> ${ackTopic} count=${payload.count} logindex=${payload.logindex}`);

        pushEvent({
          type: "ack_send_result",
          sn,
          ackTopic,
          ok: true,
          count: payload.count,
          logindex: payload.logindex,
        });
      }
    });
  } catch (e) {
    stats.ackErrors += 1;
    stats.lastAckError = e?.message || String(e);
    stats.lastAckErrorAt = new Date().toISOString();
    console.error(`ACK publish exception to ${ackTopic}:`, stats.lastAckError);

    pushEvent({
      type: "ack_send_result",
      sn,
      ackTopic,
      ok: false,
      error: stats.lastAckError,
      count: payload.count,
      logindex: payload.logindex,
    });
  }
}

// -------------------- core handling --------------------

async function handleSendLog(msg, context = {}) {
  stats.receivedSendlog += 1;

  const device_sn = msg.sn || null;
  const records = Array.isArray(msg.record) ? msg.record : [];

  // record: received sendlog (with summary)
  pushEvent({
    type: "mqtt_sendlog_received",
    ...context,
    sn: device_sn,
    count: msg.count ?? null,
    logindex: msg.logindex ?? null,
    summary: summarizeRecords(records),
  });

  if (!records.length) return;

  const out = [];

  for (const rec of records) {
    const enrollid = normalizeEnrollId(rec.enrollid);
    if (!enrollid) {
      if (DROP_INCOMPLETE) stats.droppedIncomplete += 1;
      continue;
    }

    const inout = rec.inout ?? null;
    const mode = rec.mode ?? null;
    const event = rec.event ?? null;
    const name = rec.name ?? null;

    const action = inferActionFromInout(inout) || "clock_in";
    const timestamp = normalizeTimestamp(rec.time || msg.cloudtime);

    if (DROP_INCOMPLETE) {
      if (!timestamp) {
        stats.droppedIncomplete += 1;
        continue;
      }
    }

    const key = `${device_sn}|${enrollid}|${timestamp}|${action}`;
    if (dedupe(key)) {
      stats.deduped += 1;
      continue;
    }

    out.push({
      action,
      enrollid,
      timestamp,
      device_sn,
      biometric_verified: true,
      notes: null,

      inout,
      mode,
      event,
      name,

      raw_json: rec,

      raw_msg: {
        cmd: msg.cmd,
        sn: msg.sn,
        count: msg.count,
        logindex: msg.logindex,
        cloudtime: msg.cloudtime,
      },
    });
  }

  if (!out.length) return;

  // Bulk forward
  const payload = {
    action: "bulk",
    records: out,
  };

  const ctx = {
    sn: device_sn,
    mqttTopic: context.mqttTopic ?? null,
    count: msg.count ?? null,
    logindex: msg.logindex ?? null,
    recordFirstTime: out[0]?.timestamp ?? null,
    recordLastTime: out[out.length - 1]?.timestamp ?? null,
  };

  try {
    const result = await postToLovable(payload, ctx);

    stats.forwarded += out.length;
    stats.forwardedBulk += 1;
    stats.lastForwardAt = new Date().toISOString();

    stats.lovableOk += 1;
    stats.lovableLastAt = new Date().toISOString();
    stats.lovableLastOk = true;
    stats.lovableLastStatus = 200; // res.ok already true; actual status logged in event
    stats.lovableLastError = null;

    // extra: mark a “bridge says delivered” event
    pushEvent({
      type: "bridge_delivery_ok",
      ...ctx,
      forwardedCount: out.length,
    });

    return result;
  } catch (e) {
    stats.lovableErr += 1;
    stats.lovableLastAt = new Date().toISOString();
    stats.lovableLastOk = false;
    stats.lovableLastError = e?.message || String(e);

    pushEvent({
      type: "bridge_delivery_error",
      ...ctx,
      error: stats.lovableLastError,
      forwardedCount: out.length,
    });

    throw e;
  }
}

async function handleIncomingMqtt(topic, payloadStr) {
  const msg = safeParseJson(payloadStr);
  if (!msg || typeof msg !== "object") return;

  // Only care about cmd:"sendlog"
  if (msg.cmd === "sendlog") {
    const sn = msg.sn || extractSnFromTopic(topic);

    // ACK FIRST (never wait for Lovable)
    publishSendlogAck({ sn, count: msg.count, logindex: msg.logindex });

    // Then forward to Lovable
    await handleSendLog({ ...msg, sn: sn || msg.sn }, { mqttTopic: topic });
  }
}

// ============================
// MQTT (stable + persistent)
// ============================

const mqttClient = mqtt.connect(MQTT_URL, {
  clientId: MQTT_CLIENT_ID,
  clean: MQTT_CLEAN,
  keepalive: MQTT_KEEPALIVE,
  reconnectPeriod: MQTT_RECONNECT_PERIOD,
  connectTimeout: MQTT_CONNECT_TIMEOUT,
  resubscribe: true,

  username: MQTT_USERNAME,
  password: MQTT_PASSWORD,

  will: {
    topic: `bridge/${MQTT_CLIENT_ID}/status`,
    payload: JSON.stringify({ online: false, at: new Date().toISOString() }),
    qos: 1,
    retain: true,
  },
});

function publishBridgeStatus(online, extra = {}) {
  try {
    mqttClient.publish(
      `bridge/${MQTT_CLIENT_ID}/status`,
      JSON.stringify({ online, at: new Date().toISOString(), ...extra }),
      { qos: 1, retain: true }
    );
  } catch (_) {}
}

mqttClient.on("connect", () => {
  stats.mqttConnected = true;
  stats.mqttEverConnected = true;
  stats.mqttLastConnectAt = new Date().toISOString();
  stats.mqttLastError = null;

  console.log("MQTT connected:", MQTT_URL);
  console.log(
    `MQTT session: clientId=${MQTT_CLIENT_ID} clean=${MQTT_CLEAN} keepalive=${MQTT_KEEPALIVE}s reconnectPeriod=${MQTT_RECONNECT_PERIOD}ms topic=${MQTT_SUB_TOPIC} qos=${MQTT_QOS}`
  );

  publishBridgeStatus(true);

  mqttClient.subscribe(MQTT_SUB_TOPIC, { qos: MQTT_QOS }, (err) => {
    if (err) {
      console.error("MQTT subscribe error:", err.message);
      stats.mqttLastError = `subscribe: ${err.message}`;
      stats.mqttLastErrorAt = new Date().toISOString();
    } else {
      console.log("Subscribed to:", MQTT_SUB_TOPIC);
    }
  });
});

mqttClient.on("reconnect", () => {
  stats.mqttConnected = false;
  console.log("MQTT reconnecting...");
});

mqttClient.on("offline", () => {
  stats.mqttConnected = false;
  stats.mqttLastDisconnectAt = new Date().toISOString();
  console.warn("MQTT offline (network down / broker unreachable).");
});

mqttClient.on("close", () => {
  stats.mqttConnected = false;
  stats.mqttLastCloseAt = new Date().toISOString();
  console.warn("MQTT connection closed.");
});

mqttClient.on("error", (err) => {
  stats.mqttConnected = false;
  stats.mqttLastError = err.message;
  stats.mqttLastErrorAt = new Date().toISOString();
  console.error("MQTT error:", err.message);
});

mqttClient.on("message", async (topic, payload) => {
  stats.receivedMessages += 1;
  try {
    await handleIncomingMqtt(topic, payload.toString("utf8"));
  } catch (e) {
    stats.errors += 1;
    stats.lastError = e?.message || String(e);
    console.error("Forwarding error:", stats.lastError);
  }
});

// ============================
// HTTP status + debug endpoints
// ============================

const app = express();
app.use(helmet());
app.use(express.json({ limit: "2mb" }));
app.use(morgan("combined"));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    mqtt: {
      url: MQTT_URL,
      clientId: MQTT_CLIENT_ID,
      subscribed: MQTT_SUB_TOPIC,
      connected: mqttClient.connected,
      clean: MQTT_CLEAN,
      keepalive: MQTT_KEEPALIVE,
      reconnectPeriodMs: MQTT_RECONNECT_PERIOD,
      qos: MQTT_QOS,
    },
    lovable: { endpoint: LOVABLE_ENDPOINT },
    config: {
      dedupeSeconds: DEDUPE_SECONDS,
      dropIncomplete: DROP_INCOMPLETE,
      ackTopicSuffix: MQTT_ACK_TOPIC_SUFFIX,
      recentEventsMax: RECENT_EVENTS_MAX,
      lovableResponseMaxChars: LOVABLE_RESPONSE_MAX_CHARS,
    },
    stats,
    time: new Date().toISOString(),
  });
});

/**
 * Recent truth events.
 * Optional query params:
 *  - limit=50
 *  - type=ack_send_result | mqtt_sendlog_received | lovable_response | bridge_delivery_ok | bridge_delivery_error ...
 *  - sn=AYTI10105321
 */
app.get("/recent", (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit || "50", 10), 1), 500);
  const type = req.query.type ? String(req.query.type) : null;
  const sn = req.query.sn ? String(req.query.sn) : null;

  let items = recentEvents.slice(); // oldest -> newest
  if (type) items = items.filter((e) => e.type === type);
  if (sn) items = items.filter((e) => (e.sn || e.device_sn) === sn);

  // return newest first for convenience
  items = items.slice(-limit).reverse();

  res.json({
    ok: true,
    count: items.length,
    filters: { limit, type, sn },
    items,
  });
});

/**
 * Quick “prove it” endpoint:
 * Shows the most recent received sendlog + most recent lovable response for a given SN.
 * /prove?sn=AYTI10105321
 */
app.get("/prove", (req, res) => {
  const sn = req.query.sn ? String(req.query.sn) : null;
  if (!sn) return res.status(400).json({ ok: false, error: "sn is required, e.g. /prove?sn=AYTI10105321" });

  const findLast = (types) =>
    [...recentEvents].reverse().find((e) => (e.sn === sn || e.device_sn === sn) && types.includes(e.type)) || null;

  const lastReceived = findLast(["mqtt_sendlog_received"]);
  const lastAck = findLast(["ack_send_result"]);
  const lastLovable = findLast(["lovable_response"]);
  const lastOk = findLast(["bridge_delivery_ok"]);
  const lastErr = findLast(["bridge_delivery_error"]);

  res.json({
    ok: true,
    sn,
    lastReceived,
    lastAck,
    lastLovable,
    lastDelivery: lastOk || lastErr,
    now: new Date().toISOString(),
  });
});

app.listen(PORT, () => {
  console.log(`MQTT→Lovable bridge listening on :${PORT}`);
});

// ============================
// Graceful shutdown
// ============================

function shutdown(signal) {
  console.log(`Received ${signal}. Shutting down...`);
  publishBridgeStatus(false, { signal });

  try {
    mqttClient.end(true, () => process.exit(0));
    setTimeout(() => process.exit(0), 3000).unref();
  } catch (_) {
    process.exit(0);
  }
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
