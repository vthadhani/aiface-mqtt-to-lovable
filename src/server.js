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

// Device protocol: ACK topic suffix
const MQTT_ACK_TOPIC_SUFFIX = process.env.MQTT_ACK_TOPIC_SUFFIX || "/pub"; // device listens on aiface/<SN>/pub
const MQTT_SUB_TOPIC_SUFFIX = process.env.MQTT_SUB_TOPIC_SUFFIX || "/sub"; // device sends on aiface/<SN>/sub

const LOVABLE_ENDPOINT =
  process.env.LOVABLE_ENDPOINT || "https://ndmytnnbirezyrqvejwz.supabase.co/functions/v1/attendance-api";
const LOVABLE_API_KEY = process.env.LOVABLE_API_KEY;

const DEDUPE_SECONDS = Math.max(parseInt(process.env.DEDUPE_SECONDS || "0", 10), 0);

// If true, we will drop records that don't have enough data to be meaningful
const DROP_INCOMPLETE = (process.env.DROP_INCOMPLETE || "true").toLowerCase() === "true";

if (!MQTT_URL) {
  console.error("ERROR: MQTT_URL is required.");
  process.exit(1);
}
if (!LOVABLE_API_KEY) {
  console.error("ERROR: LOVABLE_API_KEY is required.");
  process.exit(1);
}

function safeParseJson(payload) {
  try {
    return JSON.parse(payload);
  } catch {
    return null;
  }
}

// Simple in-memory dedupe: key = device_sn|enrollid|timestamp|action
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

function inferActionFromInout(inout) {
  // Many terminals use inout 0=in, 1=out
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
  // Device sends "YYYY-MM-DD HH:mm:ss"
  const t = value || null;
  if (!t) return new Date().toISOString();
  return t;
}

// ---- NEW: cloudtime formatting for ACK ----
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

// ---- NEW: derive SN from topic if msg.sn is missing ----
function extractSnFromTopic(topic) {
  // Expected: aiface/<SN>/sub
  const parts = String(topic || "").split("/");
  if (parts.length >= 3 && parts[0] === "aiface") {
    return parts[1] || null;
  }
  return null;
}

// ---- NEW: publish ACK for sendlog ----
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

  try {
    mqttClient.publish(ackTopic, JSON.stringify(payload), { qos: 1, retain: false }, (err) => {
      if (err) {
        stats.ackErrors += 1;
        stats.lastAckError = err.message;
        stats.lastAckErrorAt = new Date().toISOString();
        console.error(`ACK publish error to ${ackTopic}:`, err.message);
      } else {
        stats.acksSent += 1;
        stats.lastAckAt = new Date().toISOString();
        stats.lastAck = { sn, count: payload.count, logindex: payload.logindex };
        console.log(`ACK sent -> ${ackTopic} count=${payload.count} logindex=${payload.logindex}`);
      }
    });
  } catch (e) {
    stats.ackErrors += 1;
    stats.lastAckError = e?.message || String(e);
    stats.lastAckErrorAt = new Date().toISOString();
    console.error(`ACK publish exception to ${ackTopic}:`, stats.lastAckError);
  }
}

async function postToLovable(payload) {
  const res = await fetch(LOVABLE_ENDPOINT, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": LOVABLE_API_KEY,
    },
    body: JSON.stringify(payload),
  });

  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const err = `Lovable error ${res.status}: ${JSON.stringify(data)}`;
    throw new Error(err);
  }
  return data;
}

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

  // NEW ack stats
  acksSent: 0,
  ackErrors: 0,
  lastAckAt: null,
  lastAck: null,
  lastAckErrorAt: null,
  lastAckError: null,

  forwarded: 0,
  forwardedBulk: 0,
  droppedIncomplete: 0,
  deduped: 0,

  errors: 0,
  lastError: null,
  lastForwardAt: null,
};

async function handleSendLog(msg) {
  stats.receivedSendlog += 1;

  const device_sn = msg.sn || null;
  const records = Array.isArray(msg.record) ? msg.record : [];
  if (!records.length) return;

  // Convert each biometric record into a Lovable-friendly record.
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

  const payload = {
    action: "bulk",
    records: out,
  };

  const result = await postToLovable(payload);
  stats.forwarded += out.length;
  stats.forwardedBulk += 1;
  stats.lastForwardAt = new Date().toISOString();

  return result;
}

async function handleIncomingMqtt(topic, payloadStr) {
  const msg = safeParseJson(payloadStr);
  if (!msg || typeof msg !== "object") return;

  // Only care about cmd:"sendlog"
  if (msg.cmd === "sendlog") {
    // ---- NEW: ACK FIRST (never wait for Lovable) ----
    const sn = msg.sn || extractSnFromTopic(topic);
    publishSendlogAck({ sn, count: msg.count, logindex: msg.logindex });

    // Then forward to Lovable
    await handleSendLog({ ...msg, sn: sn || msg.sn });
  }
}

// ============================
// MQTT (stable + persistent)
// ============================

const mqttClient = mqtt.connect(MQTT_URL, {
  clientId: MQTT_CLIENT_ID,
  clean: MQTT_CLEAN, // should be false
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
// HTTP status
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
    },
    stats,
    time: new Date().toISOString(),
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
