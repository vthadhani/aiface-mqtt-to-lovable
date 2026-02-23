require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const morgan = require("morgan");
const mqtt = require("mqtt");
const Database = require("better-sqlite3");

// Node 18+ has global fetch. If you're on Node 16, pin Node 18 in Coolify.
if (typeof fetch !== "function") {
  console.error("ERROR: fetch() is not available. Use Node 18+ or add node-fetch.");
  process.exit(1);
}

const PORT = parseInt(process.env.PORT || "3000", 10);

const MQTT_URL = process.env.MQTT_URL;
const MQTT_SUB_TOPIC = process.env.MQTT_SUB_TOPIC || "aiface/+/sub";

// Stability settings (env overridable)
const MQTT_CLIENT_ID = process.env.MQTT_CLIENT_ID || "aiface-lovable-bridge";
const MQTT_USERNAME = process.env.MQTT_USERNAME || undefined;
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || undefined;
const MQTT_KEEPALIVE = Math.max(parseInt(process.env.MQTT_KEEPALIVE || "60", 10), 10);
const MQTT_RECONNECT_PERIOD = Math.max(parseInt(process.env.MQTT_RECONNECT_PERIOD || "5000", 10), 1000);
const MQTT_CONNECT_TIMEOUT = Math.max(parseInt(process.env.MQTT_CONNECT_TIMEOUT || "20000", 10), 5000);
const MQTT_CLEAN = (process.env.MQTT_CLEAN || "false").toLowerCase() === "true";
const MQTT_QOS = Number.isFinite(Number(process.env.MQTT_QOS)) ? Number(process.env.MQTT_QOS) : 1;

const MQTT_ACK_TOPIC_SUFFIX = process.env.MQTT_ACK_TOPIC_SUFFIX || "/pub";

const LOVABLE_ENDPOINT =
  process.env.LOVABLE_ENDPOINT || "https://ndmytnnbirezyrqvejwz.supabase.co/functions/v1/attendance-api";
const LOVABLE_API_KEY = process.env.LOVABLE_API_KEY;

const DEDUPE_SECONDS = Math.max(parseInt(process.env.DEDUPE_SECONDS || "0", 10), 0);
const DROP_INCOMPLETE = (process.env.DROP_INCOMPLETE || "true").toLowerCase() === "true";

// Observability controls
const RECENT_EVENTS_MAX = Math.max(parseInt(process.env.RECENT_EVENTS_MAX || "200", 10), 20);
const LOVABLE_RESPONSE_MAX_CHARS = Math.max(parseInt(process.env.LOVABLE_RESPONSE_MAX_CHARS || "1200", 10), 100);

// Queue / DB
const QUEUE_DB_PATH = process.env.QUEUE_DB_PATH || "./bridge-queue.sqlite";
const QUEUE_RETRY_INTERVAL_SECONDS = Math.max(parseInt(process.env.QUEUE_RETRY_INTERVAL_SECONDS || "30", 10), 5);
const QUEUE_RETRY_BATCH_SIZE = Math.max(parseInt(process.env.QUEUE_RETRY_BATCH_SIZE || "50", 10), 1);

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
  const yyyy = d.getFullYear();
  const mm = pad2(d.getMonth() + 1);
  const dd = pad2(d.getDate());
  const hh = pad2(d.getHours());
  const mi = pad2(d.getMinutes());
  const ss = pad2(d.getSeconds());
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function extractSnFromTopic(topic) {
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

const recentEvents = [];
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

// -------------------- SQLite store-and-forward --------------------

const db = new Database(QUEUE_DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");

db.exec(`
CREATE TABLE IF NOT EXISTS mqtt_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  sn TEXT,
  topic TEXT,
  cmd TEXT,
  count INTEGER,
  logindex INTEGER,
  payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS punch_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  sn TEXT,
  enrollid INTEGER,
  timestamp TEXT,
  action TEXT,
  payload_json TEXT NOT NULL,

  status TEXT NOT NULL DEFAULT 'pending',  -- pending | sent | failed
  attempts INTEGER NOT NULL DEFAULT 0,
  last_attempt_at TEXT,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_punch_queue_status ON punch_queue(status);
CREATE INDEX IF NOT EXISTS idx_punch_queue_sn_time ON punch_queue(sn, timestamp);
`);

const stmtInsertBatch = db.prepare(`
  INSERT INTO mqtt_batches (created_at, sn, topic, cmd, count, logindex, payload_json)
  VALUES (@created_at, @sn, @topic, @cmd, @count, @logindex, @payload_json)
`);

const stmtInsertPunch = db.prepare(`
  INSERT INTO punch_queue (created_at, sn, enrollid, timestamp, action, payload_json, status)
  VALUES (@created_at, @sn, @enrollid, @timestamp, @action, @payload_json, 'pending')
`);

const stmtSelectPending = db.prepare(`
  SELECT * FROM punch_queue
  WHERE status IN ('pending','failed')
  ORDER BY id ASC
  LIMIT ?
`);

const stmtMarkSent = db.prepare(`
  UPDATE punch_queue
  SET status='sent', attempts=attempts+1, last_attempt_at=@at, last_error=NULL
  WHERE id=@id
`);

const stmtMarkFailed = db.prepare(`
  UPDATE punch_queue
  SET status='failed', attempts=attempts+1, last_attempt_at=@at, last_error=@err
  WHERE id=@id
`);

const stmtQueueStats = db.prepare(`
  SELECT
    SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent,
    COUNT(*) AS total
  FROM punch_queue
`);

const stmtLatestQueueItems = db.prepare(`
  SELECT id, created_at, sn, enrollid, timestamp, action, status, attempts, last_attempt_at, last_error
  FROM punch_queue
  ORDER BY id DESC
  LIMIT ?
`);

// -------------------- Lovable POST (with correct OK/NOT OK detection) --------------------

function lovableLogicalOk(data) {
  // We treat these as success:
  // - data.ok === true AND (no results OR all results ok)
  // Anything else becomes failure so records remain queued.
  if (!data || typeof data !== "object") return false;

  if (data.ok !== true) return false;

  if (Array.isArray(data.results)) {
    return data.results.every((r) => r && r.ok === true);
  }
  return true;
}

async function postToLovable(payload, context = {}) {
  const startedAt = Date.now();

  pushEvent({
    type: "lovable_send_attempt",
    ...context,
    lovableEndpoint: LOVABLE_ENDPOINT,
    payloadSummary: {
      action: payload?.action,
      records: Array.isArray(payload?.records) ? payload.records.length : null,
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
  const snippet = truncate(text, LOVABLE_RESPONSE_MAX_CHARS);

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }

  const logicalOk = res.ok && lovableLogicalOk(data);

  pushEvent({
    type: "lovable_response",
    ...context,
    httpOk: res.ok,
    ok: logicalOk,
    status: res.status,
    ms,
    responseSnippet: snippet,
  });

  if (!logicalOk) {
    // even if HTTP 200, treat embedded ok:false as failure
    throw new Error(`Lovable NOT OK (http=${res.status}): ${snippet}`);
  }

  return data;
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

  acksSent: 0,
  ackErrors: 0,
  lastAckAt: null,
  lastAck: null,
  lastAckErrorAt: null,
  lastAckError: null,

  queuedPunches: 0,
  queuedBatches: 0,

  forwarded: 0,
  forwardedBulk: 0,
  droppedIncomplete: 0,
  deduped: 0,

  lovableOk: 0,
  lovableErr: 0,
  lovableLastAt: null,
  lovableLastOk: null,
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

  pushEvent({ type: "ack_send_attempt", sn, ackTopic, count: payload.count, logindex: payload.logindex });

  try {
    mqttClient.publish(ackTopic, JSON.stringify(payload), { qos: 1, retain: false }, (err) => {
      if (err) {
        stats.ackErrors += 1;
        stats.lastAckError = err.message;
        stats.lastAckErrorAt = new Date().toISOString();
        console.error(`ACK publish error to ${ackTopic}:`, err.message);
        pushEvent({ type: "ack_send_result", sn, ackTopic, ok: false, error: err.message, count: payload.count, logindex: payload.logindex });
      } else {
        stats.acksSent += 1;
        stats.lastAckAt = new Date().toISOString();
        stats.lastAck = { sn, count: payload.count, logindex: payload.logindex };
        console.log(`ACK sent -> ${ackTopic} count=${payload.count} logindex=${payload.logindex}`);
        pushEvent({ type: "ack_send_result", sn, ackTopic, ok: true, count: payload.count, logindex: payload.logindex });
      }
    });
  } catch (e) {
    stats.ackErrors += 1;
    stats.lastAckError = e?.message || String(e);
    stats.lastAckErrorAt = new Date().toISOString();
    console.error(`ACK publish exception to ${ackTopic}:`, stats.lastAckError);
    pushEvent({ type: "ack_send_result", sn, ackTopic, ok: false, error: stats.lastAckError, count: payload.count, logindex: payload.logindex });
  }
}

// -------------------- core handling --------------------

function storeBatch({ sn, topic, msg }) {
  try {
    stmtInsertBatch.run({
      created_at: new Date().toISOString(),
      sn,
      topic,
      cmd: msg.cmd ?? null,
      count: typeof msg.count === "number" ? msg.count : (parseInt(String(msg.count || "0"), 10) || 0),
      logindex: typeof msg.logindex === "number" ? msg.logindex : (parseInt(String(msg.logindex || "0"), 10) || 0),
      payload_json: JSON.stringify(msg),
    });
    stats.queuedBatches += 1;
  } catch (e) {
    console.error("DB storeBatch error:", e?.message || String(e));
  }
}

function storePunches(sn, normalizedRecords) {
  const now = new Date().toISOString();
  let inserted = 0;

  const tx = db.transaction((rows) => {
    for (const r of rows) {
      stmtInsertPunch.run({
        created_at: now,
        sn,
        enrollid: r.enrollid ?? null,
        timestamp: r.timestamp ?? null,
        action: r.action ?? null,
        payload_json: JSON.stringify(r),
      });
      inserted += 1;
    }
  });

  try {
    tx(normalizedRecords);
    stats.queuedPunches += inserted;
  } catch (e) {
    console.error("DB storePunches error:", e?.message || String(e));
  }

  return inserted;
}

function normalizeSendlogToRecords(msg) {
  const device_sn = msg.sn || null;
  const records = Array.isArray(msg.record) ? msg.record : [];
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

    if (DROP_INCOMPLETE && !timestamp) {
      stats.droppedIncomplete += 1;
      continue;
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

  return out;
}

async function deliverQueueBatch(limit = QUEUE_RETRY_BATCH_SIZE) {
  const rows = stmtSelectPending.all(limit);
  if (!rows.length) return { ok: true, processed: 0, sent: 0, failed: 0 };

  // Build a lovable bulk payload from queued items
  const records = rows.map((r) => safeParseJson(r.payload_json)).filter(Boolean);

  // If parsing fails, mark them failed
  const badRows = rows.filter((r) => !safeParseJson(r.payload_json));
  const now = new Date().toISOString();
  for (const br of badRows) {
    stmtMarkFailed.run({ id: br.id, at: now, err: "Invalid JSON payload_json in queue row" });
  }

  if (!records.length) return { ok: true, processed: rows.length, sent: 0, failed: badRows.length };

  const ctx = {
    sn: records[0]?.device_sn || null,
    queueIds: rows.map((r) => r.id),
    mode: "queue_retry",
  };

  try {
    await postToLovable({ action: "bulk", records }, ctx);

    // Mark all rows as sent
    const at = new Date().toISOString();
    const tx = db.transaction((ids) => {
      for (const id of ids) stmtMarkSent.run({ id, at });
    });
    tx(rows.map((r) => r.id));

    stats.forwarded += records.length;
    stats.forwardedBulk += 1;
    stats.lastForwardAt = new Date().toISOString();

    stats.lovableOk += 1;
    stats.lovableLastAt = new Date().toISOString();
    stats.lovableLastOk = true;
    stats.lovableLastError = null;

    pushEvent({ type: "queue_delivery_ok", processed: rows.length, forwardedCount: records.length });

    return { ok: true, processed: rows.length, sent: rows.length, failed: badRows.length };
  } catch (e) {
    const err = e?.message || String(e);
    const at = new Date().toISOString();

    const tx = db.transaction((ids) => {
      for (const id of ids) stmtMarkFailed.run({ id, at, err });
    });
    tx(rows.map((r) => r.id));

    stats.lovableErr += 1;
    stats.lovableLastAt = new Date().toISOString();
    stats.lovableLastOk = false;
    stats.lovableLastError = err;

    pushEvent({ type: "queue_delivery_error", processed: rows.length, error: err });

    return { ok: false, processed: rows.length, sent: 0, failed: rows.length, error: err };
  }
}

async function handleIncomingMqtt(topic, payloadStr) {
  const msg = safeParseJson(payloadStr);
  if (!msg || typeof msg !== "object") return;

  if (msg.cmd === "sendlog") {
    const sn = msg.sn || extractSnFromTopic(topic);

    // Store raw batch (audit)
    storeBatch({ sn, topic, msg });

    // Normalize records + store in local queue BEFORE ACK (so even if container crashes, you still have it)
    const normalized = normalizeSendlogToRecords({ ...msg, sn: sn || msg.sn });
    if (normalized.length) storePunches(sn, normalized);

    pushEvent({
      type: "mqtt_sendlog_received",
      mqttTopic: topic,
      sn,
      count: msg.count ?? null,
      logindex: msg.logindex ?? null,
      summary: summarizeRecords(Array.isArray(msg.record) ? msg.record : []),
      normalizedCount: normalized.length,
    });

    // ACK FIRST (device stability)
    publishSendlogAck({ sn, count: msg.count, logindex: msg.logindex });

    // Try immediate delivery of just these records (fast path)
    if (normalized.length) {
      try {
        await postToLovable({ action: "bulk", records: normalized }, { sn, mqttTopic: topic, mode: "immediate" });

        // If immediate succeeded, mark those queue rows as sent:
        // Simple approach: run a retry batch right away (it will pick up pending rows and mark sent)
        // This avoids needing a per-record id mapping.
        await deliverQueueBatch(normalized.length);

        stats.lovableOk += 1;
        stats.lovableLastAt = new Date().toISOString();
        stats.lovableLastOk = true;
        stats.lovableLastError = null;

        pushEvent({ type: "bridge_delivery_ok", sn, forwardedCount: normalized.length });
      } catch (e) {
        const err = e?.message || String(e);
        stats.lovableErr += 1;
        stats.lovableLastAt = new Date().toISOString();
        stats.lovableLastOk = false;
        stats.lovableLastError = err;
        pushEvent({ type: "bridge_delivery_error", sn, error: err, forwardedCount: normalized.length });

        // Do NOT throw; we already queued them, and retry worker will handle it
        console.error("Immediate Lovable push failed (queued for retry):", err);
      }
    }
  }
}

// ============================
// MQTT connection
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
    console.error("Bridge error:", stats.lastError);
  }
});

// ============================
// Retry worker
// ============================

let retryTimer = null;
function startRetryWorker() {
  if (retryTimer) return;
  retryTimer = setInterval(async () => {
    try {
      const s = stmtQueueStats.get();
      if ((s.pending || 0) + (s.failed || 0) === 0) return;
      await deliverQueueBatch(QUEUE_RETRY_BATCH_SIZE);
    } catch (e) {
      console.error("Retry worker error:", e?.message || String(e));
    }
  }, QUEUE_RETRY_INTERVAL_SECONDS * 1000).unref();

  console.log(`Queue retry worker: every ${QUEUE_RETRY_INTERVAL_SECONDS}s, batch size ${QUEUE_RETRY_BATCH_SIZE}`);
}

startRetryWorker();

// ============================
// HTTP endpoints
// ============================

const app = express();
app.use(helmet());
app.use(express.json({ limit: "2mb" }));
app.use(morgan("combined"));

app.get("/health", (req, res) => {
  const queue = stmtQueueStats.get();
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
    queue: { dbPath: QUEUE_DB_PATH, ...queue },
    config: {
      dedupeSeconds: DEDUPE_SECONDS,
      dropIncomplete: DROP_INCOMPLETE,
      ackTopicSuffix: MQTT_ACK_TOPIC_SUFFIX,
      retryIntervalSeconds: QUEUE_RETRY_INTERVAL_SECONDS,
      retryBatchSize: QUEUE_RETRY_BATCH_SIZE,
    },
    stats,
    time: new Date().toISOString(),
  });
});

app.get("/recent", (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit || "50", 10), 1), 500);
  const type = req.query.type ? String(req.query.type) : null;
  const sn = req.query.sn ? String(req.query.sn) : null;

  let items = recentEvents.slice();
  if (type) items = items.filter((e) => e.type === type);
  if (sn) items = items.filter((e) => (e.sn || e.device_sn) === sn);
  items = items.slice(-limit).reverse();

  res.json({ ok: true, count: items.length, filters: { limit, type, sn }, items });
});

app.get("/queue", (req, res) => {
  const s = stmtQueueStats.get();
  res.json({ ok: true, ...s, dbPath: QUEUE_DB_PATH, now: new Date().toISOString() });
});

app.get("/queue/items", (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit || "50", 10), 1), 500);
  const items = stmtLatestQueueItems.all(limit);
  res.json({ ok: true, count: items.length, items });
});

// Manual retry: pushes queued items now
app.post("/retry", async (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.body?.limit || `${QUEUE_RETRY_BATCH_SIZE}`, 10), 1), 1000);
  const result = await deliverQueueBatch(limit);
  res.json({ ok: true, result, now: new Date().toISOString() });
});

app.listen(PORT, () => {
  console.log(`MQTT→Lovable bridge listening on :${PORT}`);
  console.log(`Queue DB: ${QUEUE_DB_PATH}`);
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
