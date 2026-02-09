require("dotenv").config();

const express = require("express");
const helmet = require("helmet");
const morgan = require("morgan");
const mqtt = require("mqtt");

const PORT = parseInt(process.env.PORT || "3000", 10);
const MQTT_URL = process.env.MQTT_URL;
const MQTT_SUB_TOPIC = process.env.MQTT_SUB_TOPIC || "aiface/+/sub";

const LOVABLE_ENDPOINT = process.env.LOVABLE_ENDPOINT || "https://posro-web-connect.lovable.app/attendance-api";
const LOVABLE_API_KEY = process.env.LOVABLE_API_KEY;

const DEDUPE_SECONDS = Math.max(parseInt(process.env.DEDUPE_SECONDS || "0", 10), 0);

if (!MQTT_URL) {
  console.error("ERROR: MQTT_URL is required.");
  process.exit(1);
}
if (!LOVABLE_API_KEY) {
  console.error("ERROR: LOVABLE_API_KEY is required.");
  process.exit(1);
}

function safeParseJson(payload) {
  try { return JSON.parse(payload); } catch { return null; }
}

// Simple in-memory dedupe: key = device_sn|enrollid|timestamp|action
const seen = new Map();
function dedupe(key) {
  if (!DEDUPE_SECONDS) return false;
  const now = Date.now();
  // cleanup
  for (const [k, ts] of seen.entries()) {
    if (now - ts > DEDUPE_SECONDS * 1000) seen.delete(k);
  }
  if (seen.has(key)) return true;
  seen.set(key, now);
  return false;
}

function inferAction(rec) {
  // Many terminals use inout 0=in, 1=out. If yours differs, adjust here.
  if (rec.inout === 0 || rec.inout === "0") return "clock_in";
  if (rec.inout === 1 || rec.inout === "1") return "clock_out";
  // If unknown, default to clock_in to avoid losing data (you can fix later).
  return "clock_in";
}

async function postToLovable(payload) {
  const res = await fetch(LOVABLE_ENDPOINT, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": LOVABLE_API_KEY
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch { data = { raw: text }; }

  if (!res.ok) {
    const err = `Lovable error ${res.status}: ${JSON.stringify(data)}`;
    throw new Error(err);
  }
  return data;
}

let stats = {
  mqttConnected: false,
  receivedMessages: 0,
  forwarded: 0,
  forwardedBulk: 0,
  errors: 0,
  lastError: null,
  lastForwardAt: null
};

async function handleSendLog(msg) {
  const device_sn = msg.sn || null;
  const records = Array.isArray(msg.record) ? msg.record : [];
  if (!records.length) return;

  // Convert each biometric record into a Lovable-friendly action record.
  const out = [];
  for (const rec of records) {
    const enrollid = typeof rec.enrollid === "number" ? rec.enrollid : parseInt(String(rec.enrollid || "0"), 10);
    if (!enrollid || enrollid <= 0) continue;

    const action = inferAction(rec);
    const timestamp = rec.time || msg.cloudtime || new Date().toISOString();

    const key = `${device_sn}|${enrollid}|${timestamp}|${action}`;
    if (dedupe(key)) continue;

    out.push({
      action,
      enrollid,
      timestamp,
      device_sn,
      biometric_verified: true,
      notes: null
    });
  }

  if (!out.length) return;

  // Use bulk to reduce requests.
  const payload = { action: "bulk", records: out };

  const result = await postToLovable(payload);
  stats.forwarded += out.length;
  stats.forwardedBulk += 1;
  stats.lastForwardAt = new Date().toISOString();

  return result;
}

async function handleIncomingMqtt(topic, payloadStr) {
  const msg = safeParseJson(payloadStr);
  if (!msg || typeof msg !== "object") return;

  // Only care about cmd:"sendlog" (attendance punch logs)
  if (msg.cmd === "sendlog") {
    await handleSendLog(msg);
  }
}

const mqttClient = mqtt.connect(MQTT_URL, {
  clean: true,
  reconnectPeriod: 2000,
  connectTimeout: 20000
});

mqttClient.on("connect", () => {
  stats.mqttConnected = true;
  console.log("MQTT connected:", MQTT_URL);
  mqttClient.subscribe(MQTT_SUB_TOPIC, { qos: 1 }, (err) => {
    if (err) console.error("MQTT subscribe error:", err.message);
    else console.log("Subscribed to:", MQTT_SUB_TOPIC);
  });
});

mqttClient.on("reconnect", () => {
  stats.mqttConnected = false;
  console.log("MQTT reconnecting...");
});

mqttClient.on("error", (err) => {
  stats.mqttConnected = false;
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

// HTTP status
const app = express();
app.use(helmet());
app.use(express.json({ limit: "1mb" }));
app.use(morgan("combined"));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    mqtt: { url: MQTT_URL, subscribed: MQTT_SUB_TOPIC, connected: mqttClient.connected },
    lovable: { endpoint: LOVABLE_ENDPOINT },
    stats,
    time: new Date().toISOString()
  });
});

app.listen(PORT, () => {
  console.log(`MQTT→Lovable bridge listening on :${PORT}`);
});
