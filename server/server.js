const path = require('path');
const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');
const mqtt = require('mqtt');
const db = require('./db');

const PORT = Number(process.env.PORT) || 3000;
const MQTT_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';
const DEVICE_ID = process.env.DEVICE_ID || 'esp32-01';

// Topic quy ước:
//   iot/<device>/data    ESP32 -> server   {"temperature":28.5,"humidity":70,"light":340,"relay":0}
//   iot/<device>/status  ESP32 -> server   "online" | "offline" (offline do LWT của broker gửi)
//   iot/<device>/cmd     server -> ESP32   {"relay":"on"} | {"relay":"off"}
const TOPIC_DATA = 'iot/+/data';
const TOPIC_STATUS = 'iot/+/status';
const cmdTopic = (device) => `iot/${device}/cmd`;

const deviceStatus = {}; // deviceId -> 'online' | 'offline'

// ---------- HTTP + REST ----------
const app = express();
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/history', async (req, res) => {
  const sensor = String(req.query.sensor || '');
  if (!sensor) return res.status(400).json({ error: 'sensor is required' });
  const minutes = Math.min(Math.max(parseInt(req.query.minutes, 10) || 60, 1), 7 * 24 * 60);
  try {
    res.json(await db.history(DEVICE_ID, sensor, Date.now() - minutes * 60_000));
  } catch (err) {
    console.error('[db] history lỗi:', err.message);
    res.status(500).json({ error: 'database error' });
  }
});

app.get('/api/latest', async (_req, res) => {
  try {
    res.json(await db.latest(DEVICE_ID));
  } catch (err) {
    console.error('[db] latest lỗi:', err.message);
    res.status(500).json({ error: 'database error' });
  }
});

const server = http.createServer(app);

// ---------- WebSocket (server <-> dashboard) ----------
const wss = new WebSocketServer({ server, path: '/ws' });

function broadcast(message) {
  const text = JSON.stringify(message);
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(text);
  }
}

wss.on('connection', async (socket) => {
  socket.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'command' && (msg.relay === 'on' || msg.relay === 'off')) {
      mqttClient.publish(cmdTopic(DEVICE_ID), JSON.stringify({ relay: msg.relay }), { qos: 1 });
    }
  });

  // Gửi trạng thái hiện tại ngay khi dashboard kết nối.
  let latest = [];
  try {
    latest = await db.latest(DEVICE_ID);
  } catch (err) {
    console.error('[db] latest lỗi:', err.message);
  }
  if (socket.readyState === 1) {
    socket.send(JSON.stringify({
      type: 'snapshot',
      device: DEVICE_ID,
      status: deviceStatus[DEVICE_ID] || 'unknown',
      latest,
    }));
  }
});

// ---------- MQTT (broker <-> server) ----------
const mqttClient = mqtt.connect(MQTT_URL, {
  clientId: `iot-server-${Math.random().toString(16).slice(2, 8)}`,
  reconnectPeriod: 2000,
});

mqttClient.on('connect', () => {
  console.log(`[mqtt] đã kết nối ${MQTT_URL}`);
  mqttClient.subscribe([TOPIC_DATA, TOPIC_STATUS], { qos: 1 });
});
mqttClient.on('error', (err) => console.error('[mqtt] lỗi:', err.message));

mqttClient.on('message', async (topic, payload) => {
  const [, deviceId, kind] = topic.split('/');
  const text = payload.toString();

  if (kind === 'status') {
    deviceStatus[deviceId] = text === 'online' ? 'online' : 'offline';
    broadcast({ type: 'status', device: deviceId, status: deviceStatus[deviceId] });
    return;
  }

  if (kind !== 'data') return;

  let data;
  try { data = JSON.parse(text); } catch {
    console.warn(`[mqtt] bỏ qua payload không phải JSON từ ${deviceId}`);
    return;
  }

  // Chỉ nhận các trường là số hợp lệ.
  const values = {};
  for (const [sensor, value] of Object.entries(data)) {
    if (typeof value === 'number' && Number.isFinite(value)) values[sensor] = value;
  }
  if (Object.keys(values).length === 0) return;

  const ts = Date.now();
  try {
    await db.saveReadings(deviceId, values, ts);
  } catch (err) {
    console.error('[db] không lưu được:', err.message); // vẫn đẩy realtime lên dashboard
  }
  for (const [sensor, value] of Object.entries(values)) {
    broadcast({ type: 'reading', device: deviceId, sensor, value, ts });
  }
});

db.init()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`[http] dashboard: http://localhost:${PORT}`);
    });
  })
  .catch((err) => {
    console.error('[db] không kết nối được MySQL:', err.message);
    process.exit(1);
  });
