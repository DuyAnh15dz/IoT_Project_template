// Giả lập ESP32 để test toàn bộ hệ thống khi chưa có phần cứng.
// Chạy: npm run simulate
const mqtt = require('mqtt');

const MQTT_URL = process.env.MQTT_URL || 'mqtt://localhost:1883';
const DEVICE_ID = process.env.DEVICE_ID || 'esp32-01';
const INTERVAL_MS = 2000;

const client = mqtt.connect(MQTT_URL, {
  clientId: `sim-${DEVICE_ID}`,
  // Last Will: nếu thiết bị mất kết nối đột ngột, broker tự publish "offline".
  will: { topic: `iot/${DEVICE_ID}/status`, payload: 'offline', qos: 1, retain: true },
});

let temperature = 28;
let humidity = 65;
let relay = 0;

const clamp = (v, min, max) => Math.min(max, Math.max(min, v));
const walk = (v, step) => v + (Math.random() - 0.5) * step;

client.on('connect', () => {
  console.log(`[sim] ${DEVICE_ID} đã kết nối ${MQTT_URL}`);
  client.publish(`iot/${DEVICE_ID}/status`, 'online', { qos: 1, retain: true });
  client.subscribe(`iot/${DEVICE_ID}/cmd`, { qos: 1 });

  setInterval(() => {
    temperature = clamp(walk(temperature, 0.8), 20, 38);
    humidity = clamp(walk(humidity, 2), 35, 90);
    const light = Math.round(clamp(300 + Math.sin(Date.now() / 20000) * 250 + Math.random() * 30, 0, 1000));

    const payload = {
      temperature: Number(temperature.toFixed(1)),
      humidity: Number(humidity.toFixed(1)),
      light,
      relay,
    };
    client.publish(`iot/${DEVICE_ID}/data`, JSON.stringify(payload), { qos: 0 });
    console.log('[sim] publish', payload);
  }, INTERVAL_MS);
});

client.on('message', (_topic, message) => {
  try {
    const cmd = JSON.parse(message.toString());
    if (cmd.relay === 'on') relay = 1;
    if (cmd.relay === 'off') relay = 0;
    console.log('[sim] nhận lệnh', cmd, '-> relay =', relay);
  } catch {
    /* bỏ qua lệnh sai định dạng */
  }
});

process.on('SIGINT', () => {
  client.publish(`iot/${DEVICE_ID}/status`, 'offline', { qos: 1, retain: true }, () => process.exit(0));
});
