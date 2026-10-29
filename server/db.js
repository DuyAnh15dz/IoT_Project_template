const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST || 'localhost',
  port: Number(process.env.MYSQL_PORT) || 3306,
  user: process.env.MYSQL_USER || 'iot',
  password: process.env.MYSQL_PASSWORD || 'iotpass',
  database: process.env.MYSQL_DATABASE || 'iot',
  waitForConnections: true,
  connectionLimit: 10,
});

// Mỗi dòng trong `readings` là một giá trị của một sensor tại một thời điểm.
// Thiết kế dạng "dọc" này cho phép thêm sensor mới mà không phải sửa schema.
// `ts` là mili giây (Unix epoch) để dashboard dùng trực tiếp với JavaScript.
async function init() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS devices (
      id        VARCHAR(64) PRIMARY KEY,
      last_seen BIGINT      NOT NULL
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS readings (
      id        BIGINT AUTO_INCREMENT PRIMARY KEY,
      device_id VARCHAR(64) NOT NULL,
      sensor    VARCHAR(64) NOT NULL,
      \`value\`   DOUBLE      NOT NULL,
      ts        BIGINT      NOT NULL,
      INDEX idx_readings_lookup (device_id, sensor, ts)
    )
  `);
}

// Lưu nhiều sensor của cùng một lần đo trong một transaction.
async function saveReadings(deviceId, values, ts) {
  const rows = Object.entries(values).map(([sensor, value]) => [deviceId, sensor, value, ts]);
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    await conn.query(
      'INSERT INTO devices (id, last_seen) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_seen = VALUES(last_seen)',
      [deviceId, ts]
    );
    await conn.query('INSERT INTO readings (device_id, sensor, `value`, ts) VALUES ?', [rows]);
    await conn.commit();
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

// Trả về dữ liệu theo thứ tự thời gian tăng dần để vẽ biểu đồ.
async function history(deviceId, sensor, sinceMs, limit = 2000) {
  const [rows] = await pool.query(
    `SELECT ts, \`value\` FROM readings
     WHERE device_id = ? AND sensor = ? AND ts >= ?
     ORDER BY ts DESC LIMIT ?`,
    [deviceId, sensor, sinceMs, limit]
  );
  return rows.reverse();
}

// Giá trị mới nhất của từng sensor.
async function latest(deviceId) {
  const [rows] = await pool.query(
    `SELECT r.sensor, r.\`value\`, r.ts FROM readings r
     JOIN (SELECT sensor, MAX(ts) AS ts FROM readings WHERE device_id = ? GROUP BY sensor) m
       ON r.sensor = m.sensor AND r.ts = m.ts
     WHERE r.device_id = ?`,
    [deviceId, deviceId]
  );
  return rows;
}

module.exports = { init, saveReadings, history, latest };
