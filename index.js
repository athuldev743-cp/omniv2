require("dotenv").config();

const express = require("express");
const {
  default: makeWASocket,
  DisconnectReason,
  fetchLatestBaileysVersion,
  initAuthCreds,
  proto,
  BufferJSON,
} = require("@whiskeysockets/baileys");

const { Boom } = require("@hapi/boom");
const { Pool } = require("pg");
const QRCode = require("qrcode");
const P = require("pino");
const cors = require("cors");

const app = express();

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3001;

const IDLE_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes
const MAX_RECONNECTS = 5;

// ─────────────────────────────────────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

app.use(express.json({ limit: "50mb" }));

app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allowedHeaders: ["Content-Type", "x-user-email"],
    credentials: false,
  })
);

// Request logging
app.use((req, res, next) => {
  console.log(
    `[INBOUND] ${req.method} ${req.url} - User: ${
      req.headers["x-user-email"] || "none"
    }`
  );

  next();
});

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE
// ─────────────────────────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString: process.env.AIVEN_DATABASE_URL,
  ssl: {
    rejectUnauthorized: true,
    ca: process.env.AIVEN_CA_CERT,
  },
});

pool.on("error", (err) => {
  console.error("[PostgreSQL] Unexpected pool error:", err.message);
});

// ─────────────────────────────────────────────────────────────────────────────
// SESSION STATE
// userId/email → socket/session information
// ─────────────────────────────────────────────────────────────────────────────
const { randomUUID } = require("crypto");

// ─── SESSION STATE: keyed by "email::connectionId" ─────────────────────────
const sessions = new Map();
const sessionKey = (userId, connId) => `${userId}::${connId}`;

function getSession(userId, connId) {
  const k = sessionKey(userId, connId);
  if (!sessions.has(k)) {
    sessions.set(k, {
      sock: null, qr: null, connected: false,
      reconnectAttempts: 0, idleTimer: null, launching: false,
      pairingCode: null,
    });
  }
  return sessions.get(k);
}

// ─── AUTH STATE: scoped by (userId, connectionId) ───────────────────────────
async function useAuthState(userId, connId) {
  const ns = userId.toLowerCase().trim();

  async function readData(key) {
    try {
      const res = await pool.query(
        "SELECT value FROM wa_auth_state WHERE user_id=$1 AND connection_id=$2 AND key=$3",
        [ns, connId, key]
      );
      if (res.rows.length === 0) return null;
      const raw = res.rows[0].value;
      const str = typeof raw === "string" ? raw : JSON.stringify(raw);
      return JSON.parse(str, BufferJSON.reviver);
    } catch (e) { console.error(`[Auth:${ns}:${connId}] Read error [${key}]:`, e.message); return null; }
  }

  async function writeData(key, value) {
    try {
      const json = JSON.stringify(value, BufferJSON.replacer);
      await pool.query(
        `INSERT INTO wa_auth_state (user_id, connection_id, key, value, updated_at)
         VALUES ($1,$2,$3,$4::jsonb,NOW())
         ON CONFLICT (user_id, connection_id, key) DO UPDATE SET value=$4::jsonb, updated_at=NOW()`,
        [ns, connId, key, json]
      );
    } catch (e) { console.error(`[Auth:${ns}:${connId}] Write error [${key}]:`, e.message); }
  }

  async function deleteData(key) {
    try {
      await pool.query(
        "DELETE FROM wa_auth_state WHERE user_id=$1 AND connection_id=$2 AND key=$3",
        [ns, connId, key]
      );
    } catch (e) { console.error(`[Auth:${ns}:${connId}] Delete error:`, e.message); }
  }

  const creds = (await readData("creds")) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(ids.map(async (id) => {
            let value = await readData(`${type}-${id}`);
            if (type === "app-state-sync-key" && value) value = proto.Message.AppStateSyncKeyData.fromObject(value);
            data[id] = value;
          }));
          return data;
        },
        set: async (data) => {
          for (const category of Object.keys(data)) {
            for (const id of Object.keys(data[category])) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              value ? await writeData(key, value) : await deleteData(key);
            }
          }
        },
      },
    },
    saveCreds: async () => await writeData("creds", creds),
  };
}

async function clearAuthForConnection(userId, connId) {
  const ns = userId.toLowerCase().trim();
  await pool.query("DELETE FROM wa_auth_state WHERE user_id=$1 AND connection_id=$2", [ns, connId]);
  await pool.query("DELETE FROM wa_connections WHERE user_id=$1 AND connection_id=$2", [ns, connId]);
}

async function upsertConnectionRecord(userId, connId, fields) {
  const ns = userId.toLowerCase().trim();
  const cols = ["label", "phone_number", "status"];
  const existing = {};
  for (const c of cols) if (fields[c] !== undefined) existing[c] = fields[c];
  await pool.query(
    `INSERT INTO wa_connections (user_id, connection_id, label, phone_number, status)
     VALUES ($1,$2,$3,$4,$5)
     ON CONFLICT (user_id, connection_id) DO UPDATE SET
       label = COALESCE($3, wa_connections.label),
       phone_number = COALESCE($4, wa_connections.phone_number),
       status = COALESCE($5, wa_connections.status)`,
    [ns, connId, existing.label ?? null, existing.phone_number ?? null, existing.status ?? null]
  );
}

function resetIdleTimer(userId, connId) {
  const s = getSession(userId, connId);
  if (s.idleTimer) clearTimeout(s.idleTimer);
  s.idleTimer = setTimeout(() => {
    if (!s.connected) return;
    try { s.sock?.end(); } catch {}
    s.sock = null; s.connected = false; s.qr = null;
  }, IDLE_TIMEOUT_MS);
}

// ─── LAUNCH SOCKET: usePairingCode enables phone-number login instead of QR ─
async function launchSocket(userId, connId, { usePairingCode = false, phoneNumber = "" } = {}) {
  const s = getSession(userId, connId);
  if (s.sock || s.launching) return;
  s.launching = true;

  try {
    const { state, saveCreds } = await useAuthState(userId, connId);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
      version,
      logger: P({ level: "silent" }),
      auth: state,
      printQRInTerminal: false,
      browser: ["Chrome (Linux)", "Chrome", "126.0.6478.114"],
      connectTimeoutMs: 60000,
      qrTimeout: 60000,
    });

    s.sock = sock;
    s.launching = false;

    // ── Pairing code flow: request code once socket exists, before QR fires
    if (usePairingCode && phoneNumber && !state.creds.registered) {
      try {
        const cleanPhone = String(phoneNumber).replace(/\D/g, "");
        const code = await sock.requestPairingCode(cleanPhone);
        s.pairingCode = code;
        console.log(`[Socket:${userId}:${connId}] Pairing code: ${code}`);
      } catch (e) {
        console.error(`[Socket:${userId}:${connId}] Pairing code request failed:`, e.message);
      }
    }

sock.ev.on("connection.update", async (update) => {
  try {
    const { connection, lastDisconnect, qr } = update;

    if (qr && !usePairingCode) {
      if (s.qr !== qr) {
        try {
          const dataUrl = await QRCode.toDataURL(qr);
          s.qr = dataUrl.split(",")[1];
        } catch (e) { console.error(`[Socket:${userId}:${connId}] QR gen error:`, e.message); }
      }
    }

    if (connection === "open") {
      s.connected = true; s.qr = null; s.pairingCode = null; s.reconnectAttempts = 0; s.launching = false;
      resetIdleTimer(userId, connId);
      const phone = sock.user?.id?.split(":")[0] || phoneNumber || "";
      await upsertConnectionRecord(userId, connId, { phone_number: phone, status: "connected" });
      console.log(`✅ [Socket:${userId}:${connId}] Connected as ${phone}`);
    }

    if (connection === "close") {
      s.connected = false; s.qr = null; s.sock = null; s.launching = false;
      const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
      await upsertConnectionRecord(userId, connId, { status: "disconnected" });

      if (code === 428 || code === DisconnectReason.connectionClosed) {
        setTimeout(() => launchSocket(userId, connId).catch(console.error), 2000);
        return;
      }
      if (code === DisconnectReason.loggedOut) {
        await clearAuthForConnection(userId, connId);
        s.reconnectAttempts = 0;
        return;
      }
      if (code === 440) { setTimeout(() => launchSocket(userId, connId).catch(console.error), 15000); return; }
      if (code === 515) { setTimeout(() => launchSocket(userId, connId).catch(console.error), 2000); return; }

      s.reconnectAttempts++;
      if (s.reconnectAttempts >= MAX_RECONNECTS) {
        s.reconnectAttempts = 0;
        setTimeout(() => launchSocket(userId, connId).catch(console.error), 60000);
        return;
      }
      setTimeout(() => launchSocket(userId, connId).catch(console.error), Math.min(s.reconnectAttempts * 5000, 30000));
    }
  } catch (err) {
    console.error(`[Socket:${userId}:${connId}] connection.update handler error:`, err);
  }
});

    sock.ev.on("creds.update", saveCreds);
  } catch (err) {
    s.launching = false; s.sock = null;
    console.error(`[Socket:${userId}:${connId}] Launch error:`, err);
  }
}

function requireUser(req, res, next) {
  const userId = req.headers["x-user-email"] || req.query.user;
  if (!userId) return res.status(400).json({ error: "x-user-email header required" });
  req.userId = String(userId).toLowerCase().trim();
  next();
}

function toJID(phone) { /* unchanged from your current file */
  let p = String(phone).replace(/\D/g, "");
  if (p.startsWith("00")) p = p.slice(2); else if (p.startsWith("0")) p = p.slice(1);
  if (p.length === 10 && ["6","7","8","9"].includes(p[0])) p = "91" + p;
  return `${p}@s.whatsapp.net`;
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── ENDPOINTS ────────────────────────────────────────────────────────────

// List all connections for a user
app.get("/connections", requireUser, async (req, res) => {
  const { rows } = await pool.query(
    "SELECT connection_id, label, phone_number, status, created_at FROM wa_connections WHERE user_id=$1 ORDER BY created_at ASC",
    [req.userId]
  );
  const withLive = rows.map(r => {
    const s = sessions.get(sessionKey(req.userId, r.connection_id));
    return { ...r, connected: !!s?.connected, has_qr: !!s?.qr };
  });
  res.json({ connections: withLive });
});

// Create a new connection slot (does NOT auto-launch — QR/pairing pulls it)
app.post("/connections/new", requireUser, async (req, res) => {
  const connId = randomUUID();
  const label = req.body?.label || `WhatsApp ${new Date().toLocaleDateString()}`;
  await upsertConnectionRecord(req.userId, connId, { label, phone_number: "", status: "pending" });
  res.json({ connection_id: connId, label });
});

app.delete("/connections/:connId", requireUser, async (req, res) => {
  const { connId } = req.params;
  const s = getSession(req.userId, connId);
  try { if (s.sock) { try { await s.sock.logout(); } catch {} s.sock = null; } } catch {}
  s.connected = false; s.qr = null; s.launching = false;
  if (s.idleTimer) clearTimeout(s.idleTimer);
  await clearAuthForConnection(req.userId, connId);
  sessions.delete(sessionKey(req.userId, connId));
  res.json({ success: true });
});

// QR for a specific connection
app.get("/connections/:connId/qr", requireUser, async (req, res) => {
  const { connId } = req.params;
  const s = getSession(req.userId, connId);
  if (s.connected) return res.json({ status: "connected" });
  if (!s.sock && !s.launching) launchSocket(req.userId, connId).catch(console.error);
  let retries = 0;
  while (!s.qr && retries < 4) { await sleep(500); retries++; }
  if (!s.qr) return res.status(202).json({ status: "waiting" });
  res.json({ status: "pending", qr: s.qr });
});

// Pairing-code login for a specific connection
app.post("/connections/:connId/pair", requireUser, async (req, res) => {
  const { connId } = req.params;
  const { phone_number } = req.body;
  if (!phone_number) return res.status(400).json({ error: "phone_number_required" });

  const s = getSession(req.userId, connId);
  if (s.connected) return res.json({ status: "connected" });
  if (!s.sock && !s.launching) {
    launchSocket(req.userId, connId, { usePairingCode: true, phoneNumber: phone_number }).catch(console.error);
  }
  let retries = 0;
  while (!s.pairingCode && retries < 6) { await sleep(500); retries++; }
  if (!s.pairingCode) return res.status(202).json({ status: "waiting" });
  res.json({ status: "pending", pairing_code: s.pairingCode });
});

app.get("/connections/:connId/status", requireUser, (req, res) => {
  const s = getSession(req.userId, req.params.connId);
  res.json({ connected: s.connected, hasSocket: !!s.sock, hasQR: !!s.qr });
});

app.post("/connections/:connId/send", requireUser, async (req, res) => {
  const s = getSession(req.userId, req.params.connId);
  if (!s.connected || !s.sock) return res.status(503).json({ error: "not_connected" });
  const { phone, message } = req.body;
  try {
    resetIdleTimer(req.userId, req.params.connId);
    const result = await s.sock.sendMessage(toJID(phone), { text: message });
    res.json({ success: true, messageId: result.key.id });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Add these two routes to index.js, right after /connections/:connId/send ──
// ── Add these two routes to index.js, right after /connections/:connId/send ──

function b64ToBuffer(dataOrB64) {
  const clean = String(dataOrB64).includes(",") ? dataOrB64.split(",")[1] : dataOrB64;
  return Buffer.from(clean, "base64");
}

app.post("/connections/:connId/send-image", requireUser, async (req, res) => {
  const s = getSession(req.userId, req.params.connId);
  if (!s.connected || !s.sock) return res.status(503).json({ error: "not_connected" });
  const { phone, image_base64, caption } = req.body;
  if (!phone || !image_base64) return res.status(400).json({ error: "phone_and_image_required" });
  try {
    resetIdleTimer(req.userId, req.params.connId);
    const result = await s.sock.sendMessage(toJID(phone), {
      image: b64ToBuffer(image_base64),
      caption: caption || "",
    });
    res.json({ success: true, messageId: result.key.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/connections/:connId/send-bulk-media", requireUser, async (req, res) => {
  const s = getSession(req.userId, req.params.connId);
  if (!s.connected || !s.sock) return res.status(503).json({ error: "not_connected" });
  const { phone, text_message, photos_array = [], pdfs_array = [], audio_voice_base64 } = req.body;
  if (!phone) return res.status(400).json({ error: "phone_required" });

  resetIdleTimer(req.userId, req.params.connId);
  const jid = toJID(phone);
  let lastId = null;
  let captionUsed = false;
  const errors = [];

  console.log(`[send-bulk-media] photos=${photos_array.length} pdfs=${pdfs_array.length} hasAudio=${!!audio_voice_base64} hasText=${!!text_message}`);

  for (let i = 0; i < photos_array.length; i++) {
    try {
      const photoSrc = photos_array[i];
      const buf = photoSrc.startsWith("http")
        ? Buffer.from((await (await fetch(photoSrc)).arrayBuffer()))
        : b64ToBuffer(photoSrc);
      const isFirst = i === 0;
      const msg = { image: buf };
      if (isFirst && text_message) { msg.caption = text_message; captionUsed = true; }
      const r = await s.sock.sendMessage(jid, msg);
      lastId = r.key.id;
      await sleep(800);
    } catch (e) {
      console.error(`[send-bulk-media] photo ${i} failed:`, e.message);
      errors.push(`photo${i}: ${e.message}`);
    }
  }

  if (text_message && !captionUsed) {
    try {
      const r = await s.sock.sendMessage(jid, { text: text_message });
      lastId = r.key.id;
      await sleep(800);
    } catch (e) {
      console.error(`[send-bulk-media] text failed:`, e.message);
      errors.push(`text: ${e.message}`);
    }
  }

  for (let i = 0; i < pdfs_array.length; i++) {
    try {
      const pdfSrc = pdfs_array[i];
      const buf = pdfSrc.startsWith("http")
        ? Buffer.from((await (await fetch(pdfSrc)).arrayBuffer()))
        : b64ToBuffer(pdfSrc);
      const r = await s.sock.sendMessage(jid, { document: buf, mimetype: "application/pdf", fileName: `document${i + 1}.pdf` });
      lastId = r.key.id;
      await sleep(800);
    } catch (e) {
      console.error(`[send-bulk-media] pdf ${i} failed:`, e.message);
      errors.push(`pdf${i}: ${e.message}`);
    }
  }

  if (audio_voice_base64) {
    try {
      const r = await s.sock.sendMessage(jid, { audio: b64ToBuffer(audio_voice_base64), mimetype: "audio/mp4", ptt: true });
      lastId = r.key.id;
    } catch (e) {
      console.error(`[send-bulk-media] audio failed:`, e.message);
      errors.push(`audio: ${e.message}`);
    }
  }

  if (!lastId && errors.length) {
    return res.status(500).json({ error: errors.join("; ") });
  }
  res.json({ success: true, messageId: lastId, partialErrors: errors.length ? errors : undefined });
});
// (repeat the same pattern for /connections/:connId/send-image and /connections/:connId/send-bulk-media,
//  identical bodies to your current /send-image and /send-bulk-media, just reading req.params.connId
//  and calling getSession(req.userId, req.params.connId) instead of getSession(req.userId))

app.get("/health", (req, res) => {
  res.json({ ok: true, activeSessions: sessions.size, connectedSessions: Array.from(sessions.values()).filter(s => s.connected).length, uptime: process.uptime() });
});

async function relaunchExistingSessions() {
  const { rows } = await pool.query("SELECT DISTINCT user_id, connection_id FROM wa_auth_state WHERE key='creds'");
  for (const row of rows) launchSocket(row.user_id, row.connection_id).catch(console.error);
}

app.listen(PORT, () => {
  console.log(`🚀 Multi-connection WA Bridge on port ${PORT}`);
  relaunchExistingSessions().catch(console.error);
});

