require('dotenv').config();
const fs = require("fs");
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
// ... rest of your code
app.use(express.json({ limit: "50mb" }));
app.use(cors({
  origin: "*", // Temporarily allow everything to rule out CORS
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "x-user-email"], // Explicitly allow your header
  credentials: false // Must be false if origin is "*"
}));


app.use((req, res, next) => {
  console.log(`[INBOUND] ${req.method} ${req.url} - Headers:`, req.headers['x-user-email']);
  next();
});

// ─── DB Pool ──────────────────────────────────────────────────────────────────


const pool = new Pool({
  connectionString: process.env.AIVEN_DATABASE_URL,
  ssl: {
    rejectUnauthorized: true,
    ca: process.env.AIVEN_CA_CERT,  // ← env var instead of file
  },
});
const PORT = process.env.PORT || 3001;

// ─── Per-user in-memory state ─────────────────────────────────────────────────
// userId (email) → { sock, qr, connected, reconnectAttempts, idleTimer }
const sessions = new Map();

const IDLE_TIMEOUT_MS = 30 * 60 * 1000; // drop socket after 30 min idle (no sends)
const MAX_RECONNECTS  = 5;


async function updateDatabaseStatus(userId, status) {
    try {
        await fetch(`${process.env.BACKEND_API_URL}/profile/update-status`, {
            method: 'PATCH',
            headers: { 
                'Content-Type': 'application/json',
                'X-User-Email': userId 
            },
            body: JSON.stringify({ whatsapp_status: status })
        });
    } catch (err) {
        console.error(`[Sync] Failed to update DB for ${userId}:`, err);
    }
}

// ─── Auth State (namespaced by userId) ───────────────────────────────────────
async function useAuthState(userId) {
  const ns = userId.toLowerCase().trim(); // use email as namespace

  async function readData(key) {
    try {
      const res = await pool.query(
        "SELECT value FROM wa_auth_state WHERE user_id = $1 AND key = $2",
        [ns, key]
      );
      if (res.rows.length === 0) return null;
      const raw = res.rows[0].value;
      const str = typeof raw === "string" ? raw : JSON.stringify(raw);
      return JSON.parse(str, BufferJSON.reviver);
    } catch (e) {
      console.error(`[Auth:${ns}] Read error [${key}]:`, e.message);
      return null;
    }
  }

  async function writeData(key, value) {
    try {
      const json = JSON.stringify(value, BufferJSON.replacer);
      await pool.query(
        `INSERT INTO wa_auth_state (user_id, key, value, updated_at)
         VALUES ($1, $2, $3::jsonb, NOW())
         ON CONFLICT (user_id, key) DO UPDATE
         SET value = $3::jsonb, updated_at = NOW()`,
        [ns, key, json]
      );
    } catch (e) {
      console.error(`[Auth:${ns}] Write error [${key}]:`, e.message);
    }
  }

  async function deleteData(key) {
    try {
      await pool.query(
        "DELETE FROM wa_auth_state WHERE user_id = $1 AND key = $2",
        [ns, key]
      );
    } catch (e) {
      console.error(`[Auth:${ns}] Delete error:`, e.message);
    }
  }

  const creds = (await readData("creds")) || initAuthCreds();
  console.log(`[Auth:${ns}] Creds loaded: registered=${!!creds.registrationId}, noiseKey=${!!creds.noiseKey}`);

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === "app-state-sync-key" && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
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

async function clearAuthForUser(userId) {
  const ns = userId.toLowerCase().trim();
  try {
    await pool.query("DELETE FROM wa_auth_state WHERE user_id = $1", [ns]);
    console.log(`[Auth:${ns}] Auth cleared from DB`);
  } catch (e) {
    console.error(`[Auth:${ns}] Clear error:`, e.message);
  }
}

// ─── Session helpers ──────────────────────────────────────────────────────────
function getSession(userId) {
  if (!sessions.has(userId)) {
    sessions.set(userId, {
      sock: null,
      qr: null,
      connected: false,
      reconnectAttempts: 0,
      idleTimer: null,
    });
  }
  return sessions.get(userId);
}

function resetIdleTimer(userId) {
  const s = getSession(userId);
  if (s.idleTimer) clearTimeout(s.idleTimer);
  s.idleTimer = setTimeout(() => {
    if (!s.connected) return; // already gone
    console.log(`[Session:${userId}] Idle timeout — closing socket to free memory`);
    try { s.sock?.end(); } catch {}
    s.sock = null;
    s.connected = false;
    s.qr = null;
  }, IDLE_TIMEOUT_MS);
}

// ─── Core: launch a socket for one user ───────────────────────────────────────
async function launchSocket(userId) {
  const s = getSession(userId);
  if (s.sock) return; // already running

  const { state, saveCreds } = await useAuthState(userId);
  const { version } = await fetchLatestBaileysVersion();

  console.log(`[Socket:${userId}] Initialising…`);

s.sock = makeWASocket({
    version,          // ← use fetched version
    logger: P({ level: "silent" }),   // change debug → silent to reduce noise
    auth: state,
    printQRInTerminal: false,
    browser: ["Chrome (Linux)", "Chrome", "126.0.6478.114"],  // more realistic browser string
    connectTimeoutMs: 60000,
    qrTimeout: 60000,
});

  s.sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;
    
    // Explicit QR handling
    if (qr) {
        if (s.qr !== qr) { // Only log/update if the QR actually changed
            console.log(`[Socket:${userId}] New QR received`);
            const dataUrl = await QRCode.toDataURL(qr);
            s.qr = dataUrl.split(",")[1];
        }
    }

    if (connection === "open") {
      s.connected = true;
      s.qr = null;
      s.reconnectAttempts = 0;
      resetIdleTimer(userId);
      console.log(`✅ [Socket:${userId}] Connected`);
      
      // ✅ SYNC TO DATABASE: Status is now LIVE
      await fetch(`${process.env.BACKEND_API_URL}/whatsapp/update-status`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-User-Email': userId },
          body: JSON.stringify({ status: 'connected' })
      }).catch(e => console.error(`[Sync] Failed to set connected: ${e.message}`));
    }

    if (connection === "close") {
      s.connected = false;
      s.qr = null;
      s.sock = null;

      // ✅ SYNC TO DATABASE: Status is now DISCONNECTED
      await fetch(`${process.env.BACKEND_API_URL}/whatsapp/update-status`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-User-Email': userId },
          body: JSON.stringify({ status: 'disconnected' })
      }).catch(e => console.error(`[Sync] Failed to set disconnected: ${e.message}`));

      const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
      // ... rest of your disconnect logic
    
      console.log(`[Socket:${userId}] Closed. Code: ${code}`);

      if (code === DisconnectReason.loggedOut) {
        await clearAuthForUser(userId);
        s.reconnectAttempts = 0;
        setTimeout(() => launchSocket(userId), 5000);
      } else if (code === 440) {
        setTimeout(() => launchSocket(userId), 15000);
      } else if (code === 515) {
        setTimeout(() => launchSocket(userId), 2000);
      } else {
        s.reconnectAttempts++;
        if (s.reconnectAttempts >= MAX_RECONNECTS) {
          s.reconnectAttempts = 0;
          setTimeout(() => launchSocket(userId), 60000);
          return;
        }
        const delay = Math.min(s.reconnectAttempts * 5000, 30000);
        setTimeout(() => launchSocket(userId), delay);
      }
    }
  });

  s.sock.ev.on("creds.update", saveCreds);
}

// ─── Middleware: extract userId from header ───────────────────────────────────
function requireUser(req, res, next) {
  const userId = req.headers["x-user-email"] || req.query.user;
  if (!userId) return res.status(400).json({ error: "x-user-email header required" });
  req.userId = userId.toLowerCase().trim();
  next();
}

function toJID(phone) {
  let p = String(phone).replace(/\D/g, "");
  if (p.startsWith("00")) p = p.slice(2);
  else if (p.startsWith("0")) p = p.slice(1);
  if (p.length === 10 && ["6", "7", "8", "9"].includes(p[0])) p = "91" + p;
  return `${p}@s.whatsapp.net`;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ─── Endpoints ────────────────────────────────────────────────────────────────

// QR / connection status for this user
app.get("/qr", requireUser, async (req, res) => {
  const s = getSession(req.userId);
  if (s.connected) return res.json({ status: "connected" });

  if (!s.sock) {
    console.log(`[QR-Route] Launching for ${req.userId}`);
    launchSocket(req.userId).catch(console.error);
  }

  // Add a small "retry" window: wait up to 2 seconds for QR to arrive
  let retries = 0;
  while (!s.qr && retries < 4) {
    await sleep(500); 
    retries++;
  }

  if (!s.qr) return res.status(202).json({ status: "waiting" });
  res.json({ status: "pending", qr: s.qr });
});

app.get("/status", requireUser, (req, res) => {
  const s = getSession(req.userId);
  res.json({
    connected: s.connected,
    uptime: process.uptime(),
    user: req.userId,
  });
});

app.post("/send", requireUser, async (req, res) => {
  const s = getSession(req.userId);
  if (!s.connected || !s.sock)
    return res.status(503).json({ error: "not_connected" });

  const { phone, message } = req.body;
  try {
    resetIdleTimer(req.userId);
    const jid = toJID(phone);
    const result = await s.sock.sendMessage(jid, { text: message });
    res.json({ success: true, messageId: result.key.id });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/send-image", requireUser, async (req, res) => {
  const s = getSession(req.userId);
  if (!s.connected || !s.sock)
    return res.status(503).json({ error: "not_connected" });

  const { phone, image_base64, caption } = req.body;
  try {
    resetIdleTimer(req.userId);
    const buf = Buffer.from(image_base64.replace(/^data:image\/\w+;base64,/, ""), "base64");
    await s.sock.sendMessage(toJID(phone), { image: buf, caption: caption || "" });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/send-bulk-media", requireUser, async (req, res) => {
  const s = getSession(req.userId);
  if (!s.connected || !s.sock)
    return res.status(503).json({ error: "not_connected" });

  const { phone, text_message, photos_array, pdfs_array, audio_voice_base64 } = req.body;
  const jid = toJID(phone);

  try {
    resetIdleTimer(req.userId);

    if (text_message?.trim()) {
      await s.sock.sendMessage(jid, { text: text_message });
      await sleep(1500);
    }

    for (const photoB64 of photos_array || []) {
      const buf = Buffer.from(photoB64.replace(/^data:image\/\w+;base64,/, ""), "base64");
      await s.sock.sendMessage(jid, { image: buf });
      await sleep(2000);
    }

    for (let i = 0; i < (pdfs_array || []).length; i++) {
      const buf = Buffer.from(pdfs_array[i].replace(/^data:application\/pdf;base64,/, ""), "base64");
      await s.sock.sendMessage(jid, {
        document: buf,
        mimetype: "application/pdf",
        fileName: `Portfolio_${i + 1}.pdf`,
      });
      await sleep(2000);
    }

    if (audio_voice_base64) {
      const buf = Buffer.from(audio_voice_base64.replace(/^data:audio\/\w+;base64,/, ""), "base64");
      await s.sock.sendMessage(jid, { audio: buf, mimetype: "audio/mp4", ptt: true });
    }

    res.json({ success: true });
  } catch (err) {
    console.error(`[BulkMedia:${req.userId}]`, err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post("/logout", requireUser, async (req, res) => {
  const s = getSession(req.userId);
  try {
    if (s.sock) {
      try { await s.sock.logout(); } catch {}
      s.sock = null;
    }
    s.connected = false;
    s.qr = null;
    if (s.idleTimer) clearTimeout(s.idleTimer);
    await clearAuthForUser(req.userId);
    sessions.delete(req.userId);
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function relaunchExistingSessions() {
  try {
    const result = await pool.query(
      "SELECT DISTINCT user_id FROM wa_auth_state WHERE key = 'creds'"
    )
    for (const row of result.rows) {
      console.log(`[Startup] Relaunching session for ${row.user_id}`)
      launchSocket(row.user_id).catch(console.error)
    }
  } catch (e) {
    console.error('[Startup] Failed to relaunch sessions:', e.message)
  }
}


// Health check (no user needed)
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    activeSessions: sessions.size,
    connectedSessions: Array.from(sessions.values()).filter(s => s.connected).length,
    uptime: process.uptime(),
  });
});

app.listen(PORT, () => {
  console.log(`🚀 Multi-user WA Bridge on port ${PORT}`);
  relaunchExistingSessions(); 
});

// Add this at the bottom of index.js, after app.listen

