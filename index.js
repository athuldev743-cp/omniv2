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

const sessions = new Map();

// ─────────────────────────────────────────────────────────────────────────────
// DATABASE STATUS SYNC
// ─────────────────────────────────────────────────────────────────────────────

async function updateDatabaseStatus(userId, status) {
  try {
    if (!process.env.BACKEND_API_URL) {
      console.warn(
        "[Sync] BACKEND_API_URL is not configured. Skipping status update."
      );
      return;
    }

    const response = await fetch(
      `${process.env.BACKEND_API_URL}/profile/update-status`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          "X-User-Email": userId,
        },
        body: JSON.stringify({
          whatsapp_status: status,
        }),
      }
    );

    if (!response.ok) {
      console.error(
        `[Sync] Backend returned ${response.status} while updating status for ${userId}`
      );
    }
  } catch (err) {
    console.error(
      `[Sync] Failed to update DB for ${userId}:`,
      err.message
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// AUTH STATE
// PostgreSQL-backed Baileys authentication
// ─────────────────────────────────────────────────────────────────────────────

async function useAuthState(userId) {
  const ns = userId.toLowerCase().trim();

  async function readData(key) {
    try {
      const result = await pool.query(
        `
        SELECT value
        FROM wa_auth_state
        WHERE user_id = $1
          AND key = $2
        `,
        [ns, key]
      );

      if (result.rows.length === 0) {
        return null;
      }

      const raw = result.rows[0].value;

      const str =
        typeof raw === "string" ? raw : JSON.stringify(raw);

      return JSON.parse(str, BufferJSON.reviver);
    } catch (err) {
      console.error(
        `[Auth:${ns}] Read error [${key}]:`,
        err.message
      );

      return null;
    }
  }

  async function writeData(key, value) {
    try {
      const json = JSON.stringify(value, BufferJSON.replacer);

      await pool.query(
        `
        INSERT INTO wa_auth_state
          (user_id, key, value, updated_at)
        VALUES
          ($1, $2, $3::jsonb, NOW())

        ON CONFLICT (user_id, key)
        DO UPDATE SET
          value = $3::jsonb,
          updated_at = NOW()
        `,
        [ns, key, json]
      );
    } catch (err) {
      console.error(
        `[Auth:${ns}] Write error [${key}]:`,
        err.message
      );
    }
  }

  async function deleteData(key) {
    try {
      await pool.query(
        `
        DELETE FROM wa_auth_state
        WHERE user_id = $1
          AND key = $2
        `,
        [ns, key]
      );
    } catch (err) {
      console.error(
        `[Auth:${ns}] Delete error:`,
        err.message
      );
    }
  }

  const creds =
    (await readData("creds")) || initAuthCreds();

  console.log(
    `[Auth:${ns}] Creds loaded: registered=${!!creds.registrationId}, noiseKey=${!!creds.noiseKey}`
  );

  return {
    state: {
      creds,

      keys: {
        get: async (type, ids) => {
          const data = {};

          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);

              if (
                type === "app-state-sync-key" &&
                value
              ) {
                value =
                  proto.Message.AppStateSyncKeyData.fromObject(
                    value
                  );
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

              if (value) {
                await writeData(key, value);
              } else {
                await deleteData(key);
              }
            }
          }
        },
      },
    },

    saveCreds: async () => {
      await writeData("creds", creds);
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// CLEAR AUTH
// ─────────────────────────────────────────────────────────────────────────────

async function clearAuthForUser(userId) {
  const ns = userId.toLowerCase().trim();

  try {
    await pool.query(
      `
      DELETE FROM wa_auth_state
      WHERE user_id = $1
      `,
      [ns]
    );

    console.log(
      `[Auth:${ns}] Auth cleared from DB`
    );
  } catch (err) {
    console.error(
      `[Auth:${ns}] Clear error:`,
      err.message
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SESSION HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function getSession(userId) {
  if (!sessions.has(userId)) {
    sessions.set(userId, {
      sock: null,
      qr: null,
      connected: false,
      reconnectAttempts: 0,
      idleTimer: null,
      launching: false,
    });
  }

  return sessions.get(userId);
}

// ─────────────────────────────────────────────────────────────────────────────
// IDLE TIMER
// ─────────────────────────────────────────────────────────────────────────────

function resetIdleTimer(userId) {
  const s = getSession(userId);

  if (s.idleTimer) {
    clearTimeout(s.idleTimer);
  }

  s.idleTimer = setTimeout(() => {
    if (!s.connected) {
      return;
    }

    console.log(
      `[Session:${userId}] Idle timeout — closing socket to free memory`
    );

    try {
      s.sock?.end();
    } catch (err) {
      console.error(
        `[Session:${userId}] Socket close error:`,
        err.message
      );
    }

    s.sock = null;
    s.connected = false;
    s.qr = null;
  }, IDLE_TIMEOUT_MS);
}

// ─────────────────────────────────────────────────────────────────────────────
// LAUNCH SOCKET
// ─────────────────────────────────────────────────────────────────────────────

async function launchSocket(userId) {
  const s = getSession(userId);

  // Prevent duplicate socket launches
  if (s.sock || s.launching) {
    return;
  }

  s.launching = true;

  try {
    const { state, saveCreds } =
      await useAuthState(userId);

    const { version } =
      await fetchLatestBaileysVersion();

    console.log(
      `[Socket:${userId}] Initialising Baileys version ${version.join(".")}...`
    );

    const sock = makeWASocket({
      version,

      logger: P({
        level: "silent",
      }),

      auth: state,

      printQRInTerminal: false,

      browser: [
        "Chrome (Linux)",
        "Chrome",
        "126.0.6478.114",
      ],

      connectTimeoutMs: 60000,

      qrTimeout: 60000,
    });

    s.sock = sock;
    s.launching = false;

    // ─────────────────────────────────────────────────────────────────────
    // CONNECTION EVENTS
    // ─────────────────────────────────────────────────────────────────────

    sock.ev.on(
      "connection.update",
      async (update) => {
        const {
          connection,
          lastDisconnect,
          qr,
        } = update;

        // ───────────────────────────────────────────────────────────────
        // QR CODE
        // ───────────────────────────────────────────────────────────────

        if (qr) {
          if (s.qr !== qr) {
            console.log(
              `[Socket:${userId}] New QR received`
            );

            try {
              const dataUrl =
                await QRCode.toDataURL(qr);

              s.qr = dataUrl.split(",")[1];
            } catch (err) {
              console.error(
                `[Socket:${userId}] QR generation error:`,
                err.message
              );
            }
          }
        }

        // ───────────────────────────────────────────────────────────────
        // CONNECTED
        // ───────────────────────────────────────────────────────────────

        if (connection === "open") {
          s.connected = true;
          s.qr = null;
          s.reconnectAttempts = 0;
          s.launching = false;

          resetIdleTimer(userId);

          console.log(
            `✅ [Socket:${userId}] Connected`
          );

          await updateDatabaseStatus(
            userId,
            "connected"
          );
        }

        // ───────────────────────────────────────────────────────────────
        // DISCONNECTED
        // ───────────────────────────────────────────────────────────────

       if (connection === "close") {
  s.connected = false;
  s.qr = null;
  s.sock = null;
  s.launching = false;

  const code = new Boom(lastDisconnect?.error)?.output?.statusCode;
  console.log(`[Socket:${userId}] Closed. Code: ${code}`);

  // 428 or Connection Closed -> Re-trigger socket launch directly
  if (code === 428 || code === DisconnectReason.connectionClosed) {
    console.log(`[Socket:${userId}] Precondition/Connection close (428) — restarting socket...`);
    setTimeout(() => {
      launchSocket(userId).catch(console.error);
    }, 2000);
    return;
  }

          // ───────────────────────────────────────────────────────────
          // LOGGED OUT
          // ───────────────────────────────────────────────────────────

          if (
            code === DisconnectReason.loggedOut
          ) {
            console.log(
              `[Socket:${userId}] Logged out — clearing authentication`
            );

            await clearAuthForUser(userId);

            s.reconnectAttempts = 0;

            setTimeout(() => {
              launchSocket(userId).catch(
                console.error
              );
            }, 5000);

            return;
          }

          // ───────────────────────────────────────────────────────────
          // CONNECTION REPLACED / 440
          // ───────────────────────────────────────────────────────────

          if (code === 440) {
            console.log(
              `[Socket:${userId}] Connection replaced — retrying in 15s`
            );

            setTimeout(() => {
              launchSocket(userId).catch(
                console.error
              );
            }, 15000);

            return;
          }

          // ───────────────────────────────────────────────────────────
          // STREAM ERROR / 515
          // ───────────────────────────────────────────────────────────

          if (code === 515) {
            console.log(
              `[Socket:${userId}] Stream error — retrying in 2s`
            );

            setTimeout(() => {
              launchSocket(userId).catch(
                console.error
              );
            }, 2000);

            return;
          }

          // ───────────────────────────────────────────────────────────
          // NORMAL RECONNECT
          // ───────────────────────────────────────────────────────────

          s.reconnectAttempts++;

          if (
            s.reconnectAttempts >=
            MAX_RECONNECTS
          ) {
            console.log(
              `[Socket:${userId}] Maximum reconnect attempts reached — waiting 60s`
            );

            s.reconnectAttempts = 0;

            setTimeout(() => {
              launchSocket(userId).catch(
                console.error
              );
            }, 60000);

            return;
          }

          const delay = Math.min(
            s.reconnectAttempts * 5000,
            30000
          );

          console.log(
            `[Socket:${userId}] Reconnecting in ${delay}ms`
          );

          setTimeout(() => {
            launchSocket(userId).catch(
              console.error
            );
          }, delay);
        }
      }
    );

    // ───────────────────────────────────────────────────────────────────
    // SAVE CREDENTIALS
    // ───────────────────────────────────────────────────────────────────

    sock.ev.on(
      "creds.update",
      saveCreds
    );

  } catch (err) {
    s.launching = false;
    s.sock = null;

    console.error(
      `[Socket:${userId}] Launch error:`,
      err
    );

    s.reconnectAttempts++;

    const delay = Math.min(
      s.reconnectAttempts * 5000,
      30000
    );

    setTimeout(() => {
      launchSocket(userId).catch(
        console.error
      );
    }, delay);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// USER MIDDLEWARE
// ─────────────────────────────────────────────────────────────────────────────

function requireUser(req, res, next) {
  const userId =
    req.headers["x-user-email"] ||
    req.query.user;

  if (!userId) {
    return res.status(400).json({
      error: "x-user-email header required",
    });
  }

  req.userId = String(userId)
    .toLowerCase()
    .trim();

  next();
}

// ─────────────────────────────────────────────────────────────────────────────
// PHONE → WHATSAPP JID
// ─────────────────────────────────────────────────────────────────────────────

function toJID(phone) {
  let p = String(phone).replace(/\D/g, "");

  if (p.startsWith("00")) {
    p = p.slice(2);
  } else if (p.startsWith("0")) {
    p = p.slice(1);
  }

  // India number
  if (
    p.length === 10 &&
    ["6", "7", "8", "9"].includes(p[0])
  ) {
    p = "91" + p;
  }

  return `${p}@s.whatsapp.net`;
}

// ─────────────────────────────────────────────────────────────────────────────
// SLEEP
// ─────────────────────────────────────────────────────────────────────────────

function sleep(ms) {
  return new Promise((resolve) =>
    setTimeout(resolve, ms)
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// QR ENDPOINT
// ─────────────────────────────────────────────────────────────────────────────

app.get(
  "/qr",
  requireUser,
  async (req, res) => {
    const s = getSession(req.userId);

    // Already connected
    if (s.connected) {
      return res.json({
        status: "connected",
      });
    }

    // Start socket
    if (!s.sock && !s.launching) {
      console.log(
        `[QR-Route] Launching for ${req.userId}`
      );

      launchSocket(req.userId).catch(
        console.error
      );
    }

    // Wait up to 2 seconds for QR
    let retries = 0;

    while (!s.qr && retries < 4) {
      await sleep(500);
      retries++;
    }

    if (!s.qr) {
      return res.status(202).json({
        status: "waiting",
      });
    }

    return res.json({
      status: "pending",
      qr: s.qr,
    });
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// STATUS
// ─────────────────────────────────────────────────────────────────────────────

app.get(
  "/status",
  requireUser,
  (req, res) => {
    const s = getSession(req.userId);

    res.json({
      connected: s.connected,
      hasSocket: !!s.sock,
      hasQR: !!s.qr,
      uptime: process.uptime(),
      user: req.userId,
    });
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// SEND TEXT
// ─────────────────────────────────────────────────────────────────────────────

app.post(
  "/send",
  requireUser,
  async (req, res) => {
    const s = getSession(req.userId);

    if (!s.connected || !s.sock) {
      return res.status(503).json({
        error: "not_connected",
      });
    }

    const {
      phone,
      message,
    } = req.body;

    if (!phone) {
      return res.status(400).json({
        error: "phone_required",
      });
    }

    if (!message) {
      return res.status(400).json({
        error: "message_required",
      });
    }

    try {
      resetIdleTimer(req.userId);

      const jid = toJID(phone);

      const result =
        await s.sock.sendMessage(
          jid,
          {
            text: message,
          }
        );

      return res.json({
        success: true,
        messageId: result.key.id,
      });
    } catch (err) {
      console.error(
        `[Send:${req.userId}]`,
        err.message
      );

      return res.status(500).json({
        error: err.message,
      });
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// SEND IMAGE
// ─────────────────────────────────────────────────────────────────────────────

app.post(
  "/send-image",
  requireUser,
  async (req, res) => {
    const s = getSession(req.userId);

    if (!s.connected || !s.sock) {
      return res.status(503).json({
        error: "not_connected",
      });
    }

    const {
      phone,
      image_base64,
      caption,
    } = req.body;

    if (!phone) {
      return res.status(400).json({
        error: "phone_required",
      });
    }

    if (!image_base64) {
      return res.status(400).json({
        error: "image_base64_required",
      });
    }

    try {
      resetIdleTimer(req.userId);

      const cleanBase64 =
        image_base64.replace(
          /^data:image\/\w+;base64,/,
          ""
        );

      const buffer = Buffer.from(
        cleanBase64,
        "base64"
      );

      await s.sock.sendMessage(
        toJID(phone),
        {
          image: buffer,
          caption: caption || "",
        }
      );

      return res.json({
        success: true,
      });
    } catch (err) {
      console.error(
        `[SendImage:${req.userId}]`,
        err.message
      );

      return res.status(500).json({
        success: false,
        error: err.message,
      });
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// SEND BULK MEDIA
// ─────────────────────────────────────────────────────────────────────────────

app.post(
  "/send-bulk-media",
  requireUser,
  async (req, res) => {
    const s = getSession(req.userId);

    if (!s.connected || !s.sock) {
      return res.status(503).json({
        error: "not_connected",
      });
    }

    const {
      phone,
      text_message,
      photos_array,
      pdfs_array,
      audio_voice_base64,
    } = req.body;

    if (!phone) {
      return res.status(400).json({
        error: "phone_required",
      });
    }

    const jid = toJID(phone);

    try {
      resetIdleTimer(req.userId);

      // Text
      if (text_message?.trim()) {
        await s.sock.sendMessage(
          jid,
          {
            text: text_message,
          }
        );

        await sleep(1500);
      }

      // Photos
      for (
        const photoB64 of photos_array || []
      ) {
        const cleanBase64 =
          photoB64.replace(
            /^data:image\/\w+;base64,/,
            ""
          );

        const buffer = Buffer.from(
          cleanBase64,
          "base64"
        );

        await s.sock.sendMessage(
          jid,
          {
            image: buffer,
          }
        );

        await sleep(2000);
      }

      // PDFs
      for (
        let i = 0;
        i < (pdfs_array || []).length;
        i++
      ) {
        const cleanBase64 =
          pdfs_array[i].replace(
            /^data:application\/pdf;base64,/,
            ""
          );

        const buffer = Buffer.from(
          cleanBase64,
          "base64"
        );

        await s.sock.sendMessage(
          jid,
          {
            document: buffer,
            mimetype: "application/pdf",
            fileName: `Portfolio_${i + 1}.pdf`,
          }
        );

        await sleep(2000);
      }

      // Audio
      if (audio_voice_base64) {
        const cleanBase64 =
          audio_voice_base64.replace(
            /^data:audio\/\w+;base64,/,
            ""
          );

        const buffer = Buffer.from(
          cleanBase64,
          "base64"
        );

        await s.sock.sendMessage(
          jid,
          {
            audio: buffer,
            mimetype: "audio/mp4",
            ptt: true,
          }
        );
      }

      return res.json({
        success: true,
      });
    } catch (err) {
      console.error(
        `[BulkMedia:${req.userId}]`,
        err.message
      );

      return res.status(500).json({
        success: false,
        error: err.message,
      });
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// LOGOUT
// ─────────────────────────────────────────────────────────────────────────────

app.post(
  "/logout",
  requireUser,
  async (req, res) => {
    const s = getSession(req.userId);

    try {
      if (s.idleTimer) {
        clearTimeout(s.idleTimer);
        s.idleTimer = null;
      }

      if (s.sock) {
        try {
          await s.sock.logout();
        } catch (err) {
          console.warn(
            `[Logout:${req.userId}] Socket logout warning:`,
            err.message
          );
        }

        s.sock = null;
      }

      s.connected = false;
      s.qr = null;
      s.launching = false;

      await clearAuthForUser(
        req.userId
      );

      sessions.delete(req.userId);

      await updateDatabaseStatus(
        req.userId,
        "disconnected"
      );

      return res.json({
        success: true,
      });
    } catch (err) {
      console.error(
        `[Logout:${req.userId}]`,
        err.message
      );

      return res.status(500).json({
        error: err.message,
      });
    }
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// RELAUNCH EXISTING SESSIONS
// ─────────────────────────────────────────────────────────────────────────────

async function relaunchExistingSessions() {
  try {
    const result = await pool.query(
      `
      SELECT DISTINCT user_id
      FROM wa_auth_state
      WHERE key = 'creds'
      `
    );

    console.log(
      `[Startup] Found ${result.rows.length} existing WhatsApp session(s)`
    );

    for (const row of result.rows) {
      const userId = row.user_id;

      console.log(
        `[Startup] Relaunching session for ${userId}`
      );

      launchSocket(userId).catch(
        (err) => {
          console.error(
            `[Startup:${userId}]`,
            err.message
          );
        }
      );
    }
  } catch (err) {
    console.error(
      "[Startup] Failed to relaunch sessions:",
      err.message
    );
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// HEALTH CHECK
// ─────────────────────────────────────────────────────────────────────────────

app.get(
  "/health",
  (req, res) => {
    res.json({
      ok: true,
      activeSessions: sessions.size,
      connectedSessions:
        Array.from(sessions.values())
          .filter(
            (session) =>
              session.connected
          ).length,
      uptime: process.uptime(),
    });
  }
);

// ─────────────────────────────────────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(
    `🚀 Multi-user WA Bridge running on port ${PORT}`
  );

  // Restore all previously authenticated users
  relaunchExistingSessions().catch(
    console.error
  );
});
