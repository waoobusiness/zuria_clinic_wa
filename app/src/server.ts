// app/src/server.ts

import express, { Request, Response } from "express";
import cors from "cors";
import pino from "pino";
import fs from "fs-extra";
import path from "path";
import { LRUCache } from "lru-cache";
import { lookup as mimeLookup } from "mime-types";
import { EventEmitter } from "events";
import QRCode from "qrcode";

// Baileys
import makeWASocket, {
  WASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  DisconnectReason,
  WAMessage,
  AnyMessageContent,
  downloadMediaMessage,
} from "@whiskeysockets/baileys";

const logger = pino({ level: process.env.LOG_LEVEL || "info" });

// ----------- Config

const PORT = Number(process.env.PORT || 3000);

// On prend d'abord SESSIONS_DIR, sinon DATA_DIR, sinon ./sessions
const SESSIONS_DIR =
  process.env.SESSIONS_DIR ||
  process.env.DATA_DIR ||
  path.join(process.cwd(), "sessions");

// Webhook (Make / Supabase / autre)
const WEBHOOK_URL = process.env.WA_WEBHOOK_URL || process.env.WEBHOOK_URL || "";

// URL publique de la gateway (pour mediaUrl)
const PUBLIC_URL = process.env.WA_PUBLIC_URL || process.env.PUBLIC_URL || "";

// Option: stocker raw contact en mémoire (peut coûter cher en RAM)
const STORE_RAW_CONTACT = process.env.WA_STORE_RAW_CONTACT === "1";

// ----------- App

const app = express();
app.use(cors());
app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ extended: true, limit: "25mb" }));

// ----------- Types & Stores

type SessionStatus = "starting" | "qr" | "connecting" | "connected" | "closed";

type ChatSummary = {
  id: string;
  name?: string;
  unreadCount?: number;
  lastMessageTimestamp?: number;
  lastMessagePreview?: string;
  isGroup?: boolean;
};

type ContactSummary = {
  id: string;
  name?: string;
  notify?: string;
  shortName?: string;
  phone?: string | null; // numéro normalisé si dispo (waid, etc.)
  raw?: any; // seulement si WA_STORE_RAW_CONTACT=1
};

type Session = {
  orgId: string;
  sock?: WASocket;
  saveCreds?: () => Promise<void>;
  bus: EventEmitter;
  qr?: string | null;
  status: SessionStatus;
  msgCache: LRUCache<string, WAMessage>;
  chats: Map<string, ChatSummary>;
  contacts: Map<string, ContactSummary>;

  // Anti-storm / anti-OOM
  startPromise?: Promise<Session> | null;
  reconnectTimer?: NodeJS.Timeout | null;
  reconnectFailCount: number;
  lastKnownPhone: string | null; // digits E.164 sans +
  stableSessionId: string | null; // wa_${orgId}_${phoneDigits}
};

const sessions = new Map<string, Session>();

function createEmptySession(orgId: string): Session {
  return {
    orgId,
    bus: new EventEmitter(),
    status: "closed",
    qr: null,
    // baisse un peu la cache pour limiter la RAM en cas de storm
    msgCache: new LRUCache({ max: 200 }),
    chats: new Map(),
    contacts: new Map(),

    startPromise: null,
    reconnectTimer: null,
    reconnectFailCount: 0,
    lastKnownPhone: null,
    stableSessionId: null,
  };
}

function getBus(orgId: string): EventEmitter {
  let s = sessions.get(orgId);
  if (!s) {
    s = createEmptySession(orgId);
    sessions.set(orgId, s);
  }
  return s.bus;
}

function phoneToJid(to: string): string {
  // Si c'est déjà un JID complet (@lid, @s.whatsapp.net, @g.us, etc.), on le garde tel quel
  if (to.includes("@")) return to;
  const digits = to.replace(/[^\d]/g, "").replace(/^00/, "");
  return `${digits}@s.whatsapp.net`;
}

// Helper PN JID -> LID JID (best-effort)
async function getLidForPnJid(sock: any, pnJid: string): Promise<string | null> {
  const lidStore = sock?.signalRepository?.lidMapping;
  if (!lidStore || typeof lidStore.getLIDForPN !== "function") return null;

  try {
    const raw = await Promise.resolve(lidStore.getLIDForPN(pnJid));
    if (!raw) return null;

    const s = String(raw);
    if (!s) return null;

    // selon impl: "124..." ou "124...@lid"
    return s.includes("@") ? s : `${s}@lid`;
  } catch {
    return null;
  }
}

async function bufferFromInput(input?: { url?: string; base64?: string }) {
  if (!input) return undefined;

  if (input.base64) {
    const comma = input.base64.indexOf(",");
    const b64 = comma >= 0 ? input.base64.slice(comma + 1) : input.base64;
    return Buffer.from(b64, "base64");
  }

  if (input.url) {
    const r = await fetch(input.url);
    if (!r.ok) throw new Error(`fetch failed ${r.status}`);
    const arr = await r.arrayBuffer();
    return Buffer.from(arr);
  }

  return undefined;
}

function getSessionOr404(orgId: string, res: Response): Session | null {
  const s = sessions.get(orgId);
  if (!s || !s.sock?.user) {
    res.status(400).json({ ok: false, error: "Session not connected" });
    return null;
  }
  return s;
}

// ----------- Helper pour effacer complètement l’auth disque

async function clearSessionAuth(orgId: string) {
  const authDir = path.join(SESSIONS_DIR, orgId);
  try {
    await fs.remove(authDir);
    logger.info({ orgId, authDir }, "cleared auth directory");
  } catch (err) {
    logger.error({ err, orgId, authDir }, "failed clearing auth directory");
  }
}

// ----------- Anti-storm helpers

function computeBackoffMs(failCount: number) {
  // 30s, 60s, 120s, 240s, 300s max + jitter
  const base = Math.min(Math.pow(2, failCount + 1) * 15_000, 300_000);
  const jitter = Math.floor(Math.random() * 10_000);
  return base + jitter;
}

async function destroySock(sess: Session, reason: string) {
  const sock: any = sess.sock;
  if (!sock) return;

  try {
    sock.ev?.removeAllListeners?.();
  } catch {}

  try {
    sock.ws?.removeAllListeners?.();
  } catch {}

  try {
    sock.end?.(new Error(reason));
  } catch {}

  try {
    sock.ws?.close?.();
  } catch {}

  sess.sock = undefined;
  sess.saveCreds = undefined;
}

// ----------- Helpers divers

// NOUVELLE VERSION : on NE considère pas @lid, @g.us, status, etc. comme des numéros de téléphone
function jidToPhone(jid?: string | null): string | null {
  if (!jid) return null;

  const [local, domain] = jid.split("@");
  if (!local) return null;

  // Cas à ignorer pour le "numéro"
  if (
    domain === "lid" ||
    domain === "g.us" ||
    domain === "newsletter" ||
    local === "status" ||
    local.includes("-")
  ) {
    return null;
  }

  const digits = local.replace(/[^\d]/g, "");
  return digits || null;
}

function getConnectedPhone(sess: Session): string | null {
  const jid = sess.sock?.user?.id; // ex: "41782640976:52@s.whatsapp.net" ou "3615...@lid"
  if (!jid) return null;
  const main = jid.split(":")[0];
  const digits = main.replace(/[^\d]/g, "");
  return digits || null;
}

function buildMediaUrl(orgId: string, msgId: string): string | null {
  if (!PUBLIC_URL) return null;
  const base = PUBLIC_URL.replace(/\/+$/, "");
  return `${base}/wa/media/${encodeURIComponent(orgId)}/${encodeURIComponent(
    msgId
  )}`;
}

// ----------- Helper: extraire le texte d’un message

function extractMessageBody(msg: WAMessage): string | undefined {
  const m: any = msg.message;
  if (!m) return undefined;

  if (m.conversation) return m.conversation as string;
  if (m.extendedTextMessage?.text) return m.extendedTextMessage.text as string;
  if (m.imageMessage?.caption) return m.imageMessage.caption as string;
  if (m.videoMessage?.caption) return m.videoMessage.caption as string;
  if (m.buttonsMessage?.contentText) return m.buttonsMessage.contentText as string;
  if (m.listMessage?.description) return m.listMessage.description as string;

  return undefined;
}

// ----------- Helper: envoyer vers le webhook externe

async function postWebhook(
  event: string,
  orgId: string,
  payload: any
): Promise<void> {
  if (!WEBHOOK_URL) return;

  try {
    await fetch(WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        event,
        orgId,
        ts: Date.now(),
        payload,
      }),
    });
  } catch (err) {
    logger.error({ err, orgId, event }, "webhook error");
  }
}

// ----------- Helper: payload style Z-API pour un message

function buildZapiLikeMessage(msg: WAMessage, sess: Session, orgId: string): any {
  const m: any = msg.message || {};
  const connectedPhone = getConnectedPhone(sess);

  const remoteJid = msg.key.remoteJid as string | undefined;
  const phoneFromJid = jidToPhone(remoteJid || "");
  const isGroup = (remoteJid || "").endsWith("@g.us");
  const fromMe = !!msg.key.fromMe;
  const tsSec = Number(msg.messageTimestamp || 0) || 0;
  const tsMs = tsSec * 1000;

  const contact =
    (remoteJid && sess.contacts.get(remoteJid)) ||
    (phoneFromJid ? sess.contacts.get(`${phoneFromJid}@s.whatsapp.net`) : undefined);

  const contactPhone = contact?.phone ?? phoneFromJid ?? null;

  const displayName =
    contact?.name || contact?.shortName || (msg as any).pushName || contactPhone || remoteJid;

  const base: any = {
    isStatusReply: false,
    chatLid: remoteJid && remoteJid.endsWith("@lid") ? remoteJid : null,
    connectedPhone,
    waitingMessage: false,
    isEdit: false,
    isGroup,
    isNewsletter: false,
    instanceId: orgId,
    messageId: msg.key.id,

    // champs utiles pour wa-webhook
    remoteJid: remoteJid || null,
    chatId: remoteJid || null,
    phone: contactPhone,
    fromMe,
    momment: tsMs,
    status: fromMe ? "SENT" : "RECEIVED",
    chatName: displayName,
    senderPhoto: null,
    senderName: displayName,
    photo: null,
    broadcast: false,
    participantLid: null,
    forwarded: !!m.contextInfo?.isForwarded,
    type: "ReceivedCallback",
    fromApi: false,

    // Nouveau: bloc contact complet
    contact: contact
      ? {
          id: contact.id,
          name: contact.name ?? null,
          shortName: contact.shortName ?? null,
          phone: contact.phone ?? null,
        }
      : {
          id: remoteJid || null,
          name: displayName ?? null,
          shortName: displayName ?? null,
          phone: contactPhone,
        },
  };

  // Texte
  const body = extractMessageBody(msg);
  if (body) base.text = { message: body };

  // Audio
  if (m.audioMessage) {
    base.audio = {
      ptt: !!m.audioMessage.ptt,
      seconds: m.audioMessage.seconds || 0,
      audioUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      mimeType: m.audioMessage.mimetype || "audio/ogg; codecs=opus",
      viewOnce: false,
    };
  }

  // Image
  if (m.imageMessage) {
    base.image = {
      imageUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      thumbnailUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      caption: m.imageMessage.caption || "",
      mimeType: m.imageMessage.mimetype || "image/jpeg",
      viewOnce: !!m.imageMessage.viewOnce,
      width: m.imageMessage.width || 0,
      height: m.imageMessage.height || 0,
    };
  }

  // Video
  if (m.videoMessage) {
    base.video = {
      videoUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      caption: m.videoMessage.caption || "",
      mimeType: m.videoMessage.mimetype || "video/mp4",
      viewOnce: !!m.videoMessage.viewOnce,
      seconds: m.videoMessage.seconds || 0,
    };
  }

  // Document
  if (m.documentMessage) {
    base.document = {
      documentUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      fileName: m.documentMessage.fileName,
      mimeType: m.documentMessage.mimetype,
      fileSize: m.documentMessage.fileLength,
    };
  }

  // Réaction
  if (m.reactionMessage) {
    base.reaction = {
      value:
        m.reactionMessage.text ||
        m.reactionMessage.emoji ||
        m.reactionMessage.reaction ||
        "",
      time: tsMs,
      reactionBy: contactPhone,
      referencedMessage: {
        messageId: m.reactionMessage.key?.id,
        fromMe: m.reactionMessage.key?.fromMe,
        phone: jidToPhone(m.reactionMessage.key?.remoteJid) || null,
        participant: m.reactionMessage.key?.participant || null,
      },
    };
  }

  return base;
}

// Helpers pour normaliser ce qu’on garde en mémoire
function normalizeChat(raw: any): ChatSummary | null {
  if (!raw || !raw.id) return null;
  const id = raw.id as string;
  const isGroup = id.endsWith("@g.us");
  const name = raw.name || raw.subject || raw.pushName || raw.formattedName || id;
  const lastMessageTimestamp = Number(
    raw.conversationTimestamp || raw.lastMessageRecv?.messageTimestamp || raw.t || 0
  );
  const lastMessagePreview =
    raw.lastMessage?.conversation ||
    raw.lastMessage?.message?.conversation ||
    raw.lastMessage?.msg ||
    undefined;
  const unreadCount = raw.unreadCount;

  return { id, name, unreadCount, lastMessageTimestamp, lastMessagePreview, isGroup };
}

function normalizeContact(raw: any): ContactSummary | null {
  if (!raw || !raw.id) return null;
  const id = raw.id as string;
  const name = raw.name || raw.notify || raw.pushName || raw.verifiedName || id;
  const notify = raw.notify;
  const shortName = raw.shortName || raw.name || raw.pushName || raw.verifiedName || name;

  // Tentative best-effort pour récupérer le numéro
  let phone: string | null = null;

  if (typeof raw.waid === "string") {
    const digits = raw.waid.replace(/[^\d]/g, "");
    if (digits) phone = digits;
  }

  if (!phone && typeof raw.phoneNumber === "string") {
    const digits = raw.phoneNumber.replace(/[^\d]/g, "");
    if (digits) phone = digits;
  }

  if (!phone && typeof raw.number === "string") {
    const digits = raw.number.replace(/[^\d]/g, "");
    if (digits) phone = digits;
  }

  if (!phone) phone = jidToPhone(id);

  return { id, name, notify, shortName, phone, raw: STORE_RAW_CONTACT ? raw : undefined };
}

// ----------- Session bootstrap

let cachedBaileysVersion: any = null;
let cachedBaileysVersionAt = 0;

async function getBaileysVersionCached() {
  const now = Date.now();
  if (cachedBaileysVersion && now - cachedBaileysVersionAt < 10 * 60_000) {
    return cachedBaileysVersion;
  }
  const r = await fetchLatestBaileysVersion();
  cachedBaileysVersion = r.version;
  cachedBaileysVersionAt = now;
  return cachedBaileysVersion;
}

async function startSession(orgId: string): Promise<Session> {
  let sess = sessions.get(orgId);

  if (!sess) {
    sess = createEmptySession(orgId);
    sessions.set(orgId, sess);
  }

  // Mutex: si un start est déjà en cours, renvoyer le même
  if (sess.startPromise) return sess.startPromise;

  // Si une socket existe déjà et que la session n'est pas closed, ne pas recréer
  if (sess.sock && ["starting", "qr", "connecting", "connected"].includes(sess.status)) {
    return sess;
  }

  sess.status = "starting";

  sess.startPromise = (async () => {
    // Safety: si une ancienne sock traîne, on la détruit avant de recréer
    if (sess!.sock) await destroySock(sess!, "restart_before_start");

    const authDir = path.join(SESSIONS_DIR, orgId);
    await fs.ensureDir(authDir);

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    const version = await getBaileysVersionCached();

    const sock = makeWASocket({
      version,
      printQRInTerminal: false,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, logger),
      },
      browser: ["Zuria", "Chrome", "1.0.0"],
      logger,
      syncFullHistory: false,
      markOnlineOnConnect: false,
    });

    sess!.sock = sock;
    sess!.saveCreds = saveCreds;
    sess!.status = "connecting";
    sess!.qr = null;

    // Sauvegarde des creds
    sock.ev.on("creds.update", saveCreds);

    // Événements de connexion
    sock.ev.on("connection.update", (u: any) => {
      const { connection, lastDisconnect, qr } = u;

      if (qr) {
        sess!.qr = qr;
        sess!.status = "qr";
        getBus(orgId).emit("status", { type: "qr", qr });
      }

      if (connection === "open") {
        sess!.status = "connected";
        sess!.qr = null;

        // reset backoff + cancel timer
        sess!.reconnectFailCount = 0;
        if (sess!.reconnectTimer) {
          clearTimeout(sess!.reconnectTimer);
          sess!.reconnectTimer = null;
        }

        const phoneDigits = getConnectedPhone(sess!) || null;
        sess!.lastKnownPhone = phoneDigits;
        sess!.stableSessionId = phoneDigits ? `wa_${orgId}_${phoneDigits}` : null;

        getBus(orgId).emit("status", { type: "connected", user: sock.user });
        logger.info({ orgId, phoneDigits }, "WA connected");

        void postWebhook("connection.open", orgId, {
          user: sock.user,
          phone: phoneDigits,
          sessionId: sess!.stableSessionId,
        });
        return;
      }

      if (connection === "close") {
        const code: number = (lastDisconnect as any)?.error?.output?.statusCode ?? 0;

        // Ces codes doivent stopper la reconnexion automatique
        // Important: on évite de clear auth sauf logout/badSession
        const fatalCodes: number[] = [
          DisconnectReason.loggedOut,
          DisconnectReason.badSession,
          DisconnectReason.forbidden,
          DisconnectReason.connectionReplaced,
        ];

        const willReconnect = !fatalCodes.includes(code);

        sess!.status = "closed";
        getBus(orgId).emit("status", { type: "closed", code, willReconnect });

        logger.warn({ orgId, code, willReconnect }, "WA closed");

        void postWebhook("connection.close", orgId, {
          code,
          willReconnect,
          phone: sess!.lastKnownPhone,
          sessionId: sess!.stableSessionId,
          user: sock.user || null,
        });

        // Nettoie la sock pour éviter fuite mémoire
        void destroySock(sess!, `closed_${code}`);

        if (!willReconnect) {
          // clear auth seulement si vrai logout/bad session
          if (code === DisconnectReason.loggedOut || code === DisconnectReason.badSession) {
            clearSessionAuth(orgId).catch(() => {});
          }

          // stop
          return;
        }

        // schedule reconnect avec backoff, et un seul timer max
        if (!sess!.reconnectTimer) {
          const delay = computeBackoffMs(sess!.reconnectFailCount++);
          sess!.reconnectTimer = setTimeout(() => {
            sess!.reconnectTimer = null;
            logger.info({ orgId, code, delay }, "auto-restart WA session (backoff)");
            startSession(orgId).catch((err) =>
              logger.error({ err, orgId }, "failed to restart session")
            );
          }, delay);
        }
      }
    });

    // Historique initial (chats, contacts, messages)
    sock.ev.on("messaging-history.set", (payload: any) => {
      const { chats, contacts, messages, syncType } = payload || {};

      if (Array.isArray(chats)) {
        for (const c of chats) {
          const summary = normalizeChat(c);
          if (summary) sess!.chats.set(summary.id, summary);
        }
      }

      if (Array.isArray(contacts)) {
        for (const c of contacts) {
          const summary = normalizeContact(c);
          if (summary) sess!.contacts.set(summary.id, summary);
        }
      }

      if (Array.isArray(messages)) {
        for (const msg of messages as WAMessage[]) {
          if (msg.key && msg.key.id) sess!.msgCache.set(msg.key.id, msg);
        }
      }

      getBus(orgId).emit("history", {
        type: "set",
        syncType,
        chats: Array.from(sess!.chats.values()),
        contacts: Array.from(sess!.contacts.values()),
      });
    });

    // Chats & contacts live updates
    sock.ev.on("chats.upsert", (up: any) => {
      const arr = Array.isArray(up) ? up : up?.chats || [];
      const updated: ChatSummary[] = [];

      for (const c of arr) {
        const summary = normalizeChat(c);
        if (summary) {
          sess!.chats.set(summary.id, summary);
          updated.push(summary);
        }
      }

      if (updated.length) getBus(orgId).emit("chats", { type: "upsert", chats: updated });
    });

    sock.ev.on("chats.update", (updates: any) => {
      const updated: ChatSummary[] = [];

      for (const u of updates || []) {
        const id = u.id as string;
        const existing = sess!.chats.get(id) || ({ id } as ChatSummary);

        const merged: ChatSummary = {
          ...existing,
          unreadCount: u.unreadCount !== undefined ? u.unreadCount : existing.unreadCount,
          lastMessageTimestamp:
            u.conversationTimestamp !== undefined
              ? Number(u.conversationTimestamp)
              : existing.lastMessageTimestamp,
        };

        if (u.name || u.subject) merged.name = u.name || u.subject;

        sess!.chats.set(id, merged);
        updated.push(merged);
      }

      if (updated.length) getBus(orgId).emit("chats", { type: "update", chats: updated });
    });

    sock.ev.on("contacts.upsert", (up: any) => {
      const arr = Array.isArray(up) ? up : up?.contacts || [];
      const updated: ContactSummary[] = [];

      for (const c of arr) {
        const summary = normalizeContact(c);
        if (summary) {
          sess!.contacts.set(summary.id, summary);
          updated.push(summary);
        }
      }

      if (updated.length) getBus(orgId).emit("contacts", { type: "upsert", contacts: updated });
    });

    sock.ev.on("contacts.update", (updates: any) => {
      const updated: ContactSummary[] = [];

      for (const u of updates || []) {
        const id = u.id as string;
        const existing = sess!.contacts.get(id) || ({ id } as ContactSummary);

        const merged: ContactSummary = {
          ...existing,
          name: u.name || u.notify || existing.name,
          notify: u.notify ?? existing.notify,
          shortName: u.shortName ?? existing.shortName,
          phone:
            existing.phone ??
            (typeof u.waid === "string" ? u.waid.replace(/[^\d]/g, "") : existing.phone),
          raw: existing.raw ?? (STORE_RAW_CONTACT ? u : undefined),
        };

        sess!.contacts.set(id, merged);
        updated.push(merged);
      }

      if (updated.length) getBus(orgId).emit("contacts", { type: "update", contacts: updated });
    });

    // Messages entrants => cache + bus + webhook (INBOUND uniquement)
    sock.ev.on("messages.upsert", (m: any) => {
      const up = m.messages || [];
      for (const msg of up as WAMessage[]) {
        if (msg.key && msg.key.id) sess!.msgCache.set(msg.key.id, msg);

        const messageType = msg.message ? Object.keys(msg.message)[0] : undefined;
        const body = extractMessageBody(msg);

        const remoteJid = msg.key.remoteJid as string | undefined;
        const phoneFromJid = jidToPhone(remoteJid || "");
        const contact =
          (remoteJid && sess!.contacts.get(remoteJid)) ||
          (phoneFromJid ? sess!.contacts.get(`${phoneFromJid}@s.whatsapp.net`) : undefined);
        const contactPhone = contact?.phone ?? phoneFromJid ?? null;
        const contactName =
          contact?.name || contact?.shortName || (msg as any).pushName || contactPhone || remoteJid;

        const simplified = {
          id: msg.key.id,
          from: remoteJid,
          fromMe: msg.key.fromMe,
          pushName: (msg as any).pushName,
          timestamp: (msg.messageTimestamp || 0).toString(),
          messageType,
          body,
          contact: {
            id: remoteJid || null,
            name: contactName || null,
            phone: contactPhone,
          },
        };

        // SSE pour Lovable (UI)
        getBus(orgId).emit("message", { type: "message", message: simplified });

        // Webhook Supabase (INBOUND)
        if (!msg.key.fromMe) {
          const zmsg = buildZapiLikeMessage(msg, sess!, orgId);
          void postWebhook("message.incoming", orgId, { ...simplified, zapi: zmsg });
        }
      }
    });

    sock.ev.on("messages.update", (updates: any) => {
      getBus(orgId).emit("messages.update", updates);
      void postWebhook("messages.update", orgId, updates);
    });

    sock.ev.on("message-receipt.update", (r: any) => {
      getBus(orgId).emit("receipt", r);
      void postWebhook("message-receipt.update", orgId, r);
    });

    return sess!;
  })();

  try {
    return await sess.startPromise;
  } catch (err) {
    // si start échoue, remettre état safe
    sess.status = "closed";
    throw err;
  } finally {
    // libérer le mutex start même si erreur
    sess.startPromise = null;
  }
}

// ----------- SSE (événements temps réel)

app.get("/wa/sse", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  if (!orgId) return res.status(400).end("orgId required");

  req.socket.setTimeout(0);
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const bus = getBus(orgId);

  const send = (event: string, data: any) => {
    res.write(`event: ${event}\n`);
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  const s = sessions.get(orgId);
  send("hello", {
    orgId,
    status: s?.status || "closed",
    hasQR: Boolean(s?.qr),
    connected: Boolean(s?.sock?.user),
    user: s?.sock?.user || null,
  });

  if (s?.qr) {
    const qrSvg = await QRCode.toString(s.qr, { type: "svg" });
    send("qr", { qr: s.qr, svg: qrSvg });
  }

  if (s && (s.chats.size || s.contacts.size)) {
    send("history", {
      type: "set",
      syncType: "initial",
      chats: Array.from(s.chats.values()),
      contacts: Array.from(s.contacts.values()),
    });
  }

  const onStatus = (data: any) => send("status", data);
  const onMessage = (data: any) => send("message", data);
  const onUpdate = (data: any) => send("messages.update", data);
  const onReceipt = (data: any) => send("receipt", data);
  const onHistory = (data: any) => send("history", data);
  const onChats = (data: any) => send("chats", data);
  const onContacts = (data: any) => send("contacts", data);

  bus.on("status", onStatus);
  bus.on("message", onMessage);
  bus.on("messages.update", onUpdate);
  bus.on("receipt", onReceipt);
  bus.on("history", onHistory);
  bus.on("chats", onChats);
  bus.on("contacts", onContacts);

  const interval = setInterval(() => {
    res.write(": keep-alive\n\n");
  }, 25_000);

  req.on("close", () => {
    clearInterval(interval);
    bus.off("status", onStatus);
    bus.off("message", onMessage);
    bus.off("messages.update", onUpdate);
    bus.off("receipt", onReceipt);
    bus.off("history", onHistory);
    bus.off("chats", onChats);
    bus.off("contacts", onContacts);
  });
});

// ----------- Auth / Status

app.post("/wa/login", async (req: Request, res: Response) => {
  const { orgId } = req.body || {};
  if (!orgId) return res.status(400).json({ ok: false, error: "orgId required" });

  try {
    const s = await startSession(String(orgId));
    res.json({
      ok: true,
      status: s.status,
      hasQR: Boolean(s.qr),
      user: s.sock?.user || null,
    });
  } catch (err) {
    logger.error({ err, orgId }, "login error");
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.get("/wa/status", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  if (!orgId) return res.status(400).json({ ok: false, error: "orgId required" });

  const s = sessions.get(orgId);
  res.json({
    ok: true,
    status: s?.status || "closed",
    hasQR: Boolean(s?.qr),
    user: s?.sock?.user || null,
    connected: Boolean(s?.sock?.user),
  });
});

// Endpoint utilisé par l'edge function wa-resolve
app.post("/wa/resolve", async (req: Request, res: Response) => {
  const { orgId, to, peer, phone } = req.body || {};
  const input = String(to || peer || phone || "").trim();

  if (!orgId || !input) {
    return res.status(400).json({
      ok: false,
      error: "orgId and (to|peer|phone) required",
    });
  }

  const org = String(orgId);
  const sess = sessions.get(org);
  const sock: any = sess?.sock;

  const baseJid = phoneToJid(input);

  // si pas connecté: renvoyer JSON propre (pas de HTML)
  if (!sock || !sock.user) {
    return res.json({
      ok: true,
      input,
      connected: false,
      sendJid: baseJid,
      toPn: baseJid.endsWith("@s.whatsapp.net") ? baseJid : null,
      toLid: baseJid.endsWith("@lid") ? baseJid : null,
    });
  }

  try {
    // déjà LID / groupe / etc
    if (!baseJid.endsWith("@s.whatsapp.net")) {
      return res.json({
        ok: true,
        input,
        connected: true,
        sendJid: baseJid,
        toPn: null,
        toLid: baseJid.endsWith("@lid") ? baseJid : null,
      });
    }

    const lidJid = await getLidForPnJid(sock, baseJid);

    return res.json({
      ok: true,
      input,
      connected: true,
      sendJid: lidJid || baseJid,
      toPn: baseJid,
      toLid: lidJid || null,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "resolve_failed",
      detail: String(err),
    });
  }
});

app.get("/wa/qr", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  if (!orgId) return res.status(400).json({ ok: false, error: "orgId required" });

  const s = sessions.get(orgId);
  if (!s?.qr) return res.status(404).json({ ok: false, error: "No pending QR" });

  const svg = await QRCode.toString(s.qr, { type: "svg" });
  res.json({ ok: true, qr: s.qr, svg });
});

// Bootstrap : renvoyer les dernières conversations + contacts
app.get("/wa/bootstrap", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  const limit = Number(req.query.limit || 20);

  if (!orgId) return res.status(400).json({ ok: false, error: "orgId required" });

  const s = sessions.get(orgId);
  if (!s) return res.status(404).json({ ok: false, error: "No session" });

  const chats = Array.from(s.chats.values()).sort(
    (a, b) => (b.lastMessageTimestamp || 0) - (a.lastMessageTimestamp || 0)
  );

  const contacts = Array.from(s.contacts.values());

  res.json({ ok: true, chats: chats.slice(0, limit), contacts });
});

// Avatar à la demande
app.get("/wa/profile-picture", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  const jid = String(req.query.jid || "");
  if (!orgId || !jid) {
    return res.status(400).json({ ok: false, error: "orgId,jid required" });
  }

  const s = getSessionOr404(orgId, res);
  if (!s) return;

  try {
    const url = await s.sock!.profilePictureUrl(jid, "image");
    res.json({ ok: true, url: url || null });
  } catch (err) {
    logger.warn({ err, orgId, jid }, "profile picture error");
    res.json({ ok: true, url: null });
  }
});

app.post("/wa/logout", async (req: Request, res: Response) => {
  const { orgId } = req.body || {};
  if (!orgId) return res.status(400).json({ ok: false, error: "orgId required" });

  const id = String(orgId);
  const s = sessions.get(id);

  try {
    await s?.sock?.logout();
  } catch (e) {
    logger.warn({ e, orgId: id }, "logout error (ignored)");
  }

  if (s?.reconnectTimer) {
    clearTimeout(s.reconnectTimer);
    s.reconnectTimer = null;
  }

  sessions.delete(id);
  await clearSessionAuth(id);

  res.json({ ok: true });
});

// ----------- ENVOI DE MESSAGES (OUTBOUND) + webhook

app.post("/wa/send/text", async (req: Request, res: Response) => {
  const { orgId, to, text, quotedMsgId, mentions } = req.body || {};
  if (!orgId || !to || !text) {
    return res.status(400).json({ ok: false, error: "orgId,to,text required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const jid = phoneToJid(String(to));
    const options: any = {};

    if (quotedMsgId) {
      options.quoted = { key: { id: quotedMsgId, fromMe: false, remoteJid: jid } };
    }

    const content: AnyMessageContent = { text: String(text) };
    if (Array.isArray(mentions) && mentions.length) {
      (content as any).mentions = mentions.map((p: string) => phoneToJid(p));
    }

    const sent = await s.sock!.sendMessage(jid, content, options);
    const key = sent?.key;
    if (!key) throw new Error("sendMessage returned no key");

    void postWebhook("message.outgoing", String(orgId), {
      kind: "text",
      to: jid,
      key,
      body: String(text),
    });

    res.json({ ok: true, key });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/image", async (req: Request, res: Response) => {
  const { orgId, to, caption, image } = req.body || {};
  if (!orgId || !to || !image) {
    return res.status(400).json({ ok: false, error: "orgId,to,image required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const jid = phoneToJid(String(to));
    const buf = await bufferFromInput(image);

    const msg: AnyMessageContent = buf
      ? { image: buf, caption }
      : { image: { url: image.url }, caption };

    const sent = await s.sock!.sendMessage(jid, msg);
    const key = sent?.key;
    if (!key) throw new Error("sendMessage returned no key");

    void postWebhook("message.outgoing", String(orgId), {
      kind: "image",
      to: jid,
      key,
      caption: caption || null,
    });

    res.json({ ok: true, key });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/document", async (req: Request, res: Response) => {
  const { orgId, to, fileName, mimetype, document } = req.body || {};
  if (!orgId || !to || !document) {
    return res.status(400).json({ ok: false, error: "orgId,to,document required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const jid = phoneToJid(String(to));
    const buf = await bufferFromInput(document);

    const msg: AnyMessageContent = buf
      ? { document: buf, fileName: fileName || "file", mimetype }
      : { document: { url: document.url }, fileName: fileName || "file", mimetype };

    const sent = await s.sock!.sendMessage(jid, msg);
    const key = sent?.key;
    if (!key) throw new Error("sendMessage returned no key");

    void postWebhook("message.outgoing", String(orgId), {
      kind: "document",
      to: jid,
      key,
      fileName: fileName || "file",
      mimetype: mimetype || null,
    });

    res.json({ ok: true, key });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/audio", async (req: Request, res: Response) => {
  const { orgId, to, ptt, audio } = req.body || {};
  if (!orgId || !to || !audio) {
    return res.status(400).json({ ok: false, error: "orgId,to,audio required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const jid = phoneToJid(String(to));
    const buf = await bufferFromInput(audio);

    const msg: AnyMessageContent = buf
      ? { audio: buf, ptt: Boolean(ptt) }
      : { audio: { url: audio.url }, ptt: Boolean(ptt) };

    const sent = await s.sock!.sendMessage(jid, msg);
    const key = sent?.key;
    if (!key) throw new Error("sendMessage returned no key");

    void postWebhook("message.outgoing", String(orgId), {
      kind: "audio",
      to: jid,
      key,
      ptt: Boolean(ptt),
    });

    res.json({ ok: true, key });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/buttons", async (req: Request, res: Response) => {
  const { orgId, to, text, footer, buttons } = req.body || {};
  if (!orgId || !to || !text || !Array.isArray(buttons)) {
    return res.status(400).json({ ok: false, error: "orgId,to,text,buttons required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const jid = phoneToJid(String(to));

    const msg: AnyMessageContent = {
      text,
      footer,
      buttons: buttons.map((b: any, i: number) => ({
        buttonId: String(b.id ?? `btn_${i + 1}`),
        buttonText: { displayText: String(b.label ?? b.text ?? `Option ${i + 1}`) },
        type: 1,
      })),
      headerType: 1,
    } as any;

    const sent = await s.sock!.sendMessage(jid, msg);
    const key = sent?.key;
    if (!key) throw new Error("sendMessage returned no key");

    void postWebhook("message.outgoing", String(orgId), {
      kind: "buttons",
      to: jid,
      key,
      text,
    });

    res.json({ ok: true, key });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/list", async (req: Request, res: Response) => {
  const { orgId, to, title, text, footer, buttonText, sections } = req.body || {};
  if (!orgId || !to || !text || !Array.isArray(sections)) {
    return res.status(400).json({ ok: false, error: "orgId,to,text,sections required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const jid = phoneToJid(String(to));

    const msg: AnyMessageContent = {
      text,
      footer,
      title,
      buttonText: buttonText || "Choisir",
      sections: sections.map((sec: any) => ({
        title: String(sec.title || ""),
        rows: (sec.rows || []).map((r: any, i: number) => ({
          rowId: String(r.id ?? `row_${i + 1}`),
          title: String(r.title ?? `Option ${i + 1}`),
          description: r.description ? String(r.description) : undefined,
        })),
      })),
    } as any;

    const sent = await s.sock!.sendMessage(jid, msg);
    const key = sent?.key;
    if (!key) throw new Error("sendMessage returned no key");

    void postWebhook("message.outgoing", String(orgId), {
      kind: "list",
      to: jid,
      key,
      title,
      text,
    });

    res.json({ ok: true, key });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// ----------- Lecture messages récents (et médias)

app.get("/wa/messages/recent", (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  const limit = Number(req.query.limit || 50);

  const s = sessions.get(orgId);
  if (!s) return res.status(404).json({ ok: false, error: "No session" });

  const out: any[] = [];

  s.msgCache.forEach((msg, id) => {
    const body = extractMessageBody(msg);
    out.push({
      id,
      from: msg.key.remoteJid,
      fromMe: msg.key.fromMe,
      timestamp: (msg.messageTimestamp || 0).toString(),
      type: msg.message ? Object.keys(msg.message)[0] : undefined,
      body,
    });
  });

  out.sort((a, b) => Number(b.timestamp) - Number(a.timestamp));
  res.json({ ok: true, messages: out.slice(0, limit) });
});

app.post("/wa/media/download", async (req: Request, res: Response) => {
  const { orgId, msgId } = req.body || {};
  if (!orgId || !msgId) {
    return res.status(400).json({ ok: false, error: "orgId,msgId required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  const msg = s.msgCache.get(String(msgId));
  if (!msg) return res.status(404).json({ ok: false, error: "Message not in cache" });

  try {
    const buffer = await downloadMediaMessage(
      msg,
      "buffer",
      {},
      { logger, reuploadRequest: s.sock!.updateMediaMessage }
    );

    const m =
      (msg.message as any)?.imageMessage?.mimetype ||
      (msg.message as any)?.videoMessage?.mimetype ||
      (msg.message as any)?.documentMessage?.mimetype ||
      (msg.message as any)?.audioMessage?.mimetype ||
      mimeLookup("bin") ||
      "application/octet-stream";

    const base64 = (buffer as Buffer).toString("base64");

    res.json({
      ok: true,
      mimetype: m,
      base64: `data:${m};base64,${base64}`,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// GET direct pour media (pour audioUrl / imageUrl style Z-API)
app.get("/wa/media/:orgId/:msgId", async (req: Request, res: Response) => {
  const { orgId, msgId } = req.params;
  if (!orgId || !msgId) {
    return res.status(400).json({ ok: false, error: "orgId,msgId required" });
  }

  const s = getSessionOr404(orgId, res);
  if (!s) return;

  const msg = s.msgCache.get(String(msgId));
  if (!msg) return res.status(404).json({ ok: false, error: "Message not in cache" });

  try {
    const buffer = await downloadMediaMessage(
      msg,
      "buffer",
      {},
      { logger, reuploadRequest: s.sock!.updateMediaMessage }
    );

    const m =
      (msg.message as any)?.imageMessage?.mimetype ||
      (msg.message as any)?.videoMessage?.mimetype ||
      (msg.message as any)?.documentMessage?.mimetype ||
      (msg.message as any)?.audioMessage?.mimetype ||
      mimeLookup("bin") ||
      "application/octet-stream";

    res.setHeader("Content-Type", m as string);
    res.send(buffer);
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// ----------- Health

app.get("/health", (_req, res) =>
  res.json({ ok: true, service: "zuria-baileys", ts: Date.now() })
);

// ----------- Boot

async function main() {
  await fs.ensureDir(SESSIONS_DIR);
  app.listen(PORT, () => {
    logger.info(`HTTP listening on :${PORT}`);
  });
}

main().catch((e) => {
  logger.error(e);
  process.exit(1);
});
