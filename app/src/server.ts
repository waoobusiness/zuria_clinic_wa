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

const SESSIONS_DIR =
  process.env.SESSIONS_DIR ||
  process.env.DATA_DIR ||
  path.join(process.cwd(), "sessions");

const WEBHOOK_URL = process.env.WA_WEBHOOK_URL || process.env.WEBHOOK_URL || "";
const PUBLIC_URL = process.env.WA_PUBLIC_URL || process.env.PUBLIC_URL || "";

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
  phone?: string | null;
  raw?: any;
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

  reconnectTimer?: NodeJS.Timeout | null;
  reconnectAttempts: number;
};

const sessions = new Map<string, Session>();

// Anti double-start par orgId
const startLocks = new Map<string, Promise<Session>>();

// Cache Baileys version (évite refetch à chaque reconnect)
let cachedVersionPromise: Promise<{ version: any }> | null = null;
async function getBaileysVersion(): Promise<any> {
  if (!cachedVersionPromise) {
    cachedVersionPromise = fetchLatestBaileysVersion().catch((err) => {
      logger.warn({ err }, "fetchLatestBaileysVersion failed, using undefined version");
      return { version: undefined };
    });
  }
  const { version } = await cachedVersionPromise;
  return version;
}

function createEmptySession(orgId: string): Session {
  return {
    orgId,
    bus: new EventEmitter(),
    status: "closed",
    qr: null,
    msgCache: new LRUCache({ max: 1000 }),
    chats: new Map(),
    contacts: new Map(),
    reconnectTimer: null,
    reconnectAttempts: 0,
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
  if (to.includes("@")) return to;
  const digits = to.replace(/[^\d]/g, "").replace(/^00/, "");
  return `${digits}@s.whatsapp.net`;
}

async function getLidForPnJid(sock: any, pnJid: string): Promise<string | null> {
  const lidStore = sock?.signalRepository?.lidMapping;
  if (!lidStore || typeof lidStore.getLIDForPN !== "function") return null;

  try {
    const raw = await Promise.resolve(lidStore.getLIDForPN(pnJid));
    if (!raw) return null;

    const s = String(raw);
    if (!s) return null;

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

async function clearSessionAuth(orgId: string) {
  const authDir = path.join(SESSIONS_DIR, orgId);
  try {
    await fs.remove(authDir);
    logger.info({ orgId, authDir }, "cleared auth directory");
  } catch (err) {
    logger.error({ err, orgId, authDir }, "failed clearing auth directory");
  }
}

function jidToPhone(jid?: string | null): string | null {
  if (!jid) return null;

  const [local, domain] = jid.split("@");
  if (!local) return null;

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
  const jid = sess.sock?.user?.id;
  if (!jid) return null;
  const main = jid.split(":")[0];
  const digits = main.replace(/[^\d]/g, "");
  return digits || null;
}

function buildMediaUrl(orgId: string, msgId: string): string | null {
  if (!PUBLIC_URL) return null;
  const base = PUBLIC_URL.replace(/\/+$/, "");
  return `${base}/wa/media/${encodeURIComponent(orgId)}/${encodeURIComponent(msgId)}`;
}

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

async function postWebhook(event: string, orgId: string, payload: any): Promise<void> {
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

  const body = extractMessageBody(msg);
  if (body) base.text = { message: body };

  if (m.audioMessage) {
    base.audio = {
      ptt: !!m.audioMessage.ptt,
      seconds: m.audioMessage.seconds || 0,
      audioUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      mimeType: m.audioMessage.mimetype || "audio/ogg; codecs=opus",
      viewOnce: false,
    };
  }

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

  if (m.videoMessage) {
    base.video = {
      videoUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      caption: m.videoMessage.caption || "",
      mimeType: m.videoMessage.mimetype || "video/mp4",
      viewOnce: !!m.videoMessage.viewOnce,
      seconds: m.videoMessage.seconds || 0,
    };
  }

  if (m.documentMessage) {
    base.document = {
      documentUrl: msg.key.id && PUBLIC_URL ? buildMediaUrl(orgId, msg.key.id) : null,
      fileName: m.documentMessage.fileName,
      mimeType: m.documentMessage.mimetype,
      fileSize: m.documentMessage.fileLength,
    };
  }

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

  return { id, name, notify, shortName, phone, raw };
}

async function getQrSvg(qr: string): Promise<string> {
  return QRCode.toString(qr, { type: "svg" });
}

function scheduleReconnect(sess: Session, orgId: string, code: number) {
  if (sess.reconnectTimer) return;

  const attempt = Math.min(sess.reconnectAttempts, 6);
  const base = Math.min(1000 * Math.pow(2, attempt), 30_000);
  const jitter = Math.floor(Math.random() * 500);

  const delay = base + jitter;
  sess.reconnectTimer = setTimeout(() => {
    sess.reconnectTimer = null;
    sess.reconnectAttempts += 1;

    logger.info({ orgId, code, delay, attempt: sess.reconnectAttempts }, "auto-restart WA session");
    startSession(orgId).catch((err) => logger.error({ err, orgId }, "failed to restart session"));
  }, delay);
}

async function startSession(orgId: string): Promise<Session> {
  const existing = sessions.get(orgId);
  if (existing?.sock && existing.status === "connected") return existing;

  const locked = startLocks.get(orgId);
  if (locked) return locked;

  const promise = (async () => {
    let sess = sessions.get(orgId);
    if (!sess) {
      sess = createEmptySession(orgId);
      sessions.set(orgId, sess);
    }

    sess.status = "starting";

    const authDir = path.join(SESSIONS_DIR, orgId);
    await fs.ensureDir(authDir);

    const { state, saveCreds } = await useMultiFileAuthState(authDir);
    const version = await getBaileysVersion();

    // Cleanup old socket listeners if any
    if (sess.sock) {
      try {
        (sess.sock as any).ev?.removeAllListeners?.();
      } catch {}
      try {
        (sess.sock as any).end?.(new Error("restart"));
      } catch {}
    }

    const sock = makeWASocket({
      version,
      printQRInTerminal: false,
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, logger),
      },
      browser: ["Zuria", "Chrome", "1.0.0"],
      logger,
      syncFullHistory: true,
      markOnlineOnConnect: false,
    });

    sess.sock = sock;
    sess.saveCreds = saveCreds;
    sess.status = "connecting";
    sess.qr = null;

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", (u: any) => {
      const { connection, lastDisconnect, qr } = u;

      if (qr) {
        sess!.qr = qr;
        sess!.status = "qr";

        getBus(orgId).emit("status", { type: "qr", qr });

        // Optionnel: webhook QR si tu veux
        // void postWebhook("connection.qr", orgId, { qr });

        return;
      }

      if (connection === "open") {
        sess!.status = "connected";
        sess!.qr = null;
        sess!.reconnectAttempts = 0;
        if (sess!.reconnectTimer) {
          clearTimeout(sess!.reconnectTimer);
          sess!.reconnectTimer = null;
        }

        getBus(orgId).emit("status", { type: "connected", user: sock.user });
        logger.info({ orgId }, "WA connected");

        void postWebhook("connection.open", orgId, {
          user: sock.user,
          phone: getConnectedPhone(sess!),
        });
        return;
      }

      if (connection === "close") {
        const code: number = (lastDisconnect as any)?.error?.output?.statusCode ?? 0;

        const fatalCodes: number[] = [
          DisconnectReason.loggedOut,
          DisconnectReason.forbidden,
          DisconnectReason.badSession,
          DisconnectReason.connectionReplaced,
        ];

        const willReconnect = !fatalCodes.includes(code);

        sess!.status = "closed";
        getBus(orgId).emit("status", { type: "closed", code, willReconnect });

        logger.warn({ orgId, code, willReconnect }, "WA closed");

        // Critique: envoyer phone/user pour cibler wa_numbers côté Supabase
        void postWebhook("connection.close", orgId, {
          code,
          willReconnect,
          user: sock.user,
          phone: getConnectedPhone(sess!),
        });

        if (!willReconnect) {
          sessions.delete(orgId);
          clearSessionAuth(orgId).catch(() => {});
        } else {
          scheduleReconnect(sess!, orgId, code);
        }
      }
    });

    sock.ev.on("messaging-history.set", async (payload: any) => {
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

      // Forward ALL history messages to webhook → Supabase (syncType 2=initial, 3=ON_DEMAND)
      // Supabase webhook handles deduplication via message_id unique constraint
      const ninetyDaysAgo = Math.floor(Date.now() / 1000) - 90 * 24 * 60 * 60;
      logger.info({ orgId, syncType, msgCount: Array.isArray(messages) ? messages.length : 0 }, "messaging-history.set received");
      if (Array.isArray(messages) && messages.length) {
        for (const msg of messages as WAMessage[]) {
          if (!msg.key?.id) continue;
          const remoteJid = msg.key.remoteJid as string | undefined;
          if (!remoteJid) continue;
          // Skip status and broadcast
          if (remoteJid === "status@broadcast" || remoteJid.includes("newsletter")) continue;
          // Only sync messages from last 90 days
          const ninetyDaysAgo = Math.floor(Date.now() / 1000) - 90 * 24 * 60 * 60;
          if (Number(msg.messageTimestamp || 0) < ninetyDaysAgo) continue;
          // Only sync messages from last 90 days to avoid overwhelming Supabase
          if (Number(msg.messageTimestamp || 0) < ninetyDaysAgo) continue;
          const phone = jidToPhone(remoteJid);
          const simplified = {
            id: msg.key.id,
            from: remoteJid,
            fromMe: !!msg.key.fromMe,
            pushName: (msg as any).pushName,
            timestamp: (msg.messageTimestamp || 0).toString(),
            messageType: msg.message ? Object.keys(msg.message)[0] : undefined,
            body: extractMessageBody(msg),
            phone,
            isGroup: remoteJid.endsWith("@g.us"),
            isHistory: true,
            syncType,
          };
          const zmsg = buildZapiLikeMessage(msg, sess!, orgId);
          void postWebhook("message.incoming", orgId, { ...simplified, zapi: zmsg });
        }
      }

      getBus(orgId).emit("history", {
        type: "set",
        syncType,
        chats: Array.from(sess!.chats.values()),
        contacts: Array.from(sess!.contacts.values()),
      });
    });

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
          raw: existing.raw ?? u,
        };

        sess!.contacts.set(id, merged);
        updated.push(merged);
      }

      if (updated.length) getBus(orgId).emit("contacts", { type: "update", contacts: updated });
    });

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

        getBus(orgId).emit("message", { type: "message", message: simplified });

        if (!msg.key.fromMe) {
          const zmsg = buildZapiLikeMessage(msg, sess!, orgId);
          void postWebhook("message.incoming", orgId, { ...simplified, zapi: zmsg });
        } else {
        // Messages envoyés depuis le téléphone directement (pas via l'API CRM)
        const zmsg = buildZapiLikeMessage(msg, sess!, orgId);
        void postWebhook("message.outgoing", orgId, {
          ...simplified,
          kind: messageType === "audioMessage" ? "audio"
              : messageType === "imageMessage"  ? "image"
              : messageType === "videoMessage"  ? "video"
              : messageType === "documentMessage" ? "document" : "text",
          to: remoteJid,
          key: msg.key,
          body,
          zapi: zmsg,
        });
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

  startLocks.set(orgId, promise);

  try {
    return await promise;
  } finally {
    startLocks.delete(orgId);
  }
}

// ----------- SSE

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
    const qrSvg = await getQrSvg(s.qr);
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

  const onStatus = async (data: any) => {
    // Si on reçoit {type:"qr", qr}, on enrichit avec svg
    if (data?.type === "qr" && data?.qr && !data?.svg) {
      try {
        data.svg = await getQrSvg(data.qr);
      } catch {}
    }
    send("status", data);
  };

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
  const { orgId, waitQrMs } = req.body || {};
  if (!orgId) return res.status(400).json({ ok: false, error: "orgId required" });

  const org = String(orgId);
  const waitMs = Math.max(0, Math.min(Number(waitQrMs || 1200), 5000));

  try {
    const s = await startSession(org);

    // Si déjà connecté
    if (s.sock?.user && s.status === "connected") {
      return res.json({
        ok: true,
        status: s.status,
        hasQR: false,
        user: s.sock.user,
        qr: null,
        svg: null,
      });
    }

    // Si QR déjà dispo tout de suite
    if (s.qr) {
      const svg = await getQrSvg(s.qr);
      return res.json({
        ok: true,
        status: s.status,
        hasQR: true,
        user: s.sock?.user || null,
        qr: s.qr,
        svg,
      });
    }

    // Attente courte pour attraper le QR si il arrive juste après (améliore la vitesse perçue)
    if (waitMs > 0) {
      const bus = getBus(org);
      const got = await new Promise<{ qr: string; svg: string } | null>((resolve) => {
        const t = setTimeout(() => {
          cleanup();
          resolve(null);
        }, waitMs);

        const onStatus = async (data: any) => {
          if (data?.type === "qr" && data?.qr) {
            try {
              const svg = await getQrSvg(String(data.qr));
              cleanup();
              resolve({ qr: String(data.qr), svg });
            } catch {
              cleanup();
              resolve(null);
            }
          }
        };

        const cleanup = () => {
          clearTimeout(t);
          bus.off("status", onStatus);
        };

        bus.on("status", onStatus);
      });

      if (got) {
        const current = sessions.get(org);
        return res.json({
          ok: true,
          status: current?.status || "qr",
          hasQR: true,
          user: current?.sock?.user || null,
          qr: got.qr,
          svg: got.svg,
        });
      }
    }

    // Fallback: pas de QR encore, le front poll /wa/qr
    res.json({
      ok: true,
      status: s.status,
      hasQR: false,
      user: s.sock?.user || null,
      qr: null,
      svg: null,
    });
  } catch (err) {
    logger.error({ err, orgId: org }, "login error");
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

  const svg = await getQrSvg(s.qr);
  res.json({ ok: true, qr: s.qr, svg });
});

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

// ----------- OUTBOUND

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

// ----------- Lecture messages récents + médias

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

    const base64 = buffer.toString("base64");

    res.json({
      ok: true,
      mimetype: m,
      base64: `data:${m};base64,${base64}`,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

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

app.post("/wa/fetch-history", async (req: Request, res: Response) => {
  const { orgId, peer, count = 50, oldestMsgId, oldestMsgTimestamp } = req.body || {};
  if (!orgId || !peer) {
    return res.status(400).json({ ok: false, error: "orgId,peer required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    let refKey: any;
    let refTs: number;

    if (oldestMsgId && oldestMsgTimestamp) {
      refKey = { id: String(oldestMsgId), remoteJid: String(peer), fromMe: false };
      refTs = Number(oldestMsgTimestamp);
    } else {
      let oldestMsg: WAMessage | null = null;
      let oldestT = Infinity;
      s.msgCache.forEach((msg) => {
        if (msg.key.remoteJid === String(peer)) {
          const ts = Number(msg.messageTimestamp || 0);
          if (ts > 0 && ts < oldestT) { oldestT = ts; oldestMsg = msg; }
        }
      });
      if (!oldestMsg) {
        return res.status(404).json({ ok: false, error: "No cached messages found for this peer" });
      }
      refKey = (oldestMsg as WAMessage).key;
      refTs = Number((oldestMsg as WAMessage).messageTimestamp || 0);
    }

    const requestId = await (s.sock! as any).fetchMessageHistory(Number(count), refKey, refTs);
    logger.info({ orgId, peer, count, requestId }, "fetch-history requested");
    res.json({ ok: true, requestId, peer, count });
  } catch (err) {
    logger.error({ err, orgId, peer }, "/wa/fetch-history error");
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
