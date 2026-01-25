// app/src/server.ts

import express, { Request, Response } from "express";
import cors from "cors";
import pino from "pino";
import fs from "fs-extra";
import path from "path";
import { LRUCache } from "lru-cache";
import { lookup as mimeLookup } from "mime-types";
import EventEmitter from "eventemitter3";
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

// ✅ On prend d’abord SESSIONS_DIR, sinon DATA_DIR, sinon ./sessions
const SESSIONS_DIR =
  process.env.SESSIONS_DIR ||
  process.env.DATA_DIR ||
  path.join(process.cwd(), "sessions");

// 🔄 Webhook (Make / Supabase / autre)
const WEBHOOK_URL = process.env.WA_WEBHOOK_URL || process.env.WEBHOOK_URL || "";

// 🌍 URL publique de la gateway (pour mediaUrl)
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
};

const sessions = new Map<string, Session>();

function createEmptySession(orgId: string): Session {
  return {
    orgId,
    bus: new EventEmitter(),
    status: "closed",
    qr: null,
    msgCache: new LRUCache({ max: 1000 }),
    chats: new Map(),
    contacts: new Map(),
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
  const digits = to.replace(/[^\d]/g, "").replace(/^00/, "");
  return `${digits}@s.whatsapp.net`;
}

/**
 * OUTBOUND LID resolution (best effort)
 * - accepte: numéro brut, PN JID, LID JID, g.us, etc.
 * - tente: PN -> LID via sock.signalRepository.lidMapping.getLIDForPN
 */
function normalizeToJid(to: string): string {
  const v = String(to || "").trim();
  if (!v) return "";
  if (v.includes("@")) return v; // déjà un JID
  return phoneToJid(v); // numéro brut -> PN JID
}

async function getLidForPnJid(sock: any, pnJid: string): Promise<string | null> {
  const lidStore = sock?.signalRepository?.lidMapping;
  if (!lidStore || typeof lidStore.getLIDForPN !== "function") return null;

  try {
    const raw = await Promise.resolve(lidStore.getLIDForPN(pnJid));
    if (!raw) return null;

    const s = String(raw);
    if (!s) return null;

    // selon impl, ça peut être "124..." ou "124...@lid"
    return s.includes("@") ? s : `${s}@lid`;
  } catch {
    return null;
  }
}

async function getPnForLidJid(sock: any, lidJid: string): Promise<string | null> {
  const lidStore = sock?.signalRepository?.lidMapping;
  if (!lidStore || typeof lidStore.getPNForLID !== "function") return null;

  try {
    const raw = await Promise.resolve(lidStore.getPNForLID(lidJid));
    if (!raw) return null;

    const s = String(raw);
    if (!s) return null;

    // selon impl, ça peut être "4178..." ou "4178...@s.whatsapp.net"
    return s.includes("@") ? s : `${s}@s.whatsapp.net`;
  } catch {
    return null;
  }
}

async function resolveRecipientJid(
  sock: any,
  to: string
): Promise<{ sendJid: string; toPn: string | null; toLid: string | null }> {
  const jid0 = normalizeToJid(to);

  if (!jid0) return { sendJid: "", toPn: null, toLid: null };

  // Si déjà LID, on utilise tel quel, mais on essaie de retrouver le PN pour le webhook (best effort)
  if (jid0.endsWith("@lid")) {
    const pn = await getPnForLidJid(sock, jid0);
    return { sendJid: jid0, toPn: pn, toLid: jid0 };
  }

  // Si pas PN, on ne tente pas de mapping (group/newsletter/etc.)
  if (!jid0.endsWith("@s.whatsapp.net")) {
    return { sendJid: jid0, toPn: null, toLid: null };
  }

  const pnJid = jid0;
  const lidJid = await getLidForPnJid(sock, pnJid);

  return {
    sendJid: lidJid || pnJid,
    toPn: pnJid,
    toLid: lidJid || null,
  };
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

// ----------- Helpers divers

// ✅ on NE considère pas @lid, @g.us, status, etc. comme des numéros de téléphone
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
  const jid = sess.sock?.user?.id; // ex: "41782640976:52@s.whatsapp.net"
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
  if (m.buttonsMessage?.contentText)
    return m.buttonsMessage.contentText as string;
  if (m.listMessage?.description) return m.listMessage.description as string;

  return undefined;
}

// ----------- Helper: envoyer vers le webhook externe

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

// ----------- Helper: payload style Z-API pour un message
// LID aware: on essaie de retrouver le PN via remoteJidAlt ou le store lidMapping

async function buildZapiLikeMessage(
  msg: WAMessage,
  sess: Session,
  orgId: string
): Promise<any> {
  const m: any = msg.message || {};
  const connectedPhone = getConnectedPhone(sess);

  const key: any = msg.key || {};
  const remoteJid = key.remoteJid as string | undefined;
  const remoteJidAlt = key.remoteJidAlt as string | undefined;
  const participant = key.participant as string | undefined;

  const isGroup = (remoteJid || "").endsWith("@g.us");
  const isLidChat = (remoteJid || "").endsWith("@lid");

  const chatId = remoteJid || null;
  const chatLid = isLidChat ? remoteJid : null;

  let phone: string | null = null;

  if (remoteJid) {
    if (isLidChat && !isGroup) {
      // DM avec LID -> on essaie de retrouver le PN
      let pnJid: string | undefined = remoteJidAlt;

      if (!pnJid && sess.sock) {
        const lidStore = (sess.sock as any).signalRepository?.lidMapping;
        if (lidStore && typeof lidStore.getPNForLID === "function") {
          try {
            const v = await Promise.resolve(lidStore.getPNForLID(remoteJid));
            pnJid = v || undefined;
          } catch {
            // ignore erreurs de mapping
          }
        }
      }

      if (pnJid) {
        phone = jidToPhone(pnJid);
      }
    } else {
      // Cas classique (PN ou groupe)
      phone = jidToPhone(remoteJid);
    }
  }

  const tsSec = Number(msg.messageTimestamp || 0) || 0;
  const tsMs = tsSec * 1000;

  const contact =
    (chatId && sess.contacts.get(chatId)) ||
    (phone ? sess.contacts.get(`${phone}@s.whatsapp.net`) : undefined);

  const displayName =
    contact?.name ||
    contact?.shortName ||
    (msg as any).pushName ||
    phone ||
    chatId;

  const base: any = {
    isStatusReply: false,
    chatLid, // LID si dispo, sinon null
    remoteJidAlt: remoteJidAlt || null, // IMPORTANT: redondance utile pour le merge côté wa-webhook
    connectedPhone,
    waitingMessage: false,
    isEdit: false,
    isGroup,
    isNewsletter: false,
    instanceId: orgId,
    messageId: msg.key.id,
    remoteJid: chatId,
    chatId,
    phone, // toujours un PN si on l’a trouvé, sinon null
    fromMe: !!msg.key.fromMe,
    momment: tsMs,
    status: msg.key.fromMe ? "SENT" : "RECEIVED",
    chatName: displayName,
    senderPhoto: null,
    senderName: displayName,
    photo: null,
    broadcast: false,
    participantLid: isGroup ? participant || null : null,
    forwarded: !!m.contextInfo?.isForwarded,
    type: "ReceivedCallback",
    fromApi: false,
  };

  // Texte
  const body = extractMessageBody(msg);
  if (body) {
    base.text = { message: body };
  }

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
      reactionBy: phone,
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

  return {
    id,
    name,
    unreadCount,
    lastMessageTimestamp,
    lastMessagePreview,
    isGroup,
  };
}

function normalizeContact(raw: any): ContactSummary | null {
  if (!raw || !raw.id) return null;
  const id = raw.id as string;
  const name = raw.name || raw.notify || raw.pushName || id;
  const notify = raw.notify;
  const shortName = raw.shortName || raw.name || raw.pushName || name;
  return { id, name, notify, shortName };
}

// ----------- Session bootstrap

async function startSession(orgId: string): Promise<Session> {
  let sess = sessions.get(orgId);

  // Si déjà connecté, on renvoie
  if (sess?.sock && sess.status === "connected") {
    return sess;
  }

  const authDir = path.join(SESSIONS_DIR, orgId);
  await fs.ensureDir(authDir);

  const { state, saveCreds } = await useMultiFileAuthState(authDir);
  const { version } = await fetchLatestBaileysVersion();

  if (!sess) {
    sess = createEmptySession(orgId);
  }

  sessions.set(orgId, sess);

  const sock = makeWASocket({
    version,
    printQRInTerminal: false,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    browser: ["Zuria", "Chrome", "1.0.0"],
    logger,
    // on ne demande PAS l'historique complet, ça réduit la charge et la phase "AwaitingInitialSync"
    syncFullHistory: false,
    // IMPORTANT: true (pas True)
    markOnlineOnConnect: true,
  });

  sess.sock = sock;
  sess.saveCreds = saveCreds;
  sess.status = "connecting";
  sess.qr = null;

  // Sauvegarde des creds
  sock.ev.on("creds.update", saveCreds);

  // (Optionnel) log si Baileys découvre des mappings PN<->LID
  sock.ev.on("lid-mapping.update", (m: any) => {
    logger.info({ orgId, m }, "lid-mapping.update");
  });

  // Événements de connexion
  sock.ev.on("connection.update", (u: any) => {
    const { connection, lastDisconnect, qr } = u;

    // QR reçu
    if (qr) {
      sess!.qr = qr;
      sess!.status = "qr";
      getBus(orgId).emit("status", { type: "qr", qr });
    }

    // Ouvert
    if (connection === "open") {
      sess!.status = "connected";
      sess!.qr = null;
      getBus(orgId).emit("status", { type: "connected", user: sock.user });
      logger.info({ orgId }, "WA connected");

      // Webhook: statut connecté
      void postWebhook("connection.open", orgId, {
        user: sock.user,
      });
      return;
    }

    // Fermé
    if (connection === "close") {
      const code: number = (lastDisconnect as any)?.error?.output?.statusCode ?? 0;

      // Codes qu’on considère comme "non récupérables"
      const fatalCodes: number[] = [
        DisconnectReason.loggedOut, // 401
        DisconnectReason.forbidden, // 403
        DisconnectReason.badSession,
        DisconnectReason.connectionReplaced, // 410
      ];

      const willReconnect = !fatalCodes.includes(code);

      sess!.status = "closed";
      getBus(orgId).emit("status", {
        type: "closed",
        code,
        willReconnect,
      });

      logger.warn({ orgId, code, willReconnect }, "WA closed");

      // Webhook: statut fermé
      void postWebhook("connection.close", orgId, {
        code,
        willReconnect,
      });

      if (!willReconnect) {
        // on supprime la session en mémoire & disque
        sessions.delete(orgId);
        clearSessionAuth(orgId).catch(() => {});
      } else {
        // cas 515 / restartRequired & co -> on relance la session avec les mêmes creds
        setTimeout(() => {
          logger.info({ orgId, code }, "auto-restart WA session");
          startSession(orgId).catch((err) =>
            logger.error({ err, orgId }, "failed to restart session")
          );
        }, 1000);
      }
    }
  });

  // Historique initial (chats, contacts, messages)
  sock.ev.on("messaging-history.set", (payload: any) => {
    const { chats, contacts, messages, syncType } = payload || {};

    if (Array.isArray(chats)) {
      for (const c of chats) {
        const summary = normalizeChat(c);
        if (summary) {
          sess!.chats.set(summary.id, summary);
        }
      }
    }

    if (Array.isArray(contacts)) {
      for (const c of contacts) {
        const summary = normalizeContact(c);
        if (summary) {
          sess!.contacts.set(summary.id, summary);
        }
      }
    }

    if (Array.isArray(messages)) {
      for (const msg of messages as WAMessage[]) {
        if (msg.key && msg.key.id) {
          sess!.msgCache.set(msg.key.id, msg);
        }
      }
    }

    getBus(orgId).emit("history", {
      type: "set",
      syncType,
      chats: Array.from(sess!.chats.values()),
      contacts: Array.from(sess!.contacts.values()),
    });

    // On NE pousse PAS cet historique vers le webhook
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

    if (updated.length) {
      getBus(orgId).emit("chats", { type: "upsert", chats: updated });
    }
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

      if (u.name || u.subject) {
        merged.name = u.name || u.subject;
      }

      sess!.chats.set(id, merged);
      updated.push(merged);
    }

    if (updated.length) {
      getBus(orgId).emit("chats", { type: "update", chats: updated });
    }
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

    if (updated.length) {
      getBus(orgId).emit("contacts", {
        type: "upsert",
        contacts: updated,
      });
    }
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
      };

      sess!.contacts.set(id, merged);
      updated.push(merged);
    }

    if (updated.length) {
      getBus(orgId).emit("contacts", {
        type: "update",
        contacts: updated,
      });
    }
  });

  // Messages entrants => cache + bus + webhook (INBOUND uniquement)
  sock.ev.on("messages.upsert", async (m: any) => {
    const up = m.messages || [];
    for (const msg of up as WAMessage[]) {
      if (msg.key && msg.key.id) {
        sess!.msgCache.set(msg.key.id, msg);
      }

      const key: any = msg.key || {};
      const remoteJid = key.remoteJid as string | undefined;
      const remoteJidAlt = key.remoteJidAlt as string | undefined;
      const isGroup = (remoteJid || "").endsWith("@g.us");
      const isLidChat = (remoteJid || "").endsWith("@lid");

      const messageType = msg.message ? Object.keys(msg.message)[0] : undefined;
      const body = extractMessageBody(msg);

      let phone: string | null = null;

      if (remoteJid) {
        if (isLidChat && !isGroup) {
          // DM avec LID -> essaie de retrouver le PN (robuste sync/async)
          let pnJid: string | undefined = remoteJidAlt;

          if (!pnJid && sess!.sock) {
            const lidStore = (sess!.sock as any).signalRepository?.lidMapping;
            if (lidStore && typeof lidStore.getPNForLID === "function") {
              try {
                const v = await Promise.resolve(lidStore.getPNForLID(remoteJid));
                pnJid = v || undefined;
              } catch {
                // ignore
              }
            }
          }

          if (pnJid) {
            phone = jidToPhone(pnJid);
          }
        } else {
          phone = jidToPhone(remoteJid);
        }
      }

      const simplified = {
        id: msg.key.id,
        from: remoteJid,
        fromMe: msg.key.fromMe,
        pushName: (msg as any).pushName,
        timestamp: (msg.messageTimestamp || 0).toString(),
        messageType,
        body,
        phone, // PN si trouvé
        chatLid: isLidChat ? remoteJid : null,
        remoteJidAlt: remoteJidAlt || null,
        isGroup,
      };

      // 🔴 SSE pour Lovable (UI)
      getBus(orgId).emit("message", {
        type: "message",
        message: simplified,
      });

      // 🔔 Webhook Supabase (INBOUND) uniquement
      if (!msg.key.fromMe) {
        const zmsg = await buildZapiLikeMessage(msg, sess!, orgId);

        const webhookPayload = {
          ...simplified,
          zapi: zmsg,
        };

        void postWebhook("message.incoming", orgId, webhookPayload);
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

  return sess;
}

// ----------- SSE (événements temps réel)

app.get("/wa/sse", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  if (!orgId) return res.status(400).end("orgId required");

  // Garder la connexion ouverte
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

  // Snapshot initial
  const s = sessions.get(orgId);
  send("hello", {
    orgId,
    status: s?.status || "closed",
    hasQR: Boolean(s?.qr),
    connected: Boolean(s?.sock?.user),
    user: s?.sock?.user || null,
  });

  // Si un QR est déjà présent, on renvoie le SVG directement
  if (s?.qr) {
    const qrSvg = await QRCode.toString(s.qr, { type: "svg" });
    send("qr", { qr: s.qr, svg: qrSvg });
  }

  // Si on a déjà de l’historique en mémoire, on l’envoie une fois
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
  if (!orgId) {
    return res.status(400).json({ ok: false, error: "orgId required" });
  }

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
  if (!orgId) {
    return res.status(400).json({ ok: false, error: "orgId required" });
  }

  const s = sessions.get(orgId);
  res.json({
    ok: true,
    status: s?.status || "closed",
    hasQR: Boolean(s?.qr),
    user: s?.sock?.user || null,
    connected: Boolean(s?.sock?.user),
  });
});

app.get("/wa/qr", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  if (!orgId) {
    return res.status(400).json({ ok: false, error: "orgId required" });
  }

  const s = sessions.get(orgId);
  if (!s?.qr) {
    return res.status(404).json({ ok: false, error: "No pending QR" });
  }

  const svg = await QRCode.toString(s.qr, { type: "svg" });
  res.json({ ok: true, qr: s.qr, svg });
});

// ➕ Bootstrap : renvoyer les dernières conversations + contacts
app.get("/wa/bootstrap", async (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  const limit = Number(req.query.limit || 20);

  if (!orgId) {
    return res.status(400).json({ ok: false, error: "orgId required" });
  }

  const s = sessions.get(orgId);
  if (!s) {
    return res.status(404).json({ ok: false, error: "No session" });
  }

  const chats = Array.from(s.chats.values()).sort(
    (a, b) => (b.lastMessageTimestamp || 0) - (a.lastMessageTimestamp || 0)
  );

  const contacts = Array.from(s.contacts.values());

  res.json({
    ok: true,
    chats: chats.slice(0, limit),
    contacts,
  });
});

// ➕ Avatar à la demande
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
  if (!orgId) {
    return res.status(400).json({ ok: false, error: "orgId required" });
  }

  const id = String(orgId);
  const s = sessions.get(id);

  try {
    await s?.sock?.logout();
  } catch (e) {
    logger.warn({ e, orgId: id }, "logout error (ignored)");
  }

  sessions.delete(id);

  // ✅ On supprime aussi l’auth disque pour forcer un nouveau QR au prochain login
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
    const { sendJid, toPn, toLid } = await resolveRecipientJid(s.sock, String(to));

    if (!sendJid) {
      return res.status(400).json({ ok: false, error: "Invalid recipient" });
    }

    const options: any = {};

    if (quotedMsgId) {
      options.quoted = {
        key: { id: quotedMsgId, fromMe: false, remoteJid: sendJid },
      };
    }

    const content: AnyMessageContent = { text: String(text) };
    if (Array.isArray(mentions) && mentions.length) {
      (content as any).mentions = mentions
        .map((p: string) => normalizeToJid(p))
        .filter(Boolean);
    }

    const sent = await s.sock!.sendMessage(sendJid, content, options);

    // Webhook: message sortant (via Zuria)
    void postWebhook("message.outgoing", String(orgId), {
      kind: "text",
      to: sendJid, // JID réellement utilisé
      toPn, // alias PN si connu
      toLid, // alias LID si résolu
      key: sent.key,
      body: String(text),
    });

    res.json({ ok: true, key: sent.key, to: sendJid, toPn, toLid });
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
    const { sendJid, toPn, toLid } = await resolveRecipientJid(s.sock, String(to));

    if (!sendJid) {
      return res.status(400).json({ ok: false, error: "Invalid recipient" });
    }

    const buf = await bufferFromInput(image);

    const msg: AnyMessageContent = buf
      ? { image: buf, caption }
      : { image: { url: image.url }, caption };

    const sent = await s.sock!.sendMessage(sendJid, msg);

    void postWebhook("message.outgoing", String(orgId), {
      kind: "image",
      to: sendJid,
      toPn,
      toLid,
      key: sent.key,
      caption: caption || null,
    });

    res.json({ ok: true, key: sent.key, to: sendJid, toPn, toLid });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/document", async (req: Request, res: Response) => {
  const { orgId, to, fileName, mimetype, document } = req.body || {};
  if (!orgId || !to || !document) {
    return res
      .status(400)
      .json({ ok: false, error: "orgId,to,document required" });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const { sendJid, toPn, toLid } = await resolveRecipientJid(s.sock, String(to));

    if (!sendJid) {
      return res.status(400).json({ ok: false, error: "Invalid recipient" });
    }

    const buf = await bufferFromInput(document);

    const msg: AnyMessageContent = buf
      ? {
          document: buf,
          fileName: fileName || "file",
          mimetype,
        }
      : {
          document: { url: document.url },
          fileName: fileName || "file",
          mimetype,
        };

    const sent = await s.sock!.sendMessage(sendJid, msg);

    void postWebhook("message.outgoing", String(orgId), {
      kind: "document",
      to: sendJid,
      toPn,
      toLid,
      key: sent.key,
      fileName: fileName || "file",
      mimetype: mimetype || null,
    });

    res.json({ ok: true, key: sent.key, to: sendJid, toPn, toLid });
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
    const { sendJid, toPn, toLid } = await resolveRecipientJid(s.sock, String(to));

    if (!sendJid) {
      return res.status(400).json({ ok: false, error: "Invalid recipient" });
    }

    const buf = await bufferFromInput(audio);

    const msg: AnyMessageContent = buf
      ? { audio: buf, ptt: Boolean(ptt) }
      : { audio: { url: audio.url }, ptt: Boolean(ptt) };

    const sent = await s.sock!.sendMessage(sendJid, msg);

    void postWebhook("message.outgoing", String(orgId), {
      kind: "audio",
      to: sendJid,
      toPn,
      toLid,
      key: sent.key,
      ptt: Boolean(ptt),
    });

    res.json({ ok: true, key: sent.key, to: sendJid, toPn, toLid });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/buttons", async (req: Request, res: Response) => {
  const { orgId, to, text, footer, buttons } = req.body || {};
  if (!orgId || !to || !text || !Array.isArray(buttons)) {
    return res.status(400).json({
      ok: false,
      error: "orgId,to,text,buttons required",
    });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const { sendJid, toPn, toLid } = await resolveRecipientJid(s.sock, String(to));

    if (!sendJid) {
      return res.status(400).json({ ok: false, error: "Invalid recipient" });
    }

    const msg: AnyMessageContent = {
      text,
      footer,
      buttons: buttons.map((b: any, i: number) => ({
        buttonId: String(b.id ?? `btn_${i + 1}`),
        buttonText: {
          displayText: String(b.label ?? b.text ?? `Option ${i + 1}`),
        },
        type: 1,
      })),
      headerType: 1,
    } as any;

    const sent = await s.sock!.sendMessage(sendJid, msg);

    void postWebhook("message.outgoing", String(orgId), {
      kind: "buttons",
      to: sendJid,
      toPn,
      toLid,
      key: sent.key,
      text,
    });

    res.json({ ok: true, key: sent.key, to: sendJid, toPn, toLid });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

app.post("/wa/send/list", async (req: Request, res: Response) => {
  const { orgId, to, title, text, footer, buttonText, sections } = req.body || {};
  if (!orgId || !to || !text || !Array.isArray(sections)) {
    return res.status(400).json({
      ok: false,
      error: "orgId,to,text,sections required",
    });
  }

  const s = getSessionOr404(String(orgId), res);
  if (!s) return;

  try {
    const { sendJid, toPn, toLid } = await resolveRecipientJid(s.sock, String(to));

    if (!sendJid) {
      return res.status(400).json({ ok: false, error: "Invalid recipient" });
    }

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

    const sent = await s.sock!.sendMessage(sendJid, msg);

    void postWebhook("message.outgoing", String(orgId), {
      kind: "list",
      to: sendJid,
      toPn,
      toLid,
      key: sent.key,
      title,
      text,
    });

    res.json({ ok: true, key: sent.key, to: sendJid, toPn, toLid });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// ----------- Lecture messages récents (et médias)

app.get("/wa/messages/recent", (req: Request, res: Response) => {
  const orgId = String(req.query.orgId || "");
  const limit = Number(req.query.limit || 50);

  const s = sessions.get(orgId);
  if (!s) {
    return res.status(404).json({ ok: false, error: "No session" });
  }

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
  if (!msg) {
    return res.status(404).json({ ok: false, error: "Message not in cache" });
  }

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

// ➕ GET direct pour media (pour audioUrl / imageUrl style Z-API)
app.get("/wa/media/:orgId/:msgId", async (req: Request, res: Response) => {
  const { orgId, msgId } = req.params;
  if (!orgId || !msgId) {
    return res.status(400).json({ ok: false, error: "orgId,msgId required" });
  }

  const s = getSessionOr404(orgId, res);
  if (!s) return;

  const msg = s.msgCache.get(String(msgId));
  if (!msg) {
    return res.status(404).json({ ok: false, error: "Message not in cache" });
  }

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
