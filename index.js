import fs from "node:fs";
import express from "express";
import QRCode from "qrcode";
import pino from "pino";
import { createClient } from "@supabase/supabase-js";
import makeWASocket, {
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  DisconnectReason,
  Browsers,
} from "@whiskeysockets/baileys";

const BOT_VERSION = "1.1.0";
const PORT = Number(process.env.PORT || 3000);
const SESSION_DIR = process.env.SESSION_DIR || "./session";
const SEND_INTERVAL_MS = 3000;
const HEARTBEAT_MS = 30000;

/*
 * As duas linhas abaixo ja vem preenchidas com o endereco do seu projeto.
 * Se voce definir SUPABASE_URL ou WEBHOOK_URL no Render, o valor do Render ganha.
 */
const FALLBACK_SUPABASE_URL = "https://y5bsz07hp6db54mu7oic.helloreaddy.com/";
const FALLBACK_WEBHOOK_URL = "https://y5bsz07hp6db54mu7oic.helloreaddy.com/functions/v1/wa-webhook";

const SUPABASE_URL = String(process.env.SUPABASE_URL || FALLBACK_SUPABASE_URL || "")
  .trim()
  .replace(/\/+$/, "");
const SUPABASE_KEY = String(process.env.SUPABASE_SERVICE_ROLE_KEY || "").trim();
const WEBHOOK_URL = String(process.env.WEBHOOK_URL || FALLBACK_WEBHOOK_URL || "").trim();
const BOT_TOKEN = String(process.env.BOT_TOKEN || "").trim();

const MISSING = [];
if (!SUPABASE_URL) MISSING.push("SUPABASE_URL");
if (!SUPABASE_KEY) MISSING.push("SUPABASE_SERVICE_ROLE_KEY");
const CONFIG_OK = MISSING.length === 0;

const supabase = CONFIG_OK
  ? createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } })
  : null;
const logger = pino({ level: "silent" });

let sock = null;
let starting = false;
let currentStatus = CONFIG_OK ? "connecting" : "error";
let connectedPhone = "";
let flushing = false;
let reconnectTimer = null;

function configBanner() {
  console.error("");
  console.error("==========================================================");
  console.error(" O BOT AINDA NAO PODE CONECTAR");
  console.error("");
  console.error(" Faltam variaveis no Render -> aba Environment:");
  MISSING.forEach((name) => console.error("   - " + name));
  console.error("");
  console.error(" Como resolver:");
  console.error("   1. Servico no Render -> aba Environment");
  console.error("   2. Add Environment Variable");
  console.error("   3. Nome: SUPABASE_SERVICE_ROLE_KEY");
  console.error("      Valor: a chave de servico do seu Backend");
  console.error("   4. Salve e clique em Manual Deploy > Deploy latest commit");
  console.error("");
  console.error(" O servico vai continuar no ar mostrando este aviso.");
  console.error("==========================================================");
  console.error("");
}

async function publishStatus(patch) {
  if (!supabase) return;
  if (patch.status) currentStatus = patch.status;
  const now = new Date().toISOString();
  const payload = {
    id: 1,
    bot_version: BOT_VERSION,
    updated_at: now,
    last_seen: now,
    ...patch,
  };
  try {
    const { error } = await supabase.from("wa_bot_status").upsert(payload, { onConflict: "id" });
    if (error) console.error("[bot] falha ao publicar status:", error.message);
  } catch (error) {
    console.error("[bot] erro ao publicar status:", error?.message || error);
  }
}

async function logEvent(event, level = "info", detail = {}) {
  if (!supabase) return;
  try {
    await supabase.from("wa_logs").insert({ event, level, detail });
  } catch (error) {
    console.error("[bot] log:", event, error?.message || error);
  }
}

function extractText(message) {
  if (!message) return "";
  if (message.conversation) return message.conversation;
  if (message.extendedTextMessage?.text) return message.extendedTextMessage.text;
  if (message.imageMessage?.caption) return message.imageMessage.caption;
  if (message.videoMessage?.caption) return message.videoMessage.caption;
  if (message.documentWithCaptionMessage?.message) return extractText(message.documentWithCaptionMessage.message);
  if (message.buttonsResponseMessage?.selectedDisplayText) return message.buttonsResponseMessage.selectedDisplayText;
  if (message.listResponseMessage?.title) return message.listResponseMessage.title;
  if (message.templateButtonReplyMessage?.selectedDisplayText) {
    return message.templateButtonReplyMessage.selectedDisplayText;
  }
  if (message.ephemeralMessage?.message) return extractText(message.ephemeralMessage.message);
  if (message.viewOnceMessage?.message) return extractText(message.viewOnceMessage.message);
  if (message.viewOnceMessageV2?.message) return extractText(message.viewOnceMessageV2.message);
  return "";
}

function resolvePhone(msg) {
  const direct = String(msg?.key?.remoteJid || "");
  const alternate = String(msg?.key?.remoteJidAlt || "");
  const candidate = direct.endsWith("@s.whatsapp.net") ? direct : alternate || direct;
  return candidate.split("@")[0].split(":")[0];
}

async function forwardToBackend(payload) {
  if (!WEBHOOK_URL) {
    console.warn("[bot] WEBHOOK_URL nao configurado; mensagem recebida foi ignorada.");
    return;
  }
  try {
    const response = await fetch(WEBHOOK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-bot-token": BOT_TOKEN,
      },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      const text = await response.text();
      console.error("[bot] webhook respondeu", response.status, text.slice(0, 300));
      await logEvent("Webhook respondeu com erro", "error", {
        status: response.status,
        body: text.slice(0, 300),
      });
    }
  } catch (error) {
    console.error("[bot] falha ao chamar o webhook:", error?.message || error);
    await logEvent("Falha ao chamar o webhook", "error", { message: error?.message || String(error) });
  }
}

async function flushQueue() {
  if (flushing || !sock || !supabase) return;
  flushing = true;
  try {
    const { data, error } = await supabase
      .from("wa_messages")
      .select("id, body, attempts, conversation_id, wa_conversations(phone)")
      .eq("status", "queued")
      .eq("direction", "outbound")
      .order("created_at", { ascending: true })
      .limit(10);

    if (error) {
      console.error("[bot] erro ao buscar fila:", error.message);
      return;
    }

    for (const row of data || []) {
      const phone = row?.wa_conversations?.phone || "";
      if (!phone) {
        await supabase
          .from("wa_messages")
          .update({ status: "failed", error: "Conversa sem telefone" })
          .eq("id", row.id);
        continue;
      }

      const jid = `${String(phone).replace(/\D/g, "")}@s.whatsapp.net`;
      try {
        await sock.sendMessage(jid, { text: row.body });
        await supabase
          .from("wa_messages")
          .update({ status: "sent", error: "", attempts: (row.attempts || 0) + 1 })
          .eq("id", row.id);
      } catch (sendError) {
        const attempts = (row.attempts || 0) + 1;
        const failed = attempts >= 3;
        await supabase
          .from("wa_messages")
          .update({
            status: failed ? "failed" : "queued",
            attempts,
            error: String(sendError?.message || sendError).slice(0, 300),
          })
          .eq("id", row.id);
        if (failed) {
          await logEvent("Falha no envio da mensagem", "error", {
            message_id: row.id,
            phone,
            message: String(sendError?.message || sendError),
          });
        }
        break;
      }
    }
  } catch (error) {
    console.error("[bot] erro na fila:", error?.message || error);
  } finally {
    flushing = false;
  }
}

function scheduleReconnect(reason) {
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(() => {
    console.log("[bot] tentando reconectar agora. motivo:", reason || "conexao encerrada");
    startBot();
  }, 4000);
}

async function startBot() {
  if (starting || !supabase) return;
  starting = true;

  try {
    if (sock) {
      try {
        sock.end(undefined);
      } catch {
        /* ignora */
      }
      sock = null;
    }

    const { state, saveCreds } = await useMultiFileAuthState(SESSION_DIR);
    const { version } = await fetchLatestBaileysVersion();

    sock = makeWASocket({
      version,
      auth: state,
      logger,
      printQRInTerminal: false,
      browser: Browsers.ubuntu("Chrome"),
      markOnline: false,
      syncFullHistory: false,
      generateHighQualityLinkPreview: false,
      getMessage: async () => undefined,
    });

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        try {
          const dataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 });
          await publishStatus({
            status: "qr",
            qr_code: dataUrl,
            qr_updated_at: new Date().toISOString(),
            phone: connectedPhone,
          });
          console.log("[bot] novo QR gerado. Abra a aba Conexao do painel e escaneie.");
        } catch (error) {
          console.error("[bot] falha ao gerar QR:", error?.message || error);
        }
      }

      if (connection === "open") {
        const raw = sock?.user?.id || "";
        connectedPhone = raw.split(":")[0].split("@")[0] || "";
        await publishStatus({ status: "connected", qr_code: "", phone: connectedPhone });
        await logEvent("Bot conectado ao WhatsApp", "info", { phone: connectedPhone });
        console.log("[bot] conectado como", connectedPhone);
      }

      if (connection === "close") {
        const statusCode = lastDisconnect?.error?.output?.statusCode;
        const loggedOut = statusCode === DisconnectReason.loggedOut;
        console.log("[bot] conexao fechada. codigo:", statusCode, "logout:", loggedOut);

        if (loggedOut) {
          connectedPhone = "";
          await publishStatus({ status: "connecting", qr_code: "", phone: "" });
          await logEvent("Sessao do WhatsApp encerrada (logout)", "warn", {});
          try {
            await fs.promises.rm(SESSION_DIR, { recursive: true, force: true });
            console.log("[bot] sessao antiga removida; um novo QR sera gerado.");
          } catch (error) {
            console.error("[bot] nao foi possivel limpar a sessao:", error?.message || error);
          }
        } else {
          await publishStatus({ status: "connecting", qr_code: "", phone: connectedPhone });
        }

        scheduleReconnect(loggedOut ? "logout" : "queda de conexao");
      }
    });

    sock.ev.on("messages.upsert", async ({ messages, type }) => {
      if (type !== "notify") return;

      for (const msg of messages) {
        if (!msg?.message || msg.key?.fromMe) continue;
        const jid = String(msg.key?.remoteJid || "");
        if (!jid || jid === "status@broadcast" || jid.endsWith("@g.us") || jid.endsWith("@broadcast")) continue;

        const text = extractText(msg.message).trim();
        if (!text) continue;

        const phone = resolvePhone(msg);
        if (!phone) continue;

        await forwardToBackend({
          message_id: msg.key.id,
          phone,
          push_name: msg.pushName || "",
          text,
          timestamp: Number(msg.messageTimestamp || Date.now() / 1000),
          is_group: false,
        });
      }
    });

    console.log("[bot] socket iniciado. aguardando QR ou conexao...");
  } catch (error) {
    console.error("[bot] falha ao iniciar:", error?.message || error);
    await publishStatus({ status: "error", qr_code: "" });
    scheduleReconnect("erro ao iniciar");
  } finally {
    starting = false;
  }
}

const app = express();

app.get("/", (request, response) => {
  response.json({
    service: "whatsapp-ia-bot",
    version: BOT_VERSION,
    status: currentStatus,
    phone: connectedPhone || null,
    ready: CONFIG_OK,
    missing: MISSING,
  });
});

app.get("/health", (request, response) => {
  response.json({
    ok: true,
    ready: CONFIG_OK,
    missing: MISSING,
    status: currentStatus,
    version: BOT_VERSION,
    phone: connectedPhone || null,
  });
});

app.listen(PORT, () => {
  console.log("[bot] servico ouvindo na porta " + PORT);
});

setInterval(() => flushQueue(), SEND_INTERVAL_MS);
setInterval(() => publishStatus({ phone: connectedPhone }), HEARTBEAT_MS);

if (CONFIG_OK) {
  publishStatus({ status: "connecting", qr_code: "", phone: "" }).then(() => startBot());
} else {
  configBanner();
  setInterval(configBanner, 120000);
}

process.on("unhandledRejection", (reason) => {
  console.error("[bot] promise rejeitada:", reason);
});
process.on("uncaughtException", (error) => {
  console.error("[bot] excecao nao tratada:", error?.message || error);
});
process.on("SIGTERM", async () => {
  console.log("[bot] encerrando...");
  try {
    await publishStatus({ status: "disconnected" });
  } catch {
    /* ignora */
  }
  process.exit(0);
});