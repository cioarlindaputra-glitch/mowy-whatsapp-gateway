# Mowy WhatsApp gateway (QR)

This is the persistent Node service for the Mowy dashboard. The static Site cannot keep a WhatsApp Web socket alive by itself, so deploy this folder as a separate always-on service with a persistent disk mounted at `/data`.

## Deploy

1. Create a Node 22 service from this folder, or build its Dockerfile.
2. Attach a persistent disk and set `WA_SESSION_DIR=/data/whatsapp-session`.
3. Add every variable from `.env.example` in the host's secret/environment settings. `WA_ALLOWED_EMAIL` must match the email used to sign in to Mowy. Use the Supabase secret/service-role key only in the backend environment; never put it in dashboard HTML.
4. Set `APP_ORIGIN` to the exact Mowy Site URL, deploy, and wait for `GET /health` to return `{ "ok": true }`.
5. In Mowy → WhatsApp, enter the deployed HTTPS service URL, choose **Periksa backend**, then **Hubungkan WhatsApp**. Scan the live QR with WhatsApp → Perangkat tertaut → Tautkan perangkat.

The dashboard stores only the gateway URL in this browser. The gateway authenticates dashboard requests through Supabase and only permits the configured workspace email. The first successful account owns the one WhatsApp session on this instance.

## Current scope

- One linked WhatsApp account per gateway instance.
- Text messages in direct chats get AI replies using the latest saved Mowy knowledge and behavior.
- Groups, status messages, and media-only messages are ignored. A single-word `STOP`, `BERHENTI`, `JANGAN HUBUNGI`, or `UNSUBSCRIBE` opts that chat out; `START` or `MULAI` opts it back in. Opt-outs persist in the private session volume.
- The session folder contains sensitive WhatsApp credentials. Keep the host disk private, restrict dashboard access, back it up securely, and never publish it.

## WhatsApp method note

This QR adapter uses the unofficial Baileys library and is not affiliated with WhatsApp. WhatsApp's terms prohibit certain unauthorized automated use. For a business deployment where compliance and support matter, use the official WhatsApp Business Platform instead.
