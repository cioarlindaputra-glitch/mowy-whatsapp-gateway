# Mowy WhatsApp gateway (QR)

This is the persistent Node service for the Mowy dashboard. The static Site cannot keep a WhatsApp Web socket alive by itself, so deploy this folder as a separate always-on service with a persistent disk mounted at `/data`.

## Deploy

1. Create a Node 22 service from this folder, or build its Dockerfile.
2. Attach a persistent disk and set `WA_SESSION_DIR=/data/whatsapp-session`.
3. Add every variable from `.env.example` in the host's secret/environment settings. Use the Supabase secret/service-role key only in the backend environment; never put it in dashboard HTML.
4. Set `APP_ORIGIN` to the exact Mowy Site URL, deploy, and wait for `GET /health` to return `{ "ok": true }`.
5. In Mowy → WhatsApp, enter the deployed HTTPS service URL, choose **Periksa backend**, then **Hubungkan WhatsApp**. Scan the live QR with WhatsApp → Perangkat tertaut → Tautkan perangkat.

The gateway authenticates each dashboard request through Supabase. Every authenticated user ID gets a separate workspace directory for WhatsApp credentials, inbox, leads, appointments, business documents, and uploads. A gateway can serve multiple accounts; each account can connect one WhatsApp number.

## Company documents

The dashboard can extract text from PDF, DOCX, XLSX, XLS, and CSV files in the browser and send the extracted text to this gateway. The gateway stores each user's documents beneath `mowy-workspaces/<user-id>/`, so mount the parent data folder as a persistent volume (for example `/data`). The dashboard supports up to 20 files and 40,000 extracted characters total. After uploading or changing document categories, click **Simpan data** in the Knowledge Base page. Documents are kept on the gateway disk rather than Supabase user metadata. AI replies select relevant passages from the saved documents by category and message keywords.

## Inbox

New direct text messages and captions are stored in each user's workspace folder and can be listed and replied to through the authenticated `/api/inbox` routes. The dashboard polls the inbox while its Inbox page is open. Mount `/data` as a persistent disk and set `WA_SESSION_DIR=/data/whatsapp-session`; only messages received after deploying this version are recorded. Up to 100 conversations and 100 recent messages per conversation are retained.

## Current scope

- One linked WhatsApp account per user workspace; multiple workspaces can share a gateway process.
- Workspace folders are isolated by authenticated Supabase user ID. Keep the persistent disk private and do not share it across unrelated services.
- Text messages in direct chats get AI replies using the latest saved Mowy knowledge and behavior.
- Groups, status messages, and media-only messages are ignored. A single-word `STOP`, `BERHENTI`, `JANGAN HUBUNGI`, or `UNSUBSCRIBE` opts that chat out; `START` or `MULAI` opts it back in. Opt-outs persist in the private session volume.
- The session folder contains sensitive WhatsApp credentials. Keep the host disk private, restrict dashboard access, back it up securely, and never publish it.

## WhatsApp method note

This QR adapter uses the unofficial Baileys library and is not affiliated with WhatsApp. WhatsApp's terms prohibit certain unauthorized automated use. For a business deployment where compliance and support matter, use the official WhatsApp Business Platform instead.
