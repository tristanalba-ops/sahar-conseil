/**
 * SAHAR Conseil — webhook-formspree
 * Supabase Edge Function
 *
 * Reçoit le webhook Formspree à chaque soumission du formulaire site.
 * Crée automatiquement un compte + lead dans le CRM SAHAR.
 * Déclenche l'email J+0 via Brevo.
 *
 * URL de déploiement :
 *   https://ylrrcbklufshebcizgus.supabase.co/functions/v1/webhook-formspree
 *
 * À configurer dans Formspree :
 *   Form settings → Integrations → Webhooks → Add webhook → coller l'URL
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("DB_URL") ?? Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_KEY = Deno.env.get("DB_SERVICE_KEY") ?? Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
const BREVO_KEY    = Deno.env.get("BREVO_API_KEY") ?? "";
const APP_URL      = Deno.env.get("APP_URL") ?? "https://sahar-conseil.fr";
const NOTIF_EMAIL  = Deno.env.get("NOTIF_EMAIL") ?? "contact@sahar-conseil.fr";

// ── Helpers ────────────────────────────────────────────────────────────────

function genId(prefix: string): string {
  return prefix + Math.random().toString(36).slice(2, 8).toUpperCase();
}

function now(): string {
  return new Date().toLocaleDateString("fr-FR");
}

// ── Email Brevo ────────────────────────────────────────────────────────────

async function sendEmail(
  toEmail: string, toNom: string,
  subject: string, html: string
): Promise<void> {
  if (!BREVO_KEY) return;
  await fetch("https://api.brevo.com/v3/smtp/email", {
    method: "POST",
    headers: { "api-key": BREVO_KEY, "Content-Type": "application/json" },
    body: JSON.stringify({
      sender: { email: "contact@sahar-conseil.fr", name: "SAHAR Conseil" },
      to: [{ email: toEmail, name: toNom }],
      subject,
      htmlContent: html,
    }),
  });
}

// ── Templates email ────────────────────────────────────────────────────────

function emailJ0(nom: string, secteur: string): string {
  const prenom = nom.split(" ")[0];
  const dataMap: Record<string, string> = {
    "Immobilier":            "421 000 transactions DVF en Gironde disponibles",
    "Énergie / Rénovation":  "10M+ diagnostics DPE — passoires F/G identifiées",
    "Retail / Franchise":    "Base SIRENE + BPE — score attractivité zone",
    "RH / Recrutement":      "Données DARES — tensions recrutement par bassin",
  };
  const data = dataMap[secteur] ?? "Sources DVF, DPE, INSEE, SIRENE";

  return `
  <div style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:32px 16px;background:#f5f5f5">
  <div style="background:#fff;border-radius:8px;border:1px solid #e5e5e5;overflow:hidden">
    <div style="padding:20px 28px;border-bottom:1px solid #e5e5e5">
      <span style="font-size:14px;font-weight:700;color:#1a1a1a">SAHAR <span style="color:#185FA5">Conseil</span></span>
    </div>
    <div style="padding:28px">
      <p style="font-size:20px;font-weight:700;color:#1a1a1a;margin:0 0 16px;letter-spacing:-.02em">
        Reçu — on revient vers vous sous 24h.
      </p>
      <p style="font-size:15px;color:#333;line-height:1.7;margin:0 0 16px">
        Bonjour ${prenom},<br><br>
        Votre demande est bien reçue. On vous contacte sous <strong>24h ouvrées</strong>
        avec une démonstration sur vos données réelles — pas une présentation générique.
      </p>
      <div style="background:#f5f5f5;border-radius:6px;padding:16px 20px;margin:16px 0">
        <p style="margin:0 0 6px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:#888">
          Ce qu'on a sur votre secteur
        </p>
        <p style="margin:0;font-size:14px;font-weight:600;color:#1a1a1a">${data}</p>
      </div>
      <p style="font-size:15px;color:#333;margin:16px 0">
        À très vite.
      </p>
      <a href="${APP_URL}?utm_source=email&utm_medium=email&utm_campaign=j0_bienvenue&utm_content=cta_outils"
         style="display:inline-block;background:#1a1a1a;color:#fff;text-decoration:none;
         padding:11px 22px;border-radius:6px;font-size:14px;font-weight:600">
        Voir nos outils en attendant →
      </a>
    </div>
    <div style="padding:16px 28px;border-top:1px solid #f0f0f0">
      <p style="margin:0;font-size:12px;color:#aaa">
        SAHAR Conseil · <a href="${APP_URL}" style="color:#aaa">${APP_URL}</a>
      </p>
    </div>
  </div>
  </div>`;
}

function emailNotif(nom: string, email: string, secteur: string, message: string, utms: Record<string, string>): string {
  return `
  <div style="font-family:sans-serif;max-width:560px;padding:24px">
    <h2 style="margin:0 0 16px;font-size:18px">🔔 Nouveau lead SAHAR</h2>
    <table style="font-size:14px;border-collapse:collapse;width:100%">
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600;white-space:nowrap">Nom</td><td><strong>${nom}</strong></td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Email</td><td><a href="mailto:${email}">${email}</a></td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Secteur</td><td>${secteur}</td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Message</td><td>${message}</td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">UTM source</td><td><code>${utms.utm_source ?? "direct"}</code></td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">UTM campaign</td><td><code>${utms.utm_campaign ?? "—"}</code></td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Landing page</td><td><code>${utms.sahar_landing ?? "/"}</code></td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Referrer</td><td><code>${utms.sahar_referrer ?? "direct"}</code></td></tr>
      <tr><td style="padding:6px 12px 6px 0;color:#888;font-weight:600">Date</td><td>${new Date().toLocaleString("fr-FR")}</td></tr>
    </table>
  </div>`;
}

// ── Handler principal ──────────────────────────────────────────────────────

Deno.serve(async (req: Request) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, {
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
      },
    });
  }

  if (req.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  try {
    const body = await req.json();

    // ── Extraire les champs Formspree ──────────────────────────────────────
    const nom      = (body.nom || body.name || "Inconnu").trim();
    const email    = (body.email || "").trim().toLowerCase();
    const secteur  = (body.secteur || "").trim();
    const message  = (body.message || "").trim();

    // UTMs injectés par le site
    const utms = {
      utm_source:    body.utm_source   || "direct",
      utm_medium:    body.utm_medium   || "direct",
      utm_campaign:  body.utm_campaign || "",
      utm_content:   body.utm_content  || "",
      utm_term:      body.utm_term     || "",
      sahar_landing: body.sahar_landing || "/",
      sahar_referrer: body.sahar_referrer || "direct",
    };

    if (!email) {
      return new Response(JSON.stringify({ error: "email manquant" }), { status: 400 });
    }

    // ── Écrire dans Supabase ───────────────────────────────────────────────
    const sb = createClient(SUPABASE_URL, SUPABASE_KEY);

    // 1. Créer le compte dans sahar_comptes
    const compteId = genId("CPT");
    await sb.from("sahar_comptes").insert({
      id:            compteId,
      nom:           nom,
      type:          "prospect",
      secteur:       secteur.toLowerCase().replace(" / ", "_").replace(" ", "_"),
      email:         email,
      source:        "Site inbound",
      utm_source:    utms.utm_source,
      utm_medium:    utms.utm_medium,
      utm_campaign:  utms.utm_campaign,
      utm_content:   utms.utm_content,
      utm_term:      utms.utm_term,
      landing_page:  utms.sahar_landing,
      notes:         message,
      date_creation: now(),
      date_update:   now(),
    });

    // 2. Créer le lead dans sahar_leads (si table existe)
    const leadId = genId("LED");
    await sb.from("sahar_leads").insert({
      id:         leadId,
      compte_id:  compteId,
      nom:        nom,
      email:      email,
      secteur:    secteur,
      message:    message,
      source:     "site",
      utm_source: utms.utm_source,
      utm_campaign: utms.utm_campaign,
      landing_page: utms.sahar_landing,
      statut:     "nouveau",
    }).catch(() => {}); // table optionnelle

    // 3. Logger dans sahar_tracking
    await sb.from("sahar_tracking").insert({
      id:           genId("TRK"),
      email:        email,
      page:         utms.sahar_landing,
      utm_source:   utms.utm_source,
      utm_medium:   utms.utm_medium,
      utm_campaign: utms.utm_campaign,
      utm_content:  utms.utm_content,
      utm_term:     utms.utm_term,
      referrer:     utms.sahar_referrer,
      event_type:   "form_submit",
      event_data:   { secteur, message_length: message.length },
    }).catch(() => {});

    // ── Emails ────────────────────────────────────────────────────────────

    // 4. Email J+0 au lead
    await sendEmail(email, nom,
      "Reçu — on revient vers vous sous 24h",
      emailJ0(nom, secteur)
    );

    // 5. Notification interne
    await sendEmail(NOTIF_EMAIL, "SAHAR Admin",
      `🔔 Nouveau lead : ${nom} (${secteur})`,
      emailNotif(nom, email, secteur, message, utms)
    );

    return new Response(
      JSON.stringify({ success: true, compte_id: compteId }),
      {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*",
        },
      }
    );

  } catch (err) {
    console.error("Webhook error:", err);
    return new Response(
      JSON.stringify({ error: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});
