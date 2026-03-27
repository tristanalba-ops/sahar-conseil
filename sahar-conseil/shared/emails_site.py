"""
SAHAR Conseil — emails_site.py
Séquence emails conversion pour les leads du site.

Déclenchement :
  J+0  → soumission formulaire contact
  J+3  → relance si pas de réponse
  J+7  → contenu valeur (données DVF/DPE gratuites)
  Démo → confirmation de rendez-vous

Usage depuis automation ou webhook :
  from shared.emails_site import envoyer_sequence_j0, envoyer_j3, envoyer_j7, confirmer_demo
"""

from shared.automation import envoyer_email, _html, _url

# ─── UTM URL BUILDER ──────────────────────────────────────────────────────────

def _utm_url(base_url: str, source: str = "email", medium: str = "email",
             campaign: str = "sahar_sequence", content: str = "") -> str:
    """
    Génère une URL avec paramètres UTM pour tracker le trafic email.
    Ces UTMs sont captés par le site, poussés dans le datalayer GTM,
    et remontés dans GA4 pour attribution.

    Ex: https://sahar-conseil.fr/immobilier.html
        ?utm_source=email&utm_medium=email&utm_campaign=j0_immo&utm_content=cta_demo
    """
    sep = "&" if "?" in base_url else "?"
    url = f"{base_url}{sep}utm_source={source}&utm_medium={medium}&utm_campaign={campaign}"
    if content:
        url += f"&utm_content={content}"
    return url


from datetime import datetime


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _prenom(nom: str) -> str:
    nom = nom.strip()
    nom = nom.title() if nom == nom.upper() or nom == nom.lower() else nom
    return nom.split()[0]


SECTEURS_LABELS = {
    "Immobilier":           "l'immobilier",
    "Énergie / Rénovation": "la rénovation énergétique",
    "Retail / Franchise":   "le retail et la franchise",
    "Automobile":           "l'automobile",
    "RH / Recrutement":     "le recrutement",
    "Autre":                "votre secteur",
    "":                     "votre secteur",
}

SECTEURS_DATA = {
    "Immobilier":           ("421 000 transactions DVF en Gironde",
                             "Prix médian, sous-valorisation, score opportunité"),
    "Énergie / Rénovation": ("10M+ diagnostics DPE disponibles",
                             "Passoires F/G, score urgence, adresse exacte"),
    "Retail / Franchise":   ("Base SIRENE + BPE INSEE complète",
                             "Score attractivité zone, densité concurrence"),
    "RH / Recrutement":     ("Données DARES par bassin d'emploi",
                             "Tensions recrutement, métiers en pénurie"),
    "Automobile":           ("IRVE + immatriculations SDES",
                             "Zones blanches VE, potentiel concessionnaires"),
    "":                     ("Sources DVF, DPE, INSEE, SIRENE",
                             "Scoring automatique, exports, CRM pipeline"),
}


# ─── J+0 : BIENVENUE ─────────────────────────────────────────────────────────

def envoyer_j0(nom: str, email: str, secteur: str = "", message: str = "") -> bool:
    """
    Email immédiat après soumission du formulaire.
    Confirme la réception, donne une preuve de valeur, fixe une attente claire.
    """
    p = _prenom(nom)
    label = SECTEURS_LABELS.get(secteur, "votre secteur")
    data_volume, data_detail = SECTEURS_DATA.get(secteur, SECTEURS_DATA[""])

    corps = f"""
Bonjour {p},<br><br>
Votre demande est reçue. On revient vers vous sous <strong>24h ouvrées</strong> 
avec une démonstration sur vos données réelles.<br><br>

<div style="background:#f5f5f5;border-radius:6px;padding:16px 20px;margin:16px 0">
  <p style="margin:0 0 8px;font-size:12px;font-weight:700;text-transform:uppercase;
  letter-spacing:.07em;color:#888">Ce qu'on a sur {label}</p>
  <p style="margin:0 0 4px;font-size:15px;font-weight:600;color:#1a1a1a">{data_volume}</p>
  <p style="margin:0;font-size:14px;color:#555">{data_detail}</p>
</div>

On ne fait pas de présentation générique. On arrive avec vos données, 
votre zone, votre marché.<br><br>
À très vite.
"""
    cta_url = _utm_url(_url(), campaign="j0_bienvenue", content="cta_outils")
    html = _html(
        f"Reçu — on revient vers vous sous 24h.",
        corps,
        "Voir nos outils en attendant",
        cta_url
    )
    texte = (
        f"Bonjour {p}, votre demande est reçue. "
        f"Réponse sous 24h avec une démo sur vos données. "
        f"En attendant : {_url()}"
    )
    return envoyer_email(email, nom,
        "Reçu — on revient vers vous sous 24h", html, texte)


# ─── J+3 : RELANCE ───────────────────────────────────────────────────────────

def envoyer_j3(nom: str, email: str, secteur: str = "") -> bool:
    """
    Relance J+3 si pas de réponse ni de démo planifiée.
    Angle : une donnée concrète sur leur secteur pour créer de la curiosité.
    """
    p = _prenom(nom)
    label = SECTEURS_LABELS.get(secteur, "votre secteur")

    faits = {
        "Immobilier": (
            "Sur les 12 derniers mois en Gironde,",
            "les biens avec un score SAHAR &gt; 70 se sont vendus <strong>23% plus vite</strong> "
            "que la médiane du secteur.",
            "Combien de ces opportunités avez-vous manqué cette semaine ?"
        ),
        "Énergie / Rénovation": (
            "Depuis janvier 2025,",
            "les logements classés F ne peuvent plus être mis en location. "
            "En France, <strong>1,5 million de propriétaires</strong> sont concernés.",
            "Dans votre secteur, combien n'ont pas encore agi ?"
        ),
        "Retail / Franchise": (
            "Selon la BPE INSEE,",
            "<strong>23% des zones commerciales</strong> en France présentent une sous-densité "
            "d'équipements par rapport à leur bassin de population.",
            "Votre prochaine implantation est peut-être dans ces données."
        ),
        "RH / Recrutement": (
            "Selon les dernières données DARES,",
            "<strong>47% des offres d'emploi</strong> en France peinent à être pourvues "
            "faute de candidats qualifiés.",
            "Vos clients le vivent probablement en ce moment."
        ),
    }

    intro, fait, question = faits.get(secteur, (
        "Sur les données qu'on analyse,",
        "les professionnels qui utilisent le scoring open data qualifient "
        "<strong>3× plus de prospects</strong> par semaine.",
        "Ça mérite 20 minutes de démonstration."
    ))

    corps = f"""
{p},<br><br>
{intro}<br>
{fait}<br><br>
{question}<br><br>
Je peux vous montrer exactement ce qu'on a sur {label} — 
20 minutes, pas de slides, juste les données.<br><br>
Vous avez un créneau cette semaine ?
"""
    cta_url = _utm_url(_url() + "#contact", campaign="j3_relance", content=f"cta_demo_{secteur.lower().replace(' ','_') or 'generique'}")
    html = _html(
        f"Une donnée qui va vous parler, {p}.",
        corps,
        "Planifier une démo",
        cta_url
    )
    texte = (
        f"{p}, une donnée sur {label} que vous devriez voir. "
        f"20 min de démo, pas de slides. {_url()}#contact"
    )
    return envoyer_email(email, nom,
        f"Une donnée sur {label} que vous devriez voir", html, texte)


# ─── J+7 : CONTENU VALEUR ────────────────────────────────────────────────────

def envoyer_j7(nom: str, email: str, secteur: str = "") -> bool:
    """
    Email J+7 — contenu gratuit à valeur élevée.
    Donne quelque chose d'utile sans demander quoi que ce soit en retour.
    Objectif : rester présent, construire la crédibilité.
    """
    p = _prenom(nom)

    contenus = {
        "Immobilier": {
            "titre": "3 signaux DVF qui indiquent une opportunité avant tout le monde",
            "points": [
                ("<strong>Le bien se vend sous la médiane IRIS.</strong>",
                 "Un bien à −15% de la médiane de son IRIS n'est pas forcément dégradé. "
                 "C'est souvent une vente rapide, une succession, ou un prix négocié. "
                 "C'est votre signal d'entrée."),
                ("<strong>Le marché de la commune est liquide.</strong>",
                 "Plus de 15 transactions dans la commune sur 12 mois = marché actif. "
                 "Vous pouvez revendre. En dessous, vous prenez un risque de liquidité."),
                ("<strong>La dynamique est positive sur 6 mois.</strong>",
                 "Si le prix médian monte depuis 6 mois, vous achetez dans le sens du marché. "
                 "Si il baisse, vous négociez mieux mais vous portez plus de risque."),
            ],
            "lien_texte": "Tester le scoring DVF sur votre secteur",
            "lien_url": _utm_url(_url() + "/immobilier.html", campaign="j7_contenu", content="immobilier"),
        },
        "Énergie / Rénovation": {
            "titre": "Comment qualifier un prospect DPE en 30 secondes",
            "points": [
                ("<strong>L'étiquette seule ne suffit pas.</strong>",
                 "Un G construit en 2010 est moins urgent qu'un G de 1960. "
                 "L'année de construction est le premier filtre à appliquer."),
                ("<strong>La surface détermine le devis.</strong>",
                 "En dessous de 40m², les travaux sont souvent disproportionnés. "
                 "Au-dessus de 80m², le potentiel de chantier justifie le déplacement."),
                ("<strong>Le type de bien change tout.</strong>",
                 "Maison individuelle = isolation facile, pompe à chaleur possible. "
                 "Appartement en copropriété = décision collective, délai plus long."),
            ],
            "lien_texte": "Voir les F/G dans votre secteur",
            "lien_url": _utm_url(_url() + "/energie-renovation.html", campaign="j7_contenu", content="energie"),
        },
    }

    contenu = contenus.get(secteur, {
        "titre": "Ce que les données publiques vous disent sur votre marché",
        "points": [
            ("<strong>DVF</strong> — chaque vente notariée depuis 2019.",
             "Prix signé, surface, adresse, date. La vraie donnée de marché."),
            ("<strong>DPE ADEME</strong> — 10M+ diagnostics.",
             "Étiquette, consommation, adresse. Les prospects rénovation sont là."),
            ("<strong>SIRENE + BPE</strong> — toutes les entreprises et équipements.",
             "Concurrence, zone blanche, potentiel commercial."),
        ],
        "lien_texte": "Explorer les outils SAHAR",
        "lien_url": _utm_url(_url(), campaign="j7_contenu", content="generique"),
    })

    points_html = "".join([
        f'<div style="border-left:2px solid #e5e5e5;padding:8px 0 8px 16px;margin:12px 0">'
        f'<p style="margin:0 0 4px;font-size:15px">{pt}</p>'
        f'<p style="margin:0;font-size:14px;color:#555">{desc}</p>'
        f'</div>'
        for pt, desc in contenu["points"]
    ])

    corps = f"""
{p},<br><br>
Pas de pitch cette semaine — juste quelque chose d'utile.<br><br>
{points_html}
<br>
Si ces signaux vous parlent, c'est exactement ce qu'on automatise dans SAHAR.
"""
    html = _html(
        contenu["titre"],
        corps,
        contenu["lien_texte"],
        contenu["lien_url"]
    )
    texte = (
        f"{p}, 3 points sur {contenu['titre'].lower()}. "
        f"Pas de pitch — juste utile. {contenu['lien_url']}"
    )
    return envoyer_email(email, nom, contenu["titre"], html, texte)


# ─── DÉMO CONFIRMÉE ──────────────────────────────────────────────────────────

def confirmer_demo(nom: str, email: str,
                   date_demo: str, lien_visio: str = "",
                   secteur: str = "") -> bool:
    """
    Confirmation de rendez-vous démo.
    Envoyé manuellement depuis le CRM après planification.
    """
    p = _prenom(nom)
    label = SECTEURS_LABELS.get(secteur, "votre secteur")

    visio_block = ""
    if lien_visio:
        visio_block = (
            f'<div style="background:#f0f7ff;border-radius:6px;padding:14px 18px;margin:16px 0">'
            f'<p style="margin:0 0 6px;font-size:12px;font-weight:700;text-transform:uppercase;'
            f'letter-spacing:.07em;color:#185FA5">Lien de connexion</p>'
            f'<a href="{lien_visio}" style="font-size:14px;color:#185FA5;word-break:break-all">{lien_visio}</a>'
            f'</div>'
        )

    corps = f"""
Bonjour {p},<br><br>
Votre démo SAHAR est confirmée.<br><br>

<div style="background:#f5f5f5;border-radius:6px;padding:16px 20px;margin:16px 0">
  <p style="margin:0 0 4px;font-size:12px;font-weight:700;text-transform:uppercase;
  letter-spacing:.07em;color:#888">Rendez-vous</p>
  <p style="margin:0;font-size:18px;font-weight:700;color:#1a1a1a">{date_demo}</p>
</div>

{visio_block}

<strong>Ce qu'on va couvrir :</strong>
<ul style="margin:8px 0;padding-left:20px;font-size:15px">
  <li>Données disponibles sur {label}</li>
  <li>Scoring et détection d'opportunités</li>
  <li>Pipeline CRM et actions terrain</li>
  <li>Vos questions</li>
</ul>

Durée : 20 à 30 minutes. Pas de slides. On ouvre l'outil directement.<br><br>
À {date_demo.split()[0] if ' ' in date_demo else date_demo}.
"""
    html = _html(
        f"Démo confirmée — {date_demo}",
        corps,
        "Ajouter à mon agenda",
        lien_visio or _url()
    )
    texte = (
        f"Bonjour {p}, votre démo SAHAR est confirmée le {date_demo}. "
        f"{'Lien : ' + lien_visio if lien_visio else 'On vous envoie le lien.'}"
    )
    return envoyer_email(email, nom,
        f"Démo confirmée — {date_demo}", html, texte)
