"""
SAHAR Conseil — Générateur de rapport d'estimation immobilière PDF
Rapport multi-pages professionnel inspiré MeilleursAgents / Immofacile / Efficity.
Combine : analyse de marché, comparables, scoring, recommandations stratégiques.
"""

import io
import math
from datetime import datetime

import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether, PageBreak, Image
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.graphics.shapes import Drawing, Rect, String, Circle, Line, Wedge
from reportlab.graphics import renderPDF
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ── Enregistrement police Poppins (ronde, moderne) ──────────────────────────
_FONT_DIR = "/usr/share/fonts/truetype/google-fonts"
pdfmetrics.registerFont(TTFont("Poppins",       f"{_FONT_DIR}/Poppins-Regular.ttf"))
pdfmetrics.registerFont(TTFont("Poppins-Bold",   f"{_FONT_DIR}/Poppins-Bold.ttf"))
pdfmetrics.registerFont(TTFont("Poppins-Medium", f"{_FONT_DIR}/Poppins-Medium.ttf"))
pdfmetrics.registerFont(TTFont("Poppins-Italic", f"{_FONT_DIR}/Poppins-Italic.ttf"))
pdfmetrics.registerFontFamily(
    "Poppins",
    normal="Poppins",
    bold="Poppins-Bold",
    italic="Poppins-Italic",
    boldItalic="Poppins-Bold",
)

# ── Palette SAHAR — Brand Identity ────────────────────────────────────────────
# Primaire : vert SAHAR #00DC82, fond sombre #0D0D0D
SAHAR_GREEN   = colors.HexColor("#00DC82")
SAHAR_GREEN_D = colors.HexColor("#00B86B")   # vert fonce pour texte sur blanc
SAHAR_GREEN_C = colors.HexColor("#E6FFF3")   # vert tres clair (backgrounds)
DARK          = colors.HexColor("#0D0D0D")   # fond couverture
DARK_2        = colors.HexColor("#141414")   # variante sombre
DARK_3        = colors.HexColor("#1E1E1E")   # headers tables
VERT          = colors.HexColor("#00DC82")   # signal positif (= brand)
VERT_C        = colors.HexColor("#E6FFF3")
ROUGE         = colors.HexColor("#E24B4A")
ROUGE_C       = colors.HexColor("#FDE8E8")
ORANGE        = colors.HexColor("#BA7517")
ORANGE_C      = colors.HexColor("#FFF3E0")
GRIS_F        = colors.HexColor("#F5F5F5")
GRIS_M        = colors.HexColor("#CCCCCC")
GRIS_B        = colors.HexColor("#888888")
NOIR          = colors.HexColor("#1A1A1A")
BLANC         = colors.white
# Aliases de compatibilite
BLEU   = SAHAR_GREEN_D
BLEU_F = DARK
BLEU_C = SAHAR_GREEN_C

PAGE_W, PAGE_H = A4
MARGIN = 18 * mm
USABLE_W = PAGE_W - 2 * MARGIN


# ── Styles ───────────────────────────────────────────────────────────────────

def _styles():
    base = getSampleStyleSheet()
    s = {}

    # Couverture
    s["cover_title"] = ParagraphStyle(
        "cover_title", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=28,
        textColor=BLANC, leading=34, spaceAfter=6
    )
    s["cover_sub"] = ParagraphStyle(
        "cover_sub", parent=base["Normal"],
        fontName="Poppins", fontSize=14,
        textColor=colors.HexColor("#A0A0A0"), leading=18
    )
    s["cover_date"] = ParagraphStyle(
        "cover_date", parent=base["Normal"],
        fontName="Poppins", fontSize=10,
        textColor=colors.HexColor("#707070"), leading=13
    )

    # Contenu
    s["h1"] = ParagraphStyle(
        "h1", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=16,
        textColor=DARK_3, leading=20, spaceBefore=4, spaceAfter=8
    )
    s["h2"] = ParagraphStyle(
        "h2", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=12,
        textColor=SAHAR_GREEN_D, leading=16, spaceBefore=10, spaceAfter=4
    )
    s["h3"] = ParagraphStyle(
        "h3", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=10,
        textColor=SAHAR_GREEN_D, leading=13, spaceBefore=6, spaceAfter=3
    )
    s["body"] = ParagraphStyle(
        "body", parent=base["Normal"],
        fontName="Poppins", fontSize=9,
        textColor=NOIR, leading=13
    )
    s["body_bold"] = ParagraphStyle(
        "body_bold", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=9,
        textColor=NOIR, leading=13
    )
    s["small"] = ParagraphStyle(
        "small", parent=base["Normal"],
        fontName="Poppins", fontSize=7.5,
        textColor=GRIS_B, leading=10
    )
    s["caption"] = ParagraphStyle(
        "caption", parent=base["Normal"],
        fontName="Poppins-Italic", fontSize=7.5,
        textColor=GRIS_B, leading=10
    )
    s["kpi_val"] = ParagraphStyle(
        "kpi_val", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=20,
        textColor=SAHAR_GREEN_D, alignment=TA_CENTER, leading=24
    )
    s["kpi_label"] = ParagraphStyle(
        "kpi_label", parent=base["Normal"],
        fontName="Poppins", fontSize=7.5,
        textColor=GRIS_B, alignment=TA_CENTER, leading=10
    )
    s["footer"] = ParagraphStyle(
        "footer", parent=base["Normal"],
        fontName="Poppins", fontSize=6.5,
        textColor=GRIS_B, alignment=TA_CENTER
    )
    s["reco_title"] = ParagraphStyle(
        "reco_title", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=11,
        textColor=BLANC, leading=14, alignment=TA_LEFT
    )
    s["reco_body"] = ParagraphStyle(
        "reco_body", parent=base["Normal"],
        fontName="Poppins", fontSize=9,
        textColor=NOIR, leading=13
    )
    s["badge"] = ParagraphStyle(
        "badge", parent=base["Normal"],
        fontName="Poppins-Bold", fontSize=9,
        textColor=BLANC, alignment=TA_CENTER, leading=12
    )
    return s


# ── Helpers ──────────────────────────────────────────────────────────────────

def _color_signal(signal: str):
    sl = signal.lower()
    if "vendeur" in sl:
        return ROUGE
    if "quilibr" in sl:
        return ORANGE
    return VERT

def _color_score(score):
    try:
        s = float(score)
    except (TypeError, ValueError):
        return GRIS_B
    if s >= 70: return VERT
    if s >= 40: return ORANGE
    return ROUGE

def _fmt(val, fmt_str="{:,.0f}", fallback="N/D"):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return fallback
    try:
        return fmt_str.format(val)
    except (TypeError, ValueError):
        return str(val)

def _pct(val, fallback="N/D"):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return fallback
    return f"{val:+.1f}%"

def _indice_confiance(nb_transactions: int, mois_couverture: int = 12) -> tuple:
    """Retourne (label, couleur, explication) inspiré MeilleursAgents."""
    if nb_transactions >= 30:
        return ("Elevee", VERT, "Plus de 30 transactions comparables sur la periode")
    elif nb_transactions >= 10:
        return ("Moyenne", ORANGE, "Entre 10 et 30 transactions comparables")
    else:
        return ("Faible", ROUGE, f"Seulement {nb_transactions} transactions comparables")

def _section_divider(story, S, title: str, icon: str = ""):
    """Ajoute un séparateur de section."""
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width=USABLE_W, thickness=0.8, color=SAHAR_GREEN, spaceAfter=2))
    story.append(Paragraph(f"{icon} {title}" if icon else title, S["h2"]))
    story.append(Spacer(1, 2*mm))


# ── Drawing: jauge score ─────────────────────────────────────────────────────

def _draw_score_gauge(score: int, size: int = 80) -> Drawing:
    """Dessine un arc de cercle gradué représentant le score 0-100."""
    d = Drawing(size + 10, size * 0.7)
    cx, cy = (size + 10) / 2, size * 0.55

    # Arc fond
    for i in range(0, 180, 2):
        angle = math.radians(i)
        x1 = cx + (size/2 - 4) * math.cos(math.pi - angle)
        y1 = cy + (size/2 - 4) * math.sin(math.pi - angle)
        x2 = cx + (size/2 + 4) * math.cos(math.pi - angle)
        y2 = cy + (size/2 + 4) * math.sin(math.pi - angle)
        color = GRIS_M
        d.add(Line(x1, y1, x2, y2, strokeColor=color, strokeWidth=1))

    # Arc score
    score_angle = int(score * 1.8)
    for i in range(0, min(score_angle, 180), 2):
        angle = math.radians(i)
        x1 = cx + (size/2 - 4) * math.cos(math.pi - angle)
        y1 = cy + (size/2 - 4) * math.sin(math.pi - angle)
        x2 = cx + (size/2 + 4) * math.cos(math.pi - angle)
        y2 = cy + (size/2 + 4) * math.sin(math.pi - angle)
        pct = i / 180
        if pct < 0.33:
            c = ROUGE
        elif pct < 0.66:
            c = ORANGE
        else:
            c = VERT
        d.add(Line(x1, y1, x2, y2, strokeColor=c, strokeWidth=2))

    # Score text
    sc = _color_score(score)
    d.add(String(cx, cy - 12, f"{score}", fontSize=18, fontName="Poppins-Bold",
                 fillColor=sc, textAnchor="middle"))
    d.add(String(cx, cy - 22, "/100", fontSize=8, fontName="Poppins",
                 fillColor=GRIS_B, textAnchor="middle"))
    return d


# ── Drawing: barre horizontale comparative ───────────────────────────────────

def _draw_bar_compare(val_commune, val_dept, label_c="Commune", label_d="Departement",
                      max_val=None, width=220, height=40) -> Drawing:
    """Mini bar chart comparatif commune vs département."""
    d = Drawing(width, height)
    if max_val is None:
        max_val = max(val_commune or 1, val_dept or 1) * 1.2

    bar_h = 10
    y1, y2 = height - 14, height - 30

    # Labels
    d.add(String(0, y1 + 2, label_c, fontSize=7, fontName="Poppins", fillColor=NOIR))
    d.add(String(0, y2 + 2, label_d, fontSize=7, fontName="Poppins", fillColor=GRIS_B))

    x_start = 70
    bar_w = width - x_start - 50

    # Commune bar
    w1 = bar_w * min((val_commune or 0) / max_val, 1)
    d.add(Rect(x_start, y1, w1, bar_h, fillColor=SAHAR_GREEN_D, strokeColor=None))
    d.add(String(x_start + w1 + 3, y1 + 1, _fmt(val_commune),
                 fontSize=7, fontName="Poppins-Bold", fillColor=SAHAR_GREEN_D))

    # Dept bar
    w2 = bar_w * min((val_dept or 0) / max_val, 1)
    d.add(Rect(x_start, y2, w2, bar_h, fillColor=GRIS_M, strokeColor=None))
    d.add(String(x_start + w2 + 3, y2 + 1, _fmt(val_dept),
                 fontSize=7, fontName="Poppins", fillColor=GRIS_B))

    return d


# ══════════════════════════════════════════════════════════════════════════════
# ── GENERATEUR PRINCIPAL ─────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def generer_rapport_commune(
    commune: str,
    dept: str,
    row_commune: dict,
    df_transactions,
    nb_total_dept: int,
    sigma_factor: float = 1.0,
) -> bytes:
    """
    Genere un rapport PDF A4 multi-pages professionnel.
    Retourne les bytes du PDF (utilisable avec st.download_button).
    """
    buf = io.BytesIO()
    S = _styles()
    date_str = datetime.now().strftime("%d/%m/%Y")
    date_fichier = datetime.now().strftime("%Y%m%d")

    # Extraction des données
    score    = row_commune.get("Score marche", row_commune.get("Score marché", 0))
    prix_med = row_commune.get("Prix median \u20ac/m\u00b2", row_commune.get("Prix médian €/m²", 0))
    evol     = row_commune.get("Evolution 12m (%)", row_commune.get("Évolution 12m (%)", None))
    volume   = row_commune.get("Transactions 12m", 0)
    signal   = row_commune.get("Signal marche", row_commune.get("Signal marché", "—"))
    ratio_t  = row_commune.get("Ratio tension", None)
    surf_med = row_commune.get("Surface mediane m\u00b2", row_commune.get("Surface médiane m²", None))
    val_med  = row_commune.get("Prix median total \u20ac", row_commune.get("Prix médian total €", None))
    prix_n1  = row_commune.get("Prix median N-1 \u20ac/m\u00b2", row_commune.get("Prix médian N-1 €/m²", None))
    tx_n1    = row_commune.get("Transactions N-1", None)

    try:
        score = int(score) if score is not None else 0
    except (TypeError, ValueError):
        score = 0
    try:
        volume = int(volume) if volume is not None else 0
    except (TypeError, ValueError):
        volume = 0

    # Indice de confiance
    conf_label, conf_color, conf_expl = _indice_confiance(volume)

    # Stats par type si le DataFrame est disponible
    df = df_transactions.copy() if df_transactions is not None and not df_transactions.empty else pd.DataFrame()
    stats_appt = {}
    stats_maison = {}
    if not df.empty:
        for col_type in ["type_local", "type_bien"]:
            if col_type in df.columns:
                type_col = col_type
                break
        else:
            type_col = None

        if type_col:
            for typ, label, target in [("Appartement", "appt", stats_appt), ("Maison", "maison", stats_maison)]:
                sub = df[df[type_col] == typ]
                if not sub.empty:
                    target["nb"] = len(sub)
                    if "prix_m2" in sub.columns:
                        target["prix_med"] = sub["prix_m2"].median()
                        target["prix_min"] = sub["prix_m2"].quantile(0.1)
                        target["prix_max"] = sub["prix_m2"].quantile(0.9)
                    if "surface_utile" in sub.columns:
                        target["surf_med"] = sub["surface_utile"].median()
                    elif "surface_reelle_bati" in sub.columns:
                        target["surf_med"] = sub["surface_reelle_bati"].median()
                    if "valeur_fonciere" in sub.columns:
                        target["val_med"] = sub["valeur_fonciere"].median()

    # Stats département global
    dept_prix_med = None
    if not df.empty and "prix_m2" in df.columns:
        dept_prix_med = df["prix_m2"].median()

    # Distribution par tranches de prix
    tranches = {}
    if not df.empty and "prix_m2" in df.columns:
        bins = [0, 1500, 2500, 3500, 5000, 10000, float("inf")]
        labels_tr = ["< 1 500", "1 500-2 500", "2 500-3 500", "3 500-5 000", "5 000-10 000", "> 10 000"]
        df["tranche_prix"] = pd.cut(df["prix_m2"], bins=bins, labels=labels_tr, right=False)
        tranches = df["tranche_prix"].value_counts().sort_index().to_dict()

    story = []

    # ══════════════════════════════════════════════════════════════════════
    # PAGE 1 : COUVERTURE
    # ══════════════════════════════════════════════════════════════════════

    cover_data = [[""]]
    cover_t = Table(cover_data, colWidths=[USABLE_W], rowHeights=[PAGE_H - 2 * MARGIN])
    cover_t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), DARK),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))

    # Build cover content
    cover_content = []

    # Logo area
    cover_content.append(Spacer(1, 60*mm))

    # Title
    cover_content.append(Paragraph("Rapport d'analyse", ParagraphStyle(
        "ct1", fontName="Poppins", fontSize=14,
        textColor=SAHAR_GREEN, alignment=TA_CENTER, leading=18
    )))
    cover_content.append(Spacer(1, 3*mm))
    cover_content.append(Paragraph("de marche immobilier", ParagraphStyle(
        "ct2", fontName="Poppins", fontSize=14,
        textColor=SAHAR_GREEN, alignment=TA_CENTER, leading=18
    )))
    cover_content.append(Spacer(1, 8*mm))
    cover_content.append(Paragraph(commune.upper(), ParagraphStyle(
        "ct3", fontName="Poppins-Bold", fontSize=32,
        textColor=BLANC, alignment=TA_CENTER, leading=38
    )))
    cover_content.append(Spacer(1, 3*mm))
    cover_content.append(Paragraph(f"Departement {dept}", ParagraphStyle(
        "ct4", fontName="Poppins", fontSize=14,
        textColor=colors.HexColor("#A0A0A0"), alignment=TA_CENTER, leading=18
    )))
    cover_content.append(Spacer(1, 15*mm))

    # Score badge on cover
    score_bg = _color_score(score)
    cover_content.append(Paragraph(f"Score marche : {score}/100", ParagraphStyle(
        "ct5", fontName="Poppins-Bold", fontSize=16,
        textColor=BLANC, alignment=TA_CENTER, leading=20
    )))
    cover_content.append(Spacer(1, 3*mm))
    cover_content.append(Paragraph(signal, ParagraphStyle(
        "ct6", fontName="Poppins", fontSize=12,
        textColor=colors.HexColor("#A0A0A0"), alignment=TA_CENTER, leading=16
    )))
    cover_content.append(Spacer(1, 30*mm))

    # Footer
    cover_content.append(HRFlowable(width=USABLE_W * 0.6, thickness=0.5,
                                     color=SAHAR_GREEN, spaceAfter=6))
    cover_content.append(Paragraph("SAHAR Conseil", ParagraphStyle(
        "ct7", fontName="Poppins-Bold", fontSize=12,
        textColor=BLANC, alignment=TA_CENTER, leading=15
    )))
    cover_content.append(Paragraph("sahar-conseil.fr", ParagraphStyle(
        "ct8", fontName="Poppins", fontSize=9,
        textColor=SAHAR_GREEN, alignment=TA_CENTER, leading=12
    )))
    cover_content.append(Spacer(1, 4*mm))
    cover_content.append(Paragraph(f"Rapport genere le {date_str}", ParagraphStyle(
        "ct9", fontName="Poppins", fontSize=8,
        textColor=colors.HexColor("#707070"), alignment=TA_CENTER, leading=11
    )))

    # Wrap cover in a single cell table for background
    inner_t = Table([[c] for c in cover_content], colWidths=[USABLE_W])
    inner_t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), DARK),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    story.append(inner_t)
    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # PAGE 2 : SYNTHESE EXECUTIVE
    # ══════════════════════════════════════════════════════════════════════

    # Header bar
    hdr = Table([[Paragraph(
        f"<b>Synthese executive</b>  |  {commune} — Dept. {dept}", S["h1"]
    )]], colWidths=[USABLE_W])
    hdr.setStyle(TableStyle([
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(hdr)
    story.append(HRFlowable(width=USABLE_W, thickness=1.5, color=SAHAR_GREEN, spaceAfter=6))

    # KPI Cards row
    kpi_items = [
        (f"{score}/100", "Score marche", _color_score(score)),
        (_fmt(prix_med, "{:,.0f} EUR"), "Prix median EUR/m2", SAHAR_GREEN_D),
        (_pct(evol), "Evolution 12 mois", VERT if evol and evol > 0 else ROUGE if evol and evol < 0 else GRIS_B),
        (str(volume), "Transactions 12m", SAHAR_GREEN_D),
    ]

    kpi_cells = []
    for val, label, col in kpi_items:
        cell_content = Table([
            [Paragraph(val, ParagraphStyle("kv", fontName="Poppins-Bold",
                                            fontSize=18, textColor=col,
                                            alignment=TA_CENTER, leading=22))],
            [Paragraph(label, S["kpi_label"])],
        ], colWidths=[USABLE_W / 4 - 6*mm])
        cell_content.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), GRIS_F),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]))
        kpi_cells.append(cell_content)

    kpi_row = Table([kpi_cells], colWidths=[USABLE_W / 4] * 4)
    kpi_row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 2),
        ("RIGHTPADDING", (0, 0), (-1, -1), 2),
    ]))
    story.append(kpi_row)
    story.append(Spacer(1, 5*mm))

    # Indice de confiance
    conf_badge = Table([[
        Paragraph(f"<b>Indice de fiabilite :</b> {conf_label}", ParagraphStyle(
            "conf", fontName="Poppins", fontSize=9, textColor=conf_color, leading=12
        )),
        Paragraph(conf_expl, S["small"]),
    ]], colWidths=[USABLE_W * 0.35, USABLE_W * 0.65])
    conf_badge.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), GRIS_F),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(conf_badge)
    story.append(Spacer(1, 5*mm))

    # Signal marché + contexte
    signal_color = _color_signal(signal)
    signal_bg = VERT_C if "acheteur" in signal.lower() or "Opportunit" in signal else ORANGE_C if "quilibr" in signal.lower() else ROUGE_C

    signal_row = Table([[
        Paragraph(f"<b>Signal de marche :</b> {signal}", ParagraphStyle(
            "sig", fontName="Poppins-Bold", fontSize=10, textColor=signal_color, leading=14
        )),
    ]], colWidths=[USABLE_W])
    signal_row.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), signal_bg),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
    ]))
    story.append(signal_row)
    story.append(Spacer(1, 6*mm))

    # Données clés supplémentaires
    story.append(Paragraph("Donnees cles complementaires", S["h3"]))
    ratio_str = _fmt(ratio_t, "{:.2f}")
    extra_rows = [
        ["Indicateur", "Valeur", "Commentaire"],
        ["Surface mediane", _fmt(surf_med, "{:.0f} m2"), "Surface typique des biens vendus"],
        ["Prix median global", _fmt(val_med, "{:,.0f} EUR"), "Prix total median des biens"],
        ["Ratio tension", ratio_str, "Volume recent / N-1 (> 1 = dynamique)"],
        ["Part departement", f"{volume / nb_total_dept * 100:.1f}%" if nb_total_dept else "—",
         f"Sur {nb_total_dept:,} transactions dept." if nb_total_dept else ""],
        ["Transactions N-1", _fmt(tx_n1, "{:.0f}"), "Volume sur la periode precedente"],
        ["Prix N-1 EUR/m2", _fmt(prix_n1, "{:,.0f} EUR"), "Prix median de la periode precedente"],
    ]

    t_extra = Table(extra_rows, colWidths=[USABLE_W * 0.30, USABLE_W * 0.25, USABLE_W * 0.45], repeatRows=1)
    t_extra.setStyle(TableStyle([
        ("BACKGROUND",   (0, 0), (-1, 0), DARK_3),
        ("TEXTCOLOR",    (0, 0), (-1, 0), BLANC),
        ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, -1), 8),
        ("TOPPADDING",   (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 5),
        ("LEFTPADDING",  (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BLANC, GRIS_F]),
        ("GRID",         (0, 0), (-1, -1), 0.3, GRIS_M),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(t_extra)
    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # PAGE 3 : ANALYSE PAR TYPE DE BIEN
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph(f"Analyse par type de bien  |  {commune}", S["h1"]))
    story.append(HRFlowable(width=USABLE_W, thickness=1.5, color=SAHAR_GREEN, spaceAfter=6))

    if stats_appt or stats_maison:
        # Two columns layout
        type_rows = [["", "Appartements", "Maisons"]]

        metrics = [
            ("Nombre de ventes", "nb", "{:.0f}"),
            ("Prix median EUR/m2", "prix_med", "{:,.0f} EUR"),
            ("Fourchette basse (P10)", "prix_min", "{:,.0f} EUR"),
            ("Fourchette haute (P90)", "prix_max", "{:,.0f} EUR"),
            ("Surface mediane", "surf_med", "{:.0f} m2"),
            ("Prix total median", "val_med", "{:,.0f} EUR"),
        ]

        for label, key, fmt in metrics:
            va = stats_appt.get(key)
            vm = stats_maison.get(key)
            type_rows.append([label, _fmt(va, fmt), _fmt(vm, fmt)])

        t_type = Table(type_rows, colWidths=[USABLE_W * 0.40, USABLE_W * 0.30, USABLE_W * 0.30], repeatRows=1)
        t_type.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), DARK_3),
            ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
            ("GRID",          (0, 0), (-1, -1), 0.3, GRIS_M),
            ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            ("FONTNAME",      (0, 1), (0, -1), "Helvetica"),
        ]))
        story.append(t_type)
        story.append(Spacer(1, 6*mm))

        # Interpretation
        story.append(Paragraph("Interpretation", S["h3"]))
        if stats_appt.get("prix_med") and stats_maison.get("prix_med"):
            diff = stats_appt["prix_med"] - stats_maison["prix_med"]
            if diff > 0:
                story.append(Paragraph(
                    f"Les appartements affichent un prix median au m2 superieur de "
                    f"<b>{diff:,.0f} EUR</b> par rapport aux maisons, ce qui est typique "
                    f"des zones urbaines denses ou le foncier est rare.",
                    S["body"]
                ))
            else:
                story.append(Paragraph(
                    f"Les maisons affichent un prix median au m2 superieur de "
                    f"<b>{abs(diff):,.0f} EUR</b> par rapport aux appartements, "
                    f"ce qui peut indiquer un marche periurbain ou rural valorise.",
                    S["body"]
                ))
        story.append(Spacer(1, 4*mm))

        # Fourchettes de prix (encarts visuels)
        for typ, stats, bg_c in [("Appartements", stats_appt, BLEU_C), ("Maisons", stats_maison, VERT_C)]:
            if stats.get("prix_min") and stats.get("prix_max"):
                fourch = Table([[
                    Paragraph(f"<b>{typ}</b>", S["body_bold"]),
                    Paragraph(
                        f"Fourchette de prix : <b>{stats['prix_min']:,.0f} EUR</b> "
                        f"a <b>{stats['prix_max']:,.0f} EUR</b> /m2  "
                        f"(mediane : <b>{stats.get('prix_med', 0):,.0f} EUR</b>)",
                        S["body"]
                    ),
                ]], colWidths=[USABLE_W * 0.20, USABLE_W * 0.80])
                fourch.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, -1), bg_c),
                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                    ("LEFTPADDING", (0, 0), (-1, -1), 10),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]))
                story.append(fourch)
                story.append(Spacer(1, 2*mm))

    else:
        story.append(Paragraph(
            "Donnees insuffisantes pour une analyse detaillee par type de bien.",
            S["body"]
        ))

    story.append(Spacer(1, 4*mm))

    # Distribution par tranches de prix
    if tranches:
        story.append(Paragraph("Distribution par tranche de prix EUR/m2", S["h3"]))
        tr_rows = [["Tranche EUR/m2", "Nb transactions", "Part (%)"]]
        total_tr = sum(tranches.values())
        for tranche, count in tranches.items():
            pct = count / total_tr * 100 if total_tr else 0
            tr_rows.append([str(tranche), str(count), f"{pct:.1f}%"])

        t_tr = Table(tr_rows, colWidths=[USABLE_W * 0.40, USABLE_W * 0.30, USABLE_W * 0.30], repeatRows=1)
        t_tr.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), DARK_3),
            ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
            ("GRID",          (0, 0), (-1, -1), 0.3, GRIS_M),
            ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
        ]))
        story.append(t_tr)

    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # PAGE 4 : TRANSACTIONS COMPARABLES
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph(f"Transactions comparables  |  {commune}", S["h1"]))
    story.append(HRFlowable(width=USABLE_W, thickness=1.5, color=SAHAR_GREEN, spaceAfter=4))
    story.append(Paragraph(
        f"Selection des transactions les plus recentes enregistrees a {commune}. "
        "Source : DVF (Demandes de Valeurs Foncieres) — data.gouv.fr.",
        S["caption"]
    ))
    story.append(Spacer(1, 3*mm))

    if not df.empty:
        # Sort by date, take last 15
        date_col = None
        for c in ["date_mutation", "date_vente"]:
            if c in df.columns:
                date_col = c
                break

        if date_col:
            df_sorted = df.sort_values(date_col, ascending=False).head(15)
        else:
            df_sorted = df.head(15)

        # Build table
        tx_rows = [["Date", "Adresse", "Type", "Surface", "Prix total", "EUR/m2"]]
        for _, r in df_sorted.iterrows():
            dt = ""
            if date_col and pd.notna(r.get(date_col)):
                try:
                    dt = pd.Timestamp(r[date_col]).strftime("%d/%m/%Y")
                except Exception:
                    dt = str(r[date_col])[:10]

            addr = str(r.get("adresse", "—"))[:35] if r.get("adresse") else "—"
            typ = str(r.get("type_local", r.get("type_bien", "—")))
            surf = _fmt(r.get("surface_utile", r.get("surface_reelle_bati")), "{:.0f} m2")
            prix_total = _fmt(r.get("valeur_fonciere"), "{:,.0f} EUR")
            pm2 = _fmt(r.get("prix_m2"), "{:,.0f}")

            tx_rows.append([dt, addr, typ, surf, prix_total, pm2])

        cw = [USABLE_W*0.10, USABLE_W*0.30, USABLE_W*0.12, USABLE_W*0.10, USABLE_W*0.20, USABLE_W*0.12]
        t_tx = Table(tx_rows, colWidths=cw, repeatRows=1)
        t_tx.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), DARK_3),
            ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 7.5),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 5),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 3),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
            ("GRID",          (0, 0), (-1, -1), 0.3, GRIS_M),
            ("ALIGN",         (3, 0), (-1, -1), "CENTER"),
            ("ALIGN",         (1, 1), (1, -1), "LEFT"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(t_tx)
        story.append(Spacer(1, 4*mm))

        # Stats résumé des comparables
        story.append(Paragraph("Statistiques des comparables", S["h3"]))
        if "prix_m2" in df.columns:
            comp_stats = [
                ["Statistique", "Valeur EUR/m2"],
                ["Minimum", f"{df['prix_m2'].min():,.0f} EUR"],
                ["1er quartile (Q1)", f"{df['prix_m2'].quantile(0.25):,.0f} EUR"],
                ["Mediane", f"{df['prix_m2'].median():,.0f} EUR"],
                ["Moyenne", f"{df['prix_m2'].mean():,.0f} EUR"],
                ["3e quartile (Q3)", f"{df['prix_m2'].quantile(0.75):,.0f} EUR"],
                ["Maximum", f"{df['prix_m2'].max():,.0f} EUR"],
                ["Ecart-type", f"{df['prix_m2'].std():,.0f} EUR"],
            ]
            t_cs = Table(comp_stats, colWidths=[USABLE_W * 0.50, USABLE_W * 0.50], repeatRows=1)
            t_cs.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0), DARK_3),
                ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
                ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
                ("TOPPADDING",    (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING",   (0, 0), (-1, -1), 8),
                ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
                ("GRID",          (0, 0), (-1, -1), 0.3, GRIS_M),
                ("ALIGN",         (1, 1), (1, -1), "CENTER"),
            ]))
            story.append(t_cs)
    else:
        story.append(Paragraph("Aucune transaction disponible pour cette commune.", S["body"]))

    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # PAGE 5 : ESTIMATION DE PRIX — INTERVALLE DE CONFIANCE
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph(f"Estimation de prix  |  {commune}", S["h1"]))
    story.append(HRFlowable(width=USABLE_W, thickness=1.5, color=SAHAR_GREEN, spaceAfter=4))
    story.append(Paragraph(
        "Estimation basee sur les transactions comparables avec intervalles de confiance "
        "a differents niveaux de risque. L'ecart-type mesure la dispersion des prix — "
        "plus il est faible, plus le marche est homogene et l'estimation fiable.",
        S["caption"]
    ))
    story.append(Spacer(1, 4*mm))

    if not df.empty and "prix_m2" in df.columns:
        median_prix = df["prix_m2"].median()
        mean_prix = df["prix_m2"].mean()
        std_prix = df["prix_m2"].std()
        n_comp = len(df)

        # Valeur de référence
        ref_prix = median_prix  # on prend la médiane (plus robuste)

        # Surface de référence pour l'estimation en valeur totale
        ref_surface = surf_med if surf_med and not (isinstance(surf_med, float) and math.isnan(surf_med)) else 70

        # Tableau estimation EUR/m2
        story.append(Paragraph("Estimation du prix au m2", S["h3"]))

        est_rows = [
            ["Scenario", "EUR/m2", "Ecart", "Intervalle", "Niveau de risque"],
            [
                "Estimation basse (-2 sigma)",
                f"{max(ref_prix - 2*std_prix, 0):,.0f} EUR",
                f"-{2*std_prix:,.0f}",
                "95% des biens au-dessus",
                "Tres prudent"
            ],
            [
                "Estimation prudente (-1 sigma)",
                f"{max(ref_prix - std_prix, 0):,.0f} EUR",
                f"-{std_prix:,.0f}",
                "84% des biens au-dessus",
                "Prudent"
            ],
            [
                "Estimation mediane",
                f"{ref_prix:,.0f} EUR",
                "Ref.",
                "Valeur centrale",
                "Equilibre"
            ],
            [
                "Estimation optimiste (+1 sigma)",
                f"{ref_prix + std_prix:,.0f} EUR",
                f"+{std_prix:,.0f}",
                "84% des biens en dessous",
                "Optimiste"
            ],
            [
                "Estimation haute (+2 sigma)",
                f"{ref_prix + 2*std_prix:,.0f} EUR",
                f"+{2*std_prix:,.0f}",
                "95% des biens en dessous",
                "Tres optimiste"
            ],
        ]

        cw_est = [USABLE_W*0.28, USABLE_W*0.18, USABLE_W*0.14, USABLE_W*0.22, USABLE_W*0.18]
        t_est = Table(est_rows, colWidths=cw_est, repeatRows=1)
        t_est.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), DARK_3),
            ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 7.5),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
            ("GRID",          (0, 0), (-1, -1), 0.3, GRIS_M),
            ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
            # Highlight median row
            ("BACKGROUND",    (0, 3), (-1, 3), BLEU_C),
            ("FONTNAME",      (0, 3), (-1, 3), "Helvetica-Bold"),
        ]))
        story.append(t_est)
        story.append(Spacer(1, 5*mm))

        # Estimation en valeur totale pour une surface de référence
        story.append(Paragraph(f"Estimation pour un bien de {ref_surface:.0f} m2 (surface mediane)", S["h3"]))

        val_rows = [
            ["Scenario", "Prix total estime", "Fourchette"],
            [
                "Fourchette basse (-1 sigma)",
                f"{max(ref_prix - std_prix, 0) * ref_surface:,.0f} EUR",
                f"Prix prudent"
            ],
            [
                "Estimation centrale (mediane)",
                f"{ref_prix * ref_surface:,.0f} EUR",
                f"Prix de marche"
            ],
            [
                "Fourchette haute (+1 sigma)",
                f"{(ref_prix + std_prix) * ref_surface:,.0f} EUR",
                f"Prix optimiste"
            ],
        ]

        t_val = Table(val_rows, colWidths=[USABLE_W*0.35, USABLE_W*0.35, USABLE_W*0.30], repeatRows=1)
        t_val.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), DARK_3),
            ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8.5),
            ("TOPPADDING",    (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
            ("GRID",          (0, 0), (-1, -1), 0.3, GRIS_M),
            ("ALIGN",         (1, 0), (-1, -1), "CENTER"),
            ("BACKGROUND",    (0, 2), (-1, 2), BLEU_C),
            ("FONTNAME",      (0, 2), (-1, 2), "Helvetica-Bold"),
        ]))
        story.append(t_val)
        story.append(Spacer(1, 5*mm))

        # Indicateurs statistiques de dispersion
        story.append(Paragraph("Indicateurs de dispersion", S["h3"]))

        cv = (std_prix / mean_prix * 100) if mean_prix else 0
        iqr = df["prix_m2"].quantile(0.75) - df["prix_m2"].quantile(0.25)
        homogeneite = "Tres homogene" if cv < 10 else "Homogene" if cv < 20 else "Heterogene" if cv < 35 else "Tres heterogene"
        homo_color = VERT if cv < 10 else VERT if cv < 20 else ORANGE if cv < 35 else ROUGE

        disp_rows = [
            ["Indicateur", "Valeur", "Interpretation"],
            ["Ecart-type (sigma)", f"{std_prix:,.0f} EUR/m2", f"Dispersion autour de la moyenne"],
            ["Coefficient de variation", f"{cv:.1f}%", homogeneite],
            ["Ecart interquartile (IQR)", f"{iqr:,.0f} EUR/m2", "50% central des transactions"],
            ["Nb comparables", str(n_comp), conf_label],
        ]

        t_disp = Table(disp_rows, colWidths=[USABLE_W*0.35, USABLE_W*0.30, USABLE_W*0.35], repeatRows=1)
        t_disp.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), DARK_3),
            ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 8),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
            ("GRID",          (0, 0), (-1, -1), 0.3, GRIS_M),
            ("ALIGN",         (1, 0), (1, -1), "CENTER"),
        ]))
        story.append(t_disp)
        story.append(Spacer(1, 5*mm))

        # Recommandation prix personnalisée
        story.append(Paragraph("Recommandation de prix", S["h3"]))

        if cv < 15:
            reco_prix = (
                f"Le marche de {commune} est <b>homogene</b> (CV = {cv:.1f}%). "
                f"L'estimation mediane de <b>{ref_prix:,.0f} EUR/m2</b> est fiable. "
                f"Fourchette recommandee : <b>{max(ref_prix - std_prix, 0):,.0f}</b> a "
                f"<b>{ref_prix + std_prix:,.0f} EUR/m2</b> (68% de confiance)."
            )
            reco_bg = VERT_C
        elif cv < 30:
            reco_prix = (
                f"Le marche de {commune} est <b>moderement disperse</b> (CV = {cv:.1f}%). "
                f"L'estimation mediane de <b>{ref_prix:,.0f} EUR/m2</b> est une bonne reference "
                f"mais les ecarts sont significatifs. "
                f"Fourchette recommandee : <b>{max(ref_prix - std_prix, 0):,.0f}</b> a "
                f"<b>{ref_prix + std_prix:,.0f} EUR/m2</b>. "
                f"Privilegiez l'analyse des comparables les plus proches du bien vise."
            )
            reco_bg = ORANGE_C
        else:
            reco_prix = (
                f"Le marche de {commune} est <b>tres heterogene</b> (CV = {cv:.1f}%). "
                f"L'estimation mediane ({ref_prix:,.0f} EUR/m2) doit etre interpretee avec prudence. "
                f"La fourchette large ({max(ref_prix - 2*std_prix, 0):,.0f} a {ref_prix + 2*std_prix:,.0f} EUR/m2) "
                f"reflete des biens tres differents. Il est essentiel de comparer uniquement "
                f"avec des biens de caracteristiques similaires (type, surface, etat)."
            )
            reco_bg = ROUGE_C

        reco_box = Table([[Paragraph(reco_prix, S["reco_body"])]], colWidths=[USABLE_W])
        reco_box.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), reco_bg),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("LEFTPADDING", (0, 0), (-1, -1), 12),
            ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ]))
        story.append(reco_box)

    else:
        story.append(Paragraph(
            "Donnees insuffisantes pour produire une estimation statistique.",
            S["body"]
        ))

    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # PAGE 6 : RECOMMANDATIONS STRATEGIQUES
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph(f"Recommandations strategiques  |  {commune}", S["h1"]))
    story.append(HRFlowable(width=USABLE_W, thickness=1.5, color=SAHAR_GREEN, spaceAfter=6))

    # Score jauge
    gauge = _draw_score_gauge(score, 100)
    gauge_table = Table([[gauge, Paragraph(
        f"Score de marche : <b>{score}/100</b><br/>"
        f"Signal : <b>{signal}</b><br/>"
        f"Fiabilite : <b>{conf_label}</b>",
        S["body"]
    )]], colWidths=[120, USABLE_W - 120])
    gauge_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(gauge_table)
    story.append(Spacer(1, 6*mm))

    # Recommandations selon le profil
    profiles = [
        ("Pour les acheteurs", _reco_acheteur(score, signal, evol, prix_med)),
        ("Pour les vendeurs", _reco_vendeur(score, signal, evol, prix_med)),
        ("Pour les investisseurs", _reco_investisseur(score, signal, evol, volume, ratio_t)),
        ("Pour les agents immobiliers", _reco_agent(score, signal, volume, commune)),
    ]

    for title, recos in profiles:
        reco_header = Table([[Paragraph(title, S["reco_title"])]], colWidths=[USABLE_W])
        reco_header.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), DARK_3),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ]))
        story.append(reco_header)

        reco_content = Table([[Paragraph(recos, S["reco_body"])]], colWidths=[USABLE_W])
        reco_content.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), GRIS_F),
            ("TOPPADDING", (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ]))
        story.append(reco_content)
        story.append(Spacer(1, 3*mm))

    story.append(PageBreak())

    # ══════════════════════════════════════════════════════════════════════
    # PAGE 6 : METHODOLOGIE & MENTIONS LEGALES
    # ══════════════════════════════════════════════════════════════════════

    story.append(Paragraph("Methodologie et mentions legales", S["h1"]))
    story.append(HRFlowable(width=USABLE_W, thickness=1.5, color=SAHAR_GREEN, spaceAfter=6))

    story.append(Paragraph("Sources de donnees", S["h3"]))
    story.append(Paragraph(
        "<b>DVF (Demandes de Valeurs Foncieres)</b> — Base de donnees officielle publiee par "
        "la Direction Generale des Finances Publiques (DGFiP) sur data.gouv.fr. "
        "Elle recense l'ensemble des transactions immobilieres realisees en France "
        "depuis 2014, hors Alsace-Moselle et Mayotte.",
        S["body"]
    ))
    story.append(Spacer(1, 3*mm))

    story.append(Paragraph("Calcul du score de marche", S["h3"]))
    story.append(Paragraph(
        "Le score marche (0-100) est un indicateur composite calcule par SAHAR Conseil "
        "a partir de trois dimensions ponderes :<br/>"
        "- <b>Volume de transactions</b> (40%) : nombre de ventes sur 12 mois, normalise par rapport au departement<br/>"
        "- <b>Evolution des prix</b> (30%) : variation du prix median EUR/m2 par rapport a la periode precedente<br/>"
        "- <b>Ratio de tension</b> (30%) : rapport entre le volume recent et le volume N-1 "
        "(un ratio > 1 indique une acceleration du marche)",
        S["body"]
    ))
    story.append(Spacer(1, 3*mm))

    story.append(Paragraph("Indice de fiabilite", S["h3"]))
    story.append(Paragraph(
        "L'indice de fiabilite est directement lie au nombre de transactions comparables "
        "disponibles. Plus le nombre est eleve, plus l'estimation est fiable :<br/>"
        "- <b>Fiabilite elevee</b> : 30+ transactions (fourchette de prix resserree)<br/>"
        "- <b>Fiabilite moyenne</b> : 10 a 30 transactions<br/>"
        "- <b>Fiabilite faible</b> : moins de 10 transactions (fourchette large, prudence conseillee)",
        S["body"]
    ))
    story.append(Spacer(1, 3*mm))

    story.append(Paragraph("Filtres appliques", S["h3"]))
    story.append(Paragraph(
        "Seules les transactions concernant des maisons et appartements sont retenues. "
        "Les ventes avec surface inferieure a 5 m2, prix aberrants (< 200 EUR/m2 ou > 30 000 EUR/m2), "
        "et les transactions atypiques (dependances, garages, parkings) sont exclues du calcul.",
        S["body"]
    ))
    story.append(Spacer(1, 5*mm))

    story.append(Paragraph("Avertissement legal", S["h3"]))
    legal_box = Table([[Paragraph(
        "Ce rapport est fourni a titre indicatif et ne constitue en aucun cas une expertise "
        "immobiliere au sens de la Charte de l'Expertise en Evaluation Immobiliere. "
        "Les prix et indicateurs presentes sont issus de l'analyse statistique de donnees "
        "publiques et ne prennent pas en compte les specificites individuelles des biens "
        "(etat, vue, etage, travaux, DPE...). Pour une evaluation precise, "
        "il est recommande de consulter un expert immobilier agree ou un notaire.<br/><br/>"
        "<b>SAHAR Conseil</b> ne saurait etre tenu responsable de toute decision prise "
        "sur la base des informations contenues dans ce rapport.",
        S["body"]
    )]], colWidths=[USABLE_W])
    legal_box.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), ROUGE_C),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
    ]))
    story.append(legal_box)
    story.append(Spacer(1, 8*mm))

    # Final footer
    footer_data = [[
        Paragraph(
            f"SAHAR Conseil — sahar-conseil.fr  |  contact@sahar-conseil.fr  |  "
            f"Rapport genere le {date_str}  |  Donnees DVF data.gouv.fr  |  "
            f"Ref. SAHAR-{dept}-{commune[:4].upper()}-{date_fichier}",
            S["footer"]
        )
    ]]
    ft = Table(footer_data, colWidths=[USABLE_W])
    ft.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), GRIS_F),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
    ]))
    story.append(ft)

    # Build PDF
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=15*mm, bottomMargin=18*mm,
        title=f"SAHAR — Analyse marche {commune}",
        author="SAHAR Conseil",
        subject=f"Analyse immobiliere {commune} — Dept. {dept}",
    )
    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


# ── Recommandations contextuelles ────────────────────────────────────────────

def _reco_acheteur(score, signal, evol, prix_med):
    if score >= 70:
        return (
            f"Le marche de cette commune est tres dynamique (score {score}/100). "
            f"Les prix evoluent a un rythme soutenu ({_pct(evol)}) et les biens se vendent rapidement. "
            "Il est recommande de se positionner rapidement sur les biens correspondant a vos criteres "
            "et de preparer votre financement en amont pour etre reactif. "
            "Privilegiez les offres au prix pour maximiser vos chances."
        )
    elif score >= 40:
        return (
            f"Le marche est equilibre (score {score}/100), offrant des opportunites de negociation. "
            f"Avec un prix median de {_fmt(prix_med, '{:,.0f} EUR')}/m2, "
            "vous disposez d'une marge de negociation estimee entre 3% et 8% du prix affiche. "
            "Prenez le temps de comparer les biens et n'hesitez pas a faire des offres inferieures "
            "au prix demande."
        )
    else:
        return (
            f"Le marche est favorable aux acheteurs (score {score}/100). "
            "Le volume de transactions est faible et les prix sont sous pression. "
            "C'est un bon moment pour negocier fermement — des decotes de 8% a 15% "
            "sont envisageables sur les biens en vente depuis plus de 3 mois. "
            "Attention cependant a la liquidite : un bien achete dans un marche atone "
            "pourrait etre difficile a revendre rapidement."
        )


def _reco_vendeur(score, signal, evol, prix_med):
    if score >= 70:
        return (
            f"Conditions tres favorables pour vendre (score {score}/100). "
            f"Le marche est dynamique avec une evolution des prix de {_pct(evol)}. "
            "Positionnez votre bien au prix du marche — les acquéreurs sont nombreux. "
            "Un prix legerement au-dessus du median est possible si le bien presente "
            "des atouts (vue, renovation recente, DPE favorable). "
            "Mettez en avant la valorisation recente du secteur."
        )
    elif score >= 40:
        return (
            f"Marche equilibre (score {score}/100) — la vente est possible "
            "mais necessite un prix juste et un bien bien presente. "
            f"Basez-vous sur le prix median de {_fmt(prix_med, '{:,.0f} EUR')}/m2 "
            "et ajustez selon les specificites de votre bien. "
            "Investissez dans la presentation (photos pro, home staging) "
            "pour vous demarquer des autres biens en vente."
        )
    else:
        return (
            f"Marche difficile pour les vendeurs (score {score}/100). "
            "Le volume de transactions est faible et les acheteurs sont en position de force. "
            "Il est recommande de fixer un prix attractif des le depart (5 a 10% sous le median) "
            "pour generer du trafic et des visites. "
            "Si la vente n'est pas urgente, envisagez de reporter pour attendre "
            "de meilleures conditions de marche."
        )


def _reco_investisseur(score, signal, evol, volume, ratio_t):
    ratio_str = _fmt(ratio_t, "{:.2f}")
    if score >= 70:
        return (
            f"Zone tres dynamique (score {score}/100, ratio tension {ratio_str}). "
            "Le marche est porteur pour l'investissement locatif : "
            "forte demande, prix en hausse, liquidite elevee. "
            "Ciblez les petites surfaces (studios, T2) qui offrent les meilleurs rendements. "
            "Attention : les prix d'entree eleves peuvent comprimer le rendement brut. "
            "Calculez soigneusement votre cashflow previsionnel."
        )
    elif score >= 40:
        return (
            f"Zone a potentiel modere (score {score}/100). "
            f"Avec {volume} transactions sur 12 mois, la liquidite est correcte. "
            "Recherchez les biens sous-evalues ou necessitant des travaux "
            "pour creer de la valeur (strategie d'achat-renovation). "
            "Le rendement locatif peut etre interessant si le prix d'acquisition "
            "est negocie en dessous du median."
        )
    else:
        return (
            f"Zone en retrait (score {score}/100). "
            "L'investissement est plus risque — faible volume, liquidite limitee. "
            "Peut convenir pour une strategie patrimoniale a long terme "
            "avec un horizon > 10 ans, ou pour de la location saisonniere "
            "si la zone presente un interet touristique. "
            "Evitez l'investissement locatif classique sauf rendement brut > 8%."
        )


def _reco_agent(score, signal, volume, commune):
    if score >= 70:
        return (
            f"{commune} est une zone de prospection prioritaire (score {score}/100). "
            f"Avec {volume} transactions sur 12 mois, le flux de mandats potentiels est important. "
            "Concentrez votre prospection sur les proprietaires ayant achete il y a 5-8 ans "
            "(plus-value potentielle). "
            "Mettez en avant la dynamique du marche dans votre communication locale."
        )
    elif score >= 40:
        return (
            f"{commune} offre un potentiel de mandats raisonnable (score {score}/100). "
            f"{volume} transactions sur 12 mois. "
            "Differenciez-vous par la qualite de vos estimations "
            "et votre connaissance fine du marche local. "
            "Les proprietaires ont besoin d'etre rassures — "
            "presentez des comparables precis et a jour."
        )
    else:
        return (
            f"{commune} est une zone secondaire (score {score}/100). "
            "Le volume de mandats est limite. "
            "Privilegiez cette zone uniquement si vous avez une expertise locale forte "
            "ou un reseau de proprietaires etabli. "
            "Concentrez vos efforts de prospection sur les zones "
            "a plus fort potentiel du departement."
        )
