"""
SAHAR Conseil — Générateur de rapport PDF par commune
Produit un rapport A4 professionnel exportable depuis Streamlit.
"""

import io
from datetime import datetime

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT


# ── Palette SAHAR ────────────────────────────────────────────────────────────
BLEU     = colors.HexColor("#185FA5")
VERT     = colors.HexColor("#1D9E75")
ROUGE    = colors.HexColor("#E24B4A")
ORANGE   = colors.HexColor("#BA7517")
GRIS_F   = colors.HexColor("#F5F5F5")
GRIS_B   = colors.HexColor("#888888")
NOIR     = colors.HexColor("#1A1A1A")
BLANC    = colors.white


def _styles():
    base = getSampleStyleSheet()
    styles = {}

    styles["titre"] = ParagraphStyle(
        "titre", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=22,
        textColor=BLANC, leading=26, spaceAfter=4
    )
    styles["sous_titre"] = ParagraphStyle(
        "sous_titre", parent=base["Normal"],
        fontName="Helvetica", fontSize=11,
        textColor=BLANC, leading=14
    )
    styles["section"] = ParagraphStyle(
        "section", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=12,
        textColor=BLEU, leading=16, spaceBefore=10, spaceAfter=4
    )
    styles["body"] = ParagraphStyle(
        "body", parent=base["Normal"],
        fontName="Helvetica", fontSize=9,
        textColor=NOIR, leading=13
    )
    styles["caption"] = ParagraphStyle(
        "caption", parent=base["Normal"],
        fontName="Helvetica", fontSize=7.5,
        textColor=GRIS_B, leading=11
    )
    styles["kpi_val"] = ParagraphStyle(
        "kpi_val", parent=base["Normal"],
        fontName="Helvetica-Bold", fontSize=18,
        textColor=BLEU, alignment=TA_CENTER, leading=22
    )
    styles["kpi_label"] = ParagraphStyle(
        "kpi_label", parent=base["Normal"],
        fontName="Helvetica", fontSize=7.5,
        textColor=GRIS_B, alignment=TA_CENTER, leading=10
    )
    styles["footer"] = ParagraphStyle(
        "footer", parent=base["Normal"],
        fontName="Helvetica", fontSize=7,
        textColor=GRIS_B, alignment=TA_CENTER
    )
    return styles


def _couleur_signal(signal: str):
    if "vendeur" in signal.lower():
        return ROUGE
    if "équilibré" in signal.lower():
        return ORANGE
    return VERT


def _couleur_score(score: int):
    if score >= 70: return VERT
    if score >= 40: return ORANGE
    return ROUGE


def generer_rapport_commune(
    commune: str,
    dept: str,
    row_commune: dict,   # ligne du DataFrame scoring_commune
    df_transactions,     # DataFrame filtré sur la commune
    nb_total_dept: int,  # nb transactions total département
) -> bytes:
    """
    Génère un rapport PDF A4 pour une commune.
    Retourne les bytes du PDF (utilisable avec st.download_button).
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=18*mm, rightMargin=18*mm,
        topMargin=15*mm, bottomMargin=18*mm,
    )
    W = A4[0] - 36*mm  # largeur utile
    S = _styles()
    story = []

    # ── EN-TÊTE ──────────────────────────────────────────────────────────
    date_str = datetime.now().strftime("%d/%m/%Y")
    header_data = [[
        Paragraph(f"<b>Analyse de marché</b><br/>{commune} — Dép. {dept}", S["titre"]),
        Paragraph(f"SAHAR Conseil<br/>Rapport généré le {date_str}", S["sous_titre"])
    ]]
    header_table = Table(header_data, colWidths=[W * 0.65, W * 0.35])
    header_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BLEU),
        ("VALIGN",     (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING",   (0, 0), (-1, -1), 14),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 14),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 8*mm))

    # ── KPIs PRINCIPAUX ──────────────────────────────────────────────────
    score      = row_commune.get("Score marché", 0)
    prix_med   = row_commune.get("Prix médian €/m²", 0)
    evol       = row_commune.get("Évolution 12m (%)", None)
    volume     = row_commune.get("Transactions 12m", 0)
    signal     = row_commune.get("Signal marché", "—")
    ratio_t    = row_commune.get("Ratio tension", None)
    surf_med   = row_commune.get("Surface médiane m²", None)
    val_med    = row_commune.get("Prix médian total €", None)

    evol_str = f"{evol:+.1f}%" if evol is not None and str(evol) != "nan" else "N/D"
    ratio_str = f"{ratio_t:.2f}" if ratio_t is not None and str(ratio_t) != "nan" else "N/D"

    kpi_data = [[
        [Paragraph(f"{score}/100", S["kpi_val"]),
         Paragraph("Score marché", S["kpi_label"])],
        [Paragraph(f"{prix_med:,.0f} €", S["kpi_val"]),
         Paragraph("Prix médian €/m²", S["kpi_label"])],
        [Paragraph(evol_str, S["kpi_val"]),
         Paragraph("Évolution 12 mois", S["kpi_label"])],
        [Paragraph(str(int(volume)), S["kpi_val"]),
         Paragraph("Transactions 12m", S["kpi_label"])],
    ]]

    kpi_cells = []
    for cell in kpi_data[0]:
        t = Table([[cell[0]], [cell[1]]], colWidths=[W / 4 - 4*mm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), GRIS_F),
            ("TOPPADDING", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ("ROUNDEDCORNERS", [4]),
        ]))
        kpi_cells.append(t)

    kpi_row = Table([kpi_cells], colWidths=[W / 4] * 4, hAlign="CENTER")
    kpi_row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(KeepTogether([kpi_row]))
    story.append(Spacer(1, 5*mm))

    # ── SIGNAL MARCHÉ ────────────────────────────────────────────────────
    signal_color = _couleur_signal(signal)
    score_color  = _couleur_score(score)

    signal_data = [[
        Paragraph(f"<b>Signal :</b> {signal}", S["body"]),
        Paragraph(
            f"Score : <b>{score}/100</b> — "
            f"Ratio tension : <b>{ratio_str}</b> — "
            f"Surface médiane : <b>{surf_med:.0f} m²</b>" if surf_med else
            f"Score : <b>{score}/100</b> — Ratio tension : <b>{ratio_str}</b>",
            S["body"]
        ),
    ]]
    signal_table = Table(signal_data, colWidths=[W * 0.45, W * 0.55])
    signal_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), GRIS_F),
        ("TOPPADDING",    (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 10),
        ("LINEAFTER", (0, 0), (0, -1), 0.5, colors.lightgrey),
    ]))
    story.append(signal_table)
    story.append(Spacer(1, 6*mm))

    # ── ANALYSE DÉTAILLÉE ────────────────────────────────────────────────
    story.append(Paragraph("Analyse détaillée du marché", S["section"]))
    story.append(HRFlowable(width=W, thickness=0.5, color=BLEU, spaceAfter=4))

    prix_n1 = row_commune.get("Prix médian N-1 €/m²", None)
    tx_n1   = row_commune.get("Transactions N-1", None)

    analyse_rows = [
        ["Indicateur", "Valeur", "Période"],
        ["Prix médian €/m²", f"{prix_med:,.0f} €", "12 derniers mois"],
        ["Prix médian N-1 €/m²",
         f"{prix_n1:,.0f} €" if prix_n1 and str(prix_n1) != "nan" else "N/D",
         "12–24 mois"],
        ["Évolution prix", evol_str, "vs N-1"],
        ["Volume transactions", str(int(volume)), "12 derniers mois"],
        ["Volume N-1", str(int(tx_n1)) if tx_n1 and str(tx_n1) != "nan" else "N/D", "12–24 mois"],
        ["Ratio tension", ratio_str, "Volume récent / N-1"],
        ["Prix médian total", f"{val_med:,.0f} €" if val_med and str(val_med) != "nan" else "N/D", "12 derniers mois"],
        ["Surface médiane", f"{surf_med:.0f} m²" if surf_med and str(surf_med) != "nan" else "N/D", "12 derniers mois"],
        ["Part dép. transactions",
         f"{volume / nb_total_dept * 100:.1f}%" if nb_total_dept else "—",
         "12 derniers mois"],
    ]

    col_w = [W * 0.45, W * 0.30, W * 0.25]
    t = Table(analyse_rows, colWidths=col_w, repeatRows=1)
    t.setStyle(TableStyle([
        # En-tête
        ("BACKGROUND",   (0, 0), (-1, 0), BLEU),
        ("TEXTCOLOR",    (0, 0), (-1, 0), BLANC),
        ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",     (0, 0), (-1, 0), 8.5),
        ("TOPPADDING",   (0, 0), (-1, 0), 7),
        ("BOTTOMPADDING",(0, 0), (-1, 0), 7),
        # Corps
        ("FONTNAME",     (0, 1), (-1, -1), "Helvetica"),
        ("FONTSIZE",     (0, 1), (-1, -1), 8.5),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [BLANC, GRIS_F]),
        ("TOPPADDING",   (0, 1), (-1, -1), 5),
        ("BOTTOMPADDING",(0, 1), (-1, -1), 5),
        ("LEFTPADDING",  (0, 0), (-1, -1), 8),
        ("GRID",         (0, 0), (-1, -1), 0.3, colors.lightgrey),
        ("ALIGN",        (1, 1), (2, -1), "CENTER"),
        ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(t)
    story.append(Spacer(1, 6*mm))

    # ── TOP 10 TRANSACTIONS ──────────────────────────────────────────────
    if not df_transactions.empty:
        story.append(Paragraph("Dernières transactions enregistrées", S["section"]))
        story.append(HRFlowable(width=W, thickness=0.5, color=BLEU, spaceAfter=4))

        df_top = df_transactions.nlargest(10, "score")[[
            "score", "date_mutation", "adresse", "type_local",
            "surface_utile", "valeur_fonciere", "prix_m2"
        ]].copy()
        df_top["date_mutation"] = df_top["date_mutation"].dt.strftime("%d/%m/%Y")

        rows = [["Score", "Date", "Adresse", "Type", "m²", "Prix €", "€/m²"]]
        for _, r in df_top.iterrows():
            rows.append([
                str(int(r["score"])),
                str(r["date_mutation"]),
                str(r["adresse"])[:30] if r["adresse"] else "—",
                str(r["type_local"]),
                f"{r['surface_utile']:.0f}",
                f"{r['valeur_fonciere']:,.0f}",
                f"{r['prix_m2']:.0f}",
            ])

        cw = [W*0.07, W*0.10, W*0.30, W*0.12, W*0.08, W*0.17, W*0.10]
        tt = Table(rows, colWidths=cw, repeatRows=1)
        tt.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), BLEU),
            ("TEXTCOLOR",     (0, 0), (-1, 0), BLANC),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1, -1), 7.5),
            ("TOPPADDING",    (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 5),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [BLANC, GRIS_F]),
            ("GRID",          (0, 0), (-1, -1), 0.3, colors.lightgrey),
            ("ALIGN",         (0, 1), (-1, -1), "CENTER"),
            ("ALIGN",         (2, 1), (2, -1), "LEFT"),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(tt)
        story.append(Spacer(1, 5*mm))

    # ── NOTE MÉTHODOLOGIQUE ──────────────────────────────────────────────
    story.append(HRFlowable(width=W, thickness=0.3, color=colors.lightgrey))
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(
        "<b>Méthodologie :</b> Le score marché (0–100) est calculé à partir des données DVF "
        "(Demandes de Valeurs Foncières) publiées par la DGFiP sur data.gouv.fr. "
        "Il combine le volume de transactions récentes (40%), l'évolution des prix sur 12 mois (30%) "
        "et le ratio de tension offre/demande (30%). Seules les ventes de maisons et appartements "
        "avec surface > 5 m² et prix cohérents sont retenues.",
        S["caption"]
    ))
    story.append(Spacer(1, 3*mm))

    # ── FOOTER ───────────────────────────────────────────────────────────
    footer_data = [[
        Paragraph(
            f"SAHAR Conseil — sahar-conseil.fr  |  Données DVF data.gouv.fr  |  "
            f"Rapport généré le {date_str}  |  À titre indicatif uniquement",
            S["footer"]
        )
    ]]
    footer_t = Table(footer_data, colWidths=[W])
    footer_t.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), GRIS_F),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
    ]))
    story.append(footer_t)

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()
