"""
ErgoWatch — Veille Stratégique de Marché (Maroc)
=================================================
Veille hebdomadaire des grands signaux économiques marocains :
investissements, nouveaux projets, réglementation RSE — tous secteurs
susceptibles de générer des opportunités ergonomie à moyen terme.

Ceci n'est PAS un scraper d'appels d'offres — c'est une veille de
tendances de marché, pour du développement commercial proactif.

Source : Google News RSS (gratuit, sans clé API, pas de blocage robots.txt)
Fréquence : hebdomadaire (voir .github/workflows/veille_marche.yml)
"""

import os
import time
import logging
import smtplib
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - INFO - %(message)s')
log = logging.getLogger(__name__)

EMAIL_USER = os.environ.get('EMAIL_USER', '')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD', '')
EMAIL_DESTINATAIRE = os.environ.get('EMAIL_DESTINATAIRE', '')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

# ─── Secteurs suivis — poids égal ──────────────────────────────────────
SECTEURS = {
    "🏭 OCP & Industrie minière": [
        "OCP investissement usine",
        "OCP Green Maroc",
    ],
    "⚓ Portuaire & Logistique": [
        "Tanger Med extension",
        "Nador West Med Maroc",
    ],
    "✈️ Aéronautique": [
        "aéronautique Maroc investissement usine",
    ],
    "🚗 Automobile": [
        "automobile Maroc investissement usine",
    ],
    "🏗️ BTP & Infrastructures": [
        "BTP Maroc grand projet chantier",
    ],
    "🏦 Banque & Finance": [
        "banque Maroc investissement RSE",
    ],
    "⚽ Coupe du Monde 2030": [
        "Coupe du Monde 2030 Maroc infrastructures stades",
    ],
    "📋 Réglementation & RSE": [
        "réglementation RSE Maroc entreprises",
        "santé sécurité travail Maroc loi",
    ],
}

# Mots qui indiquent un vrai signal d'opportunité (pas juste une mention)
MOTS_SIGNAL = [
    'investissement', 'investit', 'milliard', 'mmdh', 'projet',
    'extension', 'nouvelle usine', 'nouvelle unité', 'chantier',
    'construction', 'inauguration', 'recrutement', "création d'emplois",
    'réglementation', 'loi', 'décret', 'norme', 'obligatoire',
    'appel à projets', 'partenariat', 'accord',
]

JOURS_FENETRE = 8  # fenêtre de recherche (8j = couvre la semaine + marge)


def fetch_google_news(query: str, max_results: int = 8) -> list:
    """Interroge le flux RSS Google News (gratuit, pas de clé API)."""
    url = (
        "https://news.google.com/rss/search?q="
        + urllib.parse.quote(query)
        + "&hl=fr&gl=MA&ceid=MA:fr"
    )
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        items = []
        for item in root.findall('.//item')[:max_results]:
            titre = (item.findtext('title') or '').strip()
            lien = (item.findtext('link') or '').strip()
            date_str = (item.findtext('pubDate') or '').strip()
            source_elem = item.find('source')
            source = source_elem.text if source_elem is not None else ''
            items.append({'titre': titre, 'lien': lien, 'date_str': date_str, 'source': source})
        return items
    except Exception as e:
        log.warning(f"Erreur requête '{query}': {e}")
        return []


def parse_pubdate(date_str: str):
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z"):
        try:
            return datetime.strptime(date_str, fmt)
        except Exception:
            continue
    return None


def est_recent(date_str: str, jours: int = JOURS_FENETRE) -> bool:
    d = parse_pubdate(date_str)
    if not d:
        return True
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    limite = datetime.now(timezone.utc) - timedelta(days=jours)
    return d >= limite


def score_signal(titre: str) -> int:
    t = titre.lower()
    return sum(1 for mot in MOTS_SIGNAL if mot in t)


def collecter_secteur(requetes: list) -> list:
    vus = set()
    resultats = []
    for q in requetes:
        for item in fetch_google_news(q):
            if not item['titre'] or item['titre'] in vus:
                continue
            if not est_recent(item['date_str']):
                continue
            vus.add(item['titre'])
            item['score'] = score_signal(item['titre'])
            resultats.append(item)
        time.sleep(1)
    resultats.sort(key=lambda x: x['score'], reverse=True)
    return resultats[:5]  # top 5 par secteur pour un digest lisible


def construire_email(resultats_par_secteur: dict):
    lignes = []
    total = 0
    for secteur, items in resultats_par_secteur.items():
        if not items:
            continue
        lignes.append(f"\n{secteur}")
        lignes.append("─" * 50)
        for it in items:
            total += 1
            lignes.append(f"• {it['titre']}")
            lignes.append(f"  {it['source']} — {it['lien']}")
        lignes.append("")
    if total == 0:
        return None, 0
    corps = f"""Bonjour,

Voici votre synthèse hebdomadaire des signaux de marché au Maroc
(tous secteurs), susceptibles de générer des opportunités ergonomie
à moyen terme.
{chr(10).join(lignes)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Cette synthèse est générée automatiquement à partir de l'actualité
économique publique (Google News). Elle signale des tendances,
pas des appels d'offres formels.

Cordialement,
Votre robot ErgoWatch 🤖
Indigo Ergonomie Maroc
"""
    return corps, total


def envoyer_email(corps: str, nb_articles: int):
    if not all([EMAIL_USER, EMAIL_PASSWORD, EMAIL_DESTINATAIRE]):
        log.warning("Variables email manquantes → email non envoyé")
        return
    msg = MIMEMultipart()
    msg['Subject'] = f"📊 ErgoWatch — Veille marché hebdo ({nb_articles} signaux)"
    msg['From'] = f"ErgoWatch <{EMAIL_USER}>"
    msg['To'] = EMAIL_DESTINATAIRE
    msg.attach(MIMEText(corps, 'plain', 'utf-8'))
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASSWORD)
            smtp.send_message(msg)
        log.info(f"📧 Email envoyé : {nb_articles} signaux")
    except Exception as e:
        log.error(f"Erreur envoi email : {e}")


def main():
    log.info("=" * 55)
    log.info("🚀 ErgoWatch — Veille stratégique de marché")

    resultats_par_secteur = {}
    total = 0
    for secteur, requetes in SECTEURS.items():
        items = collecter_secteur(requetes)
        resultats_par_secteur[secteur] = items
        log.info(f"{secteur} → {len(items)} signaux retenus")
        total += len(items)

    log.info(f"📊 Total : {total} signaux cette semaine")

    corps, nb = construire_email(resultats_par_secteur)
    if corps:
        envoyer_email(corps, nb)
    else:
        log.info("📭 Aucun signal cette semaine → pas d'email envoyé")

    log.info("✅ Terminé")


if __name__ == "__main__":
    main()
