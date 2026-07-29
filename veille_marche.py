"""
ErgoWatch — Veille Stratégique de Marché (Maroc) — v3
=======================================================================
Veille hebdomadaire des signaux économiques marocains, filtrée par
sources fiables puis qualifiée par Claude (Qui / Pourquoi / Comment / Quand).

Sources brutes : Google News RSS (agrégateur, gratuit, sans clé API)
Filtre qualité : liste blanche de presse économique reconnue (voir SOURCES_FIABLES)
Analyse : API Claude (Haiku)
Fréquence : hebdomadaire (voir .github/workflows/veille_marche.yml)
"""

import os
import json
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
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

SECTEURS = {
    "🏭 OCP & Industrie minière": ["OCP investissement usine", "OCP Green Maroc"],
    "⚓ Portuaire & Logistique": ["Tanger Med extension", "Nador West Med Maroc"],
    "✈️ Aéronautique": ["aéronautique Maroc investissement usine"],
    "🚗 Automobile": ["automobile Maroc investissement usine"],
    "🏗️ BTP & Infrastructures": ["BTP Maroc grand projet chantier"],
    "🏦 Banque & Finance": ["banque Maroc investissement RSE"],
    "⚽ Coupe du Monde 2030": ["Coupe du Monde 2030 Maroc infrastructures stades"],
    "📋 Réglementation & RSE": ["réglementation RSE Maroc entreprises", "santé sécurité travail Maroc loi"],
}

JOURS_FENETRE = 8

# ─── Sources de confiance ───────────────────────────────────────────
# Presse économique marocaine et panafricaine reconnue. Modifiez cette
# liste librement — ajoutez ou retirez des sources selon votre confiance.
SOURCES_FIABLES = [
    "médias24", "medias24",
    "la vie éco", "la vie eco",
    "l'economiste", "leconomiste",
    "challenge.ma", "challenge",
    "le matin", "lematin",
    "aujourd'hui le maroc",
    "les inspirations éco", "leseco",
    "telquel",
    "hespress",
    "maroc diplomatique",
    "agence ecofin", "ecofin",
    "l'opinion", "lopinion",
    "africa intelligence",
    "jeune afrique",
    "reuters",
    "bloomberg",
    "the north africa post",
    "map express", "map.ma", "maghreb arabe presse",
]


def source_fiable(source: str) -> bool:
    s = (source or '').lower()
    return any(ref in s for ref in SOURCES_FIABLES)


def fetch_google_news(query: str, max_results: int = 10) -> list:
    url = ("https://news.google.com/rss/search?q=" + urllib.parse.quote(query) + "&hl=fr&gl=MA&ceid=MA:fr")
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
    return d >= datetime.now(timezone.utc) - timedelta(days=jours)


def collecter_tout() -> list:
    """Collecte brute, filtrée par sources fiables, tous secteurs confondus."""
    vus = set()
    candidats = []
    rejetes_source = 0
    for secteur, requetes in SECTEURS.items():
        for q in requetes:
            for item in fetch_google_news(q):
                if not item['titre'] or item['titre'] in vus:
                    continue
                if not est_recent(item['date_str']):
                    continue
                if not source_fiable(item['source']):
                    rejetes_source += 1
                    continue
                vus.add(item['titre'])
                item['secteur'] = secteur
                candidats.append(item)
            time.sleep(1)
    log.info(f"📥 {len(candidats)} articles retenus (sources fiables) — {rejetes_source} écartés (source non reconnue)")
    return candidats


def analyser_avec_ia(candidats: list) -> list:
    if not candidats:
        return []
    if not ANTHROPIC_API_KEY:
        log.warning("ANTHROPIC_API_KEY manquante → analyse IA impossible")
        return []

    articles_texte = "\n".join(
        f"{i+1}. [{c['secteur']}] {c['titre']} (source: {c['source']})"
        for i, c in enumerate(candidats)
    )

    prompt = f"""Tu es analyste business pour Indigo Ergonomie Maroc, un cabinet
de conseil en ergonomie au travail. Nos services : audit et aménagement de postes
de travail, prévention des troubles musculosquelettiques (TMS), mise en conformité
santé-sécurité et RSE.

Voici {len(candidats)} articles d'actualité économique marocaine de la semaine,
tous issus de presse économique reconnue. Pour CHAQUE article, évalue s'il
représente une opportunité commerciale réaliste à moyen terme (nouvelle usine,
extension, recrutement massif, nouvelle réglementation applicable, grand chantier...).
Sois strict : marque pertinent=false si le lien avec l'ergonomie n'est pas réaliste.

Réponds UNIQUEMENT avec du JSON valide, sans texte avant/après, sans balises markdown :
[
  {{"num": 1, "pertinent": true, "qui": "nom entreprise/organisme",
    "pourquoi": "raison précise en 1 phrase", "comment": "angle d'approche en 1 phrase",
    "quand": "timing recommandé"}},
  ...
]
Inclue une entrée pour chaque article (même les non pertinents, avec pertinent=false).

Articles :
{articles_texte}
"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        texte = next((b['text'] for b in data.get('content', []) if b.get('type') == 'text'), '')
        texte = texte.strip().removeprefix('```json').removeprefix('```').removesuffix('```').strip()
        analyses = json.loads(texte)

        resultats = []
        for a in analyses:
            if not a.get('pertinent'):
                continue
            idx = a.get('num', 0) - 1
            if 0 <= idx < len(candidats):
                c = candidats[idx]
                resultats.append({**c, **a})
        log.info(f"🤖 IA : {len(resultats)} opportunités réelles sur {len(candidats)} articles")
        return resultats
    except Exception as e:
        log.error(f"Erreur analyse IA : {e}")
        return []


def construire_email(opportunites: list):
    if not opportunites:
        return None, 0

    blocs = []
    for i, o in enumerate(opportunites, 1):
        blocs.append(f"""
{i}. [{o['secteur']}] {o['titre']}
   👤 QUI      : {o.get('qui', '—')}
   💡 POURQUOI : {o.get('pourquoi', '—')}
   🎯 COMMENT  : {o.get('comment', '—')}
   ⏰ QUAND    : {o.get('quand', '—')}
   🔗 Source   : {o['source']} — {o['lien']}
""")

    corps = f"""Bonjour,

Voici {len(opportunites)} opportunité(s) commerciale(s) identifiée(s) et
qualifiée(s) par IA cette semaine, à partir de presse économique reconnue
uniquement (voir liste des sources suivies dans le script).
{''.join(blocs)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sources filtrées : Médias24, La Vie Éco, L'Economiste, Challenge.ma,
Le Matin, Les Inspirations Éco, Telquel, Hespress, Agence Ecofin,
L'Opinion, Africa Intelligence, Jeune Afrique, Reuters, Bloomberg, MAP.

Cette analyse est générée automatiquement, qualifiée par Claude.
Vérifiez toujours les détails avant une prise de contact.

Cordialement,
Votre robot ErgoWatch 🤖
Indigo Ergonomie Maroc
"""
    return corps, len(opportunites)


def envoyer_email(corps: str, nb: int):
    if not all([EMAIL_USER, EMAIL_PASSWORD, EMAIL_DESTINATAIRE]):
        log.warning("Variables email manquantes → email non envoyé")
        return
    msg = MIMEMultipart()
    msg['Subject'] = f"🎯 ErgoWatch — {nb} opportunité(s) qualifiée(s) cette semaine"
    msg['From'] = f"ErgoWatch <{EMAIL_USER}>"
    msg['To'] = EMAIL_DESTINATAIRE
    msg.attach(MIMEText(corps, 'plain', 'utf-8'))
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASSWORD)
            smtp.send_message(msg)
        log.info(f"📧 Email envoyé : {nb} opportunités")
    except Exception as e:
        log.error(f"Erreur envoi email : {e}")


def main():
    log.info("=" * 55)
    log.info("🚀 ErgoWatch — Veille stratégique de marché (v3)")

    candidats = collecter_tout()
    opportunites = analyser_avec_ia(candidats)

    corps, nb = construire_email(opportunites)
    if corps:
        envoyer_email(corps, nb)
    else:
        log.info("📭 Aucune opportunité qualifiée cette semaine → pas d'email envoyé")

    log.info("✅ Terminé")


if __name__ == "__main__":
    main()
