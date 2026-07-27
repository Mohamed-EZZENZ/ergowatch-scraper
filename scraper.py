"""
ErgoWatch Maroc — Scraper v2
============================
Nouveautés par rapport à la v1 :
  1. Nettoyage automatique des offres expirées (marquées 'Clôturé')
  2. Détection des VRAIS nouveaux AO (comparaison avec la base)
  3. Email envoyé UNIQUEMENT s'il y a du nouveau, avec le détail des offres
  4. Recherche élargie (plus de mots-clés, plus de pages)
  5. Logs détaillés pour diagnostiquer ce qui est réellement trouvé
"""

import os
import re
import time
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_DESTINATAIRE = os.getenv("EMAIL_DESTINATAIRE")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ergowatch")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept-Language": "fr-FR,fr;q=0.9,ar;q=0.8",
}

SEARCH_TERMS = [
    "ergonomie", "ergonomique", "poste de travail", "conditions de travail",
    "santé au travail", "risques professionnels", "TMS",
    "aménagement bureaux", "mobilier de bureau", "accessibilité",
]

KEYWORD_WEIGHTS = {
    "ergonomie": 30, "ergonomique": 30, "ergonome": 30, "ergonomics": 30,
    "facteurs humains": 25, "human factors": 25,
    "poste de travail": 20, "postes de travail": 20,
    "prévention tms": 20, "troubles musculosquelettiques": 20, "tms": 15,
    "santé au travail": 15, "conditions de travail": 15,
    "risques professionnels": 12, "pénibilité": 12,
    "aménagement bureau": 12, "aménagement des bureaux": 12,
    "mobilier ergonomique": 15, "siège ergonomique": 15,
    "conception ux": 10, "usabilité": 10, "expérience utilisateur": 10,
    "accessibilité": 8, "wcag": 10,
    "mobilier de bureau": 8, "formation sécurité": 6,
}

EXCLUDE_TERMS = [
    "fourniture de véhicules", "travaux de construction", "gardiennage",
    "nettoyage des locaux", "denrées alimentaires",
]


def compute_pertinence(text: str) -> int:
    t = text.lower()
    if any(x in t for x in EXCLUDE_TERMS):
        return 0
    score = sum(w for kw, w in KEYWORD_WEIGHTS.items() if kw in t)
    return min(score, 100)


def extract_keywords(text: str) -> list:
    t = text.lower()
    return [kw for kw in KEYWORD_WEIGHTS if kw in t][:6]


def parse_date(raw: str):
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def compute_statut(date_limite):
    if not date_limite:
        return "Ouvert"
    try:
        d = datetime.strptime(date_limite, "%Y-%m-%d").date()
        delta = (d - date.today()).days
        if delta < 0:
            return "Clôturé"
        if delta <= 7:
            return "Clôture proche"
        if delta <= 14:
            return "Urgent"
        return "Ouvert"
    except Exception:
        return "Ouvert"


def scrape_marchespublics() -> list:
    results = []
    base = "https://www.marchespublics.gov.ma"

    for term in SEARCH_TERMS:
        try:
            url = f"{base}/index.php?page=entreprise.EntrepriseAdvancedSearch&AllCons&keyWord={requests.utils.quote(term)}"
            resp = requests.get(url, headers=HEADERS, timeout=20)
            log.info(f"[Maroc] Recherche '{term}' → HTTP {resp.status_code}, {len(resp.text)} octets")

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = (soup.select("table.table-results tr")
                    or soup.select("div.ligne-resultat")
                    or soup.select("table tr"))

            found_this_term = 0
            for row in rows:
                text = row.get_text(" ", strip=True)
                if len(text) < 30:
                    continue
                pertinence = compute_pertinence(text)
                if pertinence < 30:
                    continue

                link = row.find("a")
                url_ao = (base + link["href"]) if link and link.get("href", "").startswith("/") else (link["href"] if link and link.get("href", "").startswith("http") else base)

                m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
                dl = parse_date(m.group(1)) if m else None

                results.append({
                    "titre": text[:180],
                    "organisme": "Marchés Publics Maroc",
                    "date_publication": str(date.today()),
                    "date_limite": dl,
                    "budget": "À consulter",
                    "pertinence": pertinence,
                    "mots_cles": extract_keywords(text),
                    "statut": compute_statut(dl),
                    "source": "marchespublics.gov.ma",
                    "url": url_ao,
                    "description": text[:400],
                    "wilaya": "National",
                    "reference": "",
                })
                found_this_term += 1

            log.info(f"[Maroc] '{term}' → {found_this_term} AO pertinents")
            time.sleep(2)

        except Exception as e:
            log.warning(f"[Maroc] Erreur sur '{term}': {e}")

    log.info(f"[Maroc] TOTAL brut : {len(results)} AO trouvés")
    return results


def deduplicate(items: list) -> list:
    seen, out = set(), []
    for it in items:
        key = re.sub(r"\W+", "", it["titre"].lower())[:80]
        if key not in seen:
            seen.add(key)
            out.append(it)
    return out


def mark_expired(sb: Client):
    today = str(date.today())
    try:
        res = sb.table("appels_offres") \
            .update({"statut": "Clôturé"}) \
            .lt("date_limite", today) \
            .neq("statut", "Clôturé") \
            .execute()
        n = len(res.data or [])
        log.info(f"🧹 {n} offre(s) expirée(s) marquée(s) 'Clôturé'")
    except Exception as e:
        log.error(f"Erreur nettoyage expirés : {e}")


def filter_new_offers(items: list, sb: Client) -> list:
    try:
        existing = sb.table("appels_offres").select("titre").execute()
        existing_keys = {re.sub(r"\W+", "", r["titre"].lower())[:80] for r in (existing.data or [])}
    except Exception as e:
        log.error(f"Erreur lecture base : {e}")
        existing_keys = set()

    new = [it for it in items
           if re.sub(r"\W+", "", it["titre"].lower())[:80] not in existing_keys]
    log.info(f"🆕 {len(new)} NOUVEAU(X) AO sur {len(items)} trouvés")
    return new


def save(items: list, sb: Client):
    if not items:
        return
    try:
        sb.table("appels_offres").upsert(items, on_conflict="titre,organisme").execute()
        log.info(f"💾 {len(items)} AO sauvegardés")
    except Exception as e:
        log.error(f"Erreur sauvegarde : {e}")


def send_email_new_offers(new_offers: list):
    if not new_offers:
        log.info("📭 Aucun nouvel AO → pas d'email envoyé")
        return
    if not all([EMAIL_USER, EMAIL_PASSWORD, EMAIL_DESTINATAIRE]):
        log.warning("Variables email manquantes → email non envoyé")
        return

    new_offers = sorted(new_offers, key=lambda x: -x["pertinence"])

    lignes = []
    for ao in new_offers:
        lignes.append(f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 {ao['titre']}
   Pertinence : {ao['pertinence']}%  |  Statut : {ao['statut']}
   Source : {ao['source']}
   Date limite : {ao['date_limite'] or 'À consulter'}
   Lien : {ao['url']}
""")

    body = f"""Bonjour,

{len(new_offers)} NOUVEAU(X) appel(s) d'offre en ergonomie détecté(s) :
{''.join(lignes)}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
👉 Tableau de bord : https://votre-app.netlify.app

Votre robot ErgoWatch 🤖
"""

    msg = MIMEMultipart()
    msg["Subject"] = f"🚨 ErgoWatch — {len(new_offers)} nouvel(aux) AO ergonomie !"
    msg["From"] = f"ErgoWatch <{EMAIL_USER}>"
    msg["To"] = EMAIL_DESTINATAIRE
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASSWORD)
            smtp.send_message(msg)
        log.info(f"📧 Email envoyé : {len(new_offers)} nouveaux AO")
    except Exception as e:
        log.error(f"Erreur envoi email : {e}")


def main():
    log.info("=" * 55)
    log.info("🚀 ErgoWatch v2 — Scraping Maroc")

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    mark_expired(sb)
    found = deduplicate(scrape_marchespublics())
    found = [f for f in found if f["statut"] != "Clôturé"]
    new_offers = filter_new_offers(found, sb)
    save(found, sb)
    send_email_new_offers(new_offers)

    log.info("✅ Terminé")


if __name__ == "__main__":
    main()
