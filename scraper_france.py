"""
ErgoWatch France — Scraper automatique
INDIGO ERGONOMIE

Sources :
  1. BOAMP Open Data API (principal - officiel)
  2. PLACE — marches-publics.gouv.fr (marchés des ministères)
  3. TED / JOUE API (marchés européens)
  4. Klekoon (agrégateur)

Exécution : GitHub Actions, 2x/jour (7h et 18h)
"""

import os
import json
import time
import re
import logging
from datetime import datetime, date
from typing import Optional

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format='%(asctime)s — %(levelname)s — %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION SUPABASE
# ============================================================
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# ============================================================
# MOTS-CLÉS ERGONOMIE (France)
# ============================================================
MOTS_CLES_PRINCIPAUX = [
    'ergonomie', 'ergonome', 'ergonomiste', 'ergonomique',
    'TMS', 'troubles musculo-squelettiques', 'musculosquelettique',
    'prévention des risques professionnels',
    'amélioration des conditions de travail',
    'qualité de vie au travail', 'QVT',
    'gestes et postures',
    'aménagement des postes de travail',
    'analyse de l\'activité de travail',
    'facteurs humains',
]

MOTS_CLES_SECONDAIRES = [
    'santé au travail', 'médecine du travail',
    'risques psychosociaux', 'RPS',
    'DUERP', 'document unique',
    'prévention des AT', 'accidents du travail',
    'formation sécurité', 'prévention des risques',
    'CHSCT', 'CSE', 'sécurité au travail',
    'bien-être au travail', 'burn-out',
    'aménagement de bureaux', 'flex office', 'open space',
    'télétravail ergonomie', 'poste de travail',
    'éclairage au travail', 'ambiances physiques de travail',
    'charge physique de travail', 'charge mentale',
    'absentéisme', 'maintien dans l\'emploi',
    'IPRP', 'intervenant en prévention',
    'audit organisationnel', 'audit conditions de travail',
]

TOUS_MOTS_CLES = MOTS_CLES_PRINCIPAUX + MOTS_CLES_SECONDAIRES

# Termes de recherche BOAMP (optimisés pour leur moteur)
TERMES_BOAMP = [
    'ergonomie',
    'ergonome',
    'troubles musculo',
    'TMS prévention',
    'conditions de travail',
    'qualité de vie au travail',
    'DUERP',
    'risques professionnels formation',
    'gestes et postures',
    'facteurs humains',
]

# ============================================================
# CALCUL DE PERTINENCE
# ============================================================
def calculer_pertinence(titre: str, description: str, organisme: str = '') -> tuple[int, list[str]]:
    """Calcule un score de pertinence 0-100 et retourne les mots-clés trouvés."""
    texte = f"{titre} {description} {organisme}".lower()
    mots_trouves = []
    score = 0

    for mk in MOTS_CLES_PRINCIPAUX:
        if mk.lower() in texte:
            score += 12
            mots_trouves.append(mk)

    for mk in MOTS_CLES_SECONDAIRES:
        if mk.lower() in texte:
            score += 4
            if mk not in mots_trouves:
                mots_trouves.append(mk)

    # Bonus si l'objet principal est dans le titre
    if any(mk.lower() in titre.lower() for mk in ['ergonomie', 'ergonome', 'TMS', 'ergonomiste']):
        score += 20

    return min(score, 100), mots_trouves[:8]  # Max 8 mots-clés


# ============================================================
# SOURCE 1 : BOAMP Open Data API (officielle, gratuite)
# ============================================================
def scraper_boamp():
    """
    API BOAMP Open Data :
    https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records

    Champs utiles :
    - idweb : identifiant BOAMP
    - objet : objet du marché (=titre)
    - nomacheteur : nom de l'acheteur
    - dateparution : date de parution (YYYY-MM-DD)
    - datelimitereponse : date limite de remise des offres
    - montant : montant estimé
    - urlsource : URL de l'avis
    - nature : nature du marché
    - typeavis : type d'avis (AO, AAC...)
    - region : région administrative
    """
    logger.info("📡 BOAMP API — démarrage")
    resultats = []

    BASE_URL = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"

    for terme in TERMES_BOAMP:
        try:
            # Requête minimale — pas de select ni order_by pour éviter les 400
            params = {
                'q': terme,
                'limit': 50,
            }

            response = requests.get(BASE_URL, params=params, timeout=30)

            if response.status_code != 200:
                logger.warning(f"  BOAMP '{terme}' — HTTP {response.status_code}: {response.text[:200]}")
                continue

            data = response.json()

            records = data.get('results', [])
            logger.info(f"  BOAMP '{terme}' → {len(records)} avis")

            # DEBUG : afficher les champs réels du 1er enregistrement
            if records and terme == 'ergonomie':
                sample = records[0]
                logger.info(f"  🔑 Champs disponibles: {list(sample.keys())}")
                for k in list(sample.keys())[:10]:
                    logger.info(f"    {k} = {str(sample.get(k,''))[:100]}")

            for r in records:
                # Chercher le titre — essayer tous les noms de champs possibles
                objet = ''
                for field in ['objet', 'intitule', 'libelle', 'titre',
                              'objet_marche', 'description', 'denomination',
                              'libelle_marche', 'marche', 'objet_consultation']:
                    val = r.get(field, '')
                    if val and len(str(val)) > 10:
                        objet = str(val)
                        break

                # Fallback : prendre le plus long champ texte disponible
                if not objet:
                    for k, v in r.items():
                        if isinstance(v, str) and len(v) > 20:
                            objet = v
                            break

                organisme = ''
                for field in ['nomacheteur', 'acheteur', 'pouvoir_adjudicateur',
                              'acheteur_nom', 'entite_adjudicatrice', 'organisme']:
                    val = r.get(field, '')
                    if val:
                        organisme = str(val)
                        break

                if not objet or len(objet) < 5:
                    continue

                score, mots = calculer_pertinence(objet, '', organisme)

                # Si le score est 0 mais que le terme de recherche est pertinent,
                # on donne un score minimum (BOAMP a déjà filtré par pertinence)
                if score == 0 and terme in ['ergonomie', 'ergonome', 'ergonomiste']:
                    score = 40
                    mots = [terme]
                elif score == 0:
                    score = 25
                    mots = [terme]

                if score < 20:
                    continue

                # Budget — chercher dans plusieurs champs possibles
                budget = None
                montant_raw = (r.get('montant') or r.get('valeur_estimee') or
                               r.get('valeur') or None)
                if montant_raw:
                    try:
                        val = float(str(montant_raw).replace(',', '.').replace(' ', ''))
                        budget = f"{val:,.0f} €".replace(',', ' ')
                    except Exception:
                        budget = str(montant_raw)[:50]

                # Région
                region = (r.get('region') or r.get('lieu_execution') or
                          r.get('departement') or r.get('code_departement') or 'France')

                # Date publication
                date_pub = (r.get('dateparution') or r.get('date_publication') or
                            r.get('date_parution') or date.today().isoformat())

                # Date limite
                date_lim = (r.get('datelimitereponse') or r.get('date_limite') or
                            r.get('date_limite_reponse') or None)

                # URL — construire depuis l'ID si pas de champ url direct
                idweb = (r.get('idweb') or r.get('id') or r.get('numero') or '')
                url_source = (r.get('urlsource') or r.get('url') or r.get('lien') or '')
                if not url_source and idweb:
                    url_source = f"https://www.boamp.fr/avis/detail/{idweb}"
                if not url_source:
                    url_source = 'https://www.boamp.fr'

                # Description
                nature = r.get('nature', '')
                typeavis = r.get('typeavis', '')
                desc = f"Marché public BOAMP"
                if nature:
                    desc += f" - {nature}"
                if typeavis:
                    desc += f" - {typeavis}"

                ao = {
                    'titre': objet[:500],
                    'organisme': organisme[:200] if organisme else 'Non précisé',
                    'date_publication': str(date_pub)[:10] if date_pub else date.today().isoformat(),
                    'date_limite': str(date_lim)[:10] if date_lim else None,
                    'budget': budget,
                    'pertinence': score,
                    'mots_cles': mots,
                    'statut': 'Ouvert',
                    'source': 'BOAMP',
                    'url': url_source,
                    'description': desc,
                    'wilaya': str(region)[:100] if region else 'France',
                    'reference': str(idweb)[:50] if idweb else '',
                }
                resultats.append(ao)

                # Log le premier résultat pour debug
                if len(resultats) == 1:
                    logger.info(f"  🔍 Premier AO trouvé: {objet[:80]}")

            time.sleep(0.5)  # Respecter l'API

        except Exception as e:
            logger.warning(f"  Erreur BOAMP terme '{terme}': {e}")

    logger.info(f"✅ BOAMP — {len(resultats)} avis collectés avant dédoublonnage")
    return resultats


# ============================================================
# SOURCE 2 : PLACE — marches-publics.gouv.fr (marchés État)
# ============================================================
def scraper_place():
    """
    PLACE = Plateforme des Achats de l'État (ministères, services centraux).
    URL de recherche :
    https://www.marches-publics.gouv.fr/index.php?page=entreprise.EntrepriseAdvancedSearch
    &AllCons&keyword=ergonomie&nbResultats=50
    """
    logger.info("📡 PLACE (marches-publics.gouv.fr) — démarrage")
    resultats = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8',
        'Referer': 'https://www.marches-publics.gouv.fr/',
    }

    termes_place = ['ergonomie', 'ergonome', 'TMS', 'conditions de travail', 'facteurs humains']

    for terme in termes_place:
        try:
            import urllib.parse
            params = {
                'page': 'entreprise.EntrepriseAdvancedSearch',
                'AllCons': '',
                'keyword': terme,
                'nbResultats': '50',
                'typeRecherche': '0',
            }
            url = 'https://www.marches-publics.gouv.fr/index.php?' + urllib.parse.urlencode(params)

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logger.warning(f"  PLACE '{terme}' — HTTP {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # PLACE affiche les résultats dans un tableau HTML
            # Sélecteurs multiples pour s'adapter aux versions de la plateforme
            lignes = (
                soup.select('table.tableResultats tr[class*="result"]') or
                soup.select('table tr[id*="consultation"]') or
                soup.select('div.result-item') or
                soup.select('table.table tbody tr') or
                soup.select('tr.tenderRow') or
                []
            )

            # Si pas de lignes trouvées, chercher toutes les lignes de tableau avec des liens
            if not lignes:
                lignes = [tr for tr in soup.select('table tr') if tr.find('a') and len(tr.find_all('td')) >= 2]

            logger.info(f"  PLACE '{terme}' → {len(lignes)} lignes trouvées")

            for ligne in lignes[:20]:
                cellules = ligne.find_all('td')
                if len(cellules) < 2:
                    continue

                # Extraire titre (objet/intitulé)
                titre = ''
                lien_el = ligne.find('a')
                if lien_el:
                    titre = lien_el.get_text(strip=True)

                # Fallback: première grande cellule de texte
                if not titre or len(titre) < 10:
                    for td in cellules:
                        t = td.get_text(strip=True)
                        if len(t) > 15:
                            titre = t
                            break

                if not titre or len(titre) < 15:
                    continue

                # Organisme (souvent la 2ème colonne)
                organisme = cellules[1].get_text(strip=True) if len(cellules) > 1 else ''
                if len(organisme) > 200:
                    organisme = organisme[:200]

                # Date limite (chercher une date au format JJ/MM/AAAA)
                date_limite = None
                texte_ligne = ligne.get_text(' ', strip=True)
                dates = re.findall(r'\b(\d{2})[/\-](\d{2})[/\-](\d{4})\b', texte_ligne)
                if dates:
                    j, m, a = dates[-1]
                    date_limite = f"{a}-{m}-{j}"

                # Référence
                reference = ''
                for td in cellules:
                    t = td.get_text(strip=True)
                    if re.match(r'^[\w\-]{3,30}$', t) and any(c.isdigit() for c in t):
                        reference = t[:50]
                        break

                # URL de la consultation
                url_ao = 'https://www.marches-publics.gouv.fr'
                if lien_el and lien_el.get('href'):
                    href = lien_el['href']
                    if href.startswith('http'):
                        url_ao = href
                    else:
                        url_ao = 'https://www.marches-publics.gouv.fr' + href

                score, mots = calculer_pertinence(titre, '', organisme)
                if score < 20:
                    continue

                ao = {
                    'titre': titre[:500],
                    'organisme': organisme if organisme else 'Ministère (PLACE)',
                    'date_publication': date.today().isoformat(),
                    'date_limite': date_limite,
                    'budget': None,
                    'pertinence': score,
                    'mots_cles': mots,
                    'statut': 'Ouvert',
                    'source': 'PLACE',
                    'url': url_ao,
                    'description': f'Marché public État - Plateforme PLACE - terme "{terme}"',
                    'wilaya': 'France (État)',
                    'reference': reference,
                }
                resultats.append(ao)

            time.sleep(1.5)  # Respecter la plateforme

        except Exception as e:
            logger.warning(f"  Erreur PLACE terme '{terme}': {e}")

    logger.info(f"✅ PLACE — {len(resultats)} avis collectés")
    return resultats


# ============================================================
# SOURCE 3 : TED Open Data API (marchés européens)
# ============================================================
def scraper_ted():
    """
    API TED (Tenders Electronic Daily) - marchés >215 000€
    https://ted.europa.eu/api/v3.0/notices/search
    """
    logger.info("📡 TED/JOUE API — démarrage")
    resultats = []

    try:
        # API TED v3
        url = "https://ted.europa.eu/api/v3.0/notices/search"

        payload = {
            "query": "FT ~ \"ergonomie\" AND PC = FR",
            "fields": ["ND", "TI", "AA", "PD", "DT", "NC", "AC", "TY"],
            "page": 1,
            "pageSize": 25,
            "scope": 1,
            "language": "FR"
        }

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        response = requests.post(url, json=payload, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            notices = data.get('notices', [])
            logger.info(f"  TED 'ergonomie' → {len(notices)} avis")

            for notice in notices:
                titre = notice.get('TI', {}).get('FRA', '') or notice.get('TI', {}).get('ENG', '')
                organisme = notice.get('AA', {}).get('AAN', '') if notice.get('AA') else ''

                if not titre:
                    continue

                score, mots = calculer_pertinence(titre, '', organisme)
                if score < 15:
                    continue

                nd = notice.get('ND', '')
                url_avis = f"https://ted.europa.eu/udl?uri=TED:NOTICE:{nd}:TEXT:FR:HTML" if nd else 'https://ted.europa.eu'

                ao = {
                    'titre': titre[:500],
                    'organisme': organisme[:200] if organisme else 'Organisme UE',
                    'date_publication': notice.get('PD', ''),
                    'date_limite': notice.get('DT', ''),
                    'budget': None,
                    'pertinence': score,
                    'mots_cles': mots,
                    'statut': 'Ouvert',
                    'source': 'TED/JOUE',
                    'url': url_avis,
                    'description': 'Marché européen publié au Journal Officiel de l\'UE',
                    'wilaya': 'France (marché UE)',
                    'reference': nd[:50] if nd else '',
                }
                resultats.append(ao)
        else:
            logger.warning(f"  TED API — statut {response.status_code}")

    except Exception as e:
        logger.warning(f"  Erreur TED API: {e}")

    logger.info(f"✅ TED — {len(resultats)} avis collectés")
    return resultats


# ============================================================
# SOURCE 3 : Klekoon (scraping HTML)
# ============================================================
def scraper_klekoon():
    """Scraping de Klekoon.com — agrégateur d'AO publics."""
    logger.info("📡 Klekoon — démarrage")
    resultats = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'fr-FR,fr;q=0.9',
    }

    termes = ['ergonomie', 'ergonome', 'TMS+prévention', 'conditions+travail+audit']

    for terme in termes:
        try:
            url = f"https://www.klekoon.com/search-by-keyword.html?keyword={terme}&type=0"
            response = requests.get(url, headers=headers, timeout=20)

            if response.status_code != 200:
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            # Chercher les éléments d'annonces
            annonces = soup.find_all(['article', 'div'], class_=re.compile(r'(annonce|result|tender|marche)', re.I))

            for annonce in annonces[:15]:
                titre_el = annonce.find(['h2', 'h3', 'h4', 'a'])
                if not titre_el:
                    continue

                titre = titre_el.get_text(strip=True)
                if len(titre) < 15:
                    continue

                organisme = ''
                org_el = annonce.find(class_=re.compile(r'(organisme|acheteur|pouvoir)', re.I))
                if org_el:
                    organisme = org_el.get_text(strip=True)

                score, mots = calculer_pertinence(titre, '', organisme)
                if score < 25:
                    continue

                lien = '#'
                lien_el = titre_el if titre_el.name == 'a' else annonce.find('a')
                if lien_el and lien_el.get('href'):
                    href = lien_el['href']
                    lien = href if href.startswith('http') else f"https://www.klekoon.com{href}"

                ao = {
                    'titre': titre[:500],
                    'organisme': organisme[:200] if organisme else 'Non précisé',
                    'date_publication': date.today().isoformat(),
                    'date_limite': None,
                    'budget': None,
                    'pertinence': score,
                    'mots_cles': mots,
                    'statut': 'Ouvert',
                    'source': 'Klekoon',
                    'url': lien,
                    'description': f'Appel d\'offres trouvé via Klekoon - terme "{terme}"',
                    'wilaya': 'France',
                    'reference': '',
                }
                resultats.append(ao)

            time.sleep(1)

        except Exception as e:
            logger.warning(f"  Erreur Klekoon terme '{terme}': {e}")

    logger.info(f"✅ Klekoon — {len(resultats)} résultats")
    return resultats


# ============================================================
# SAUVEGARDE SUPABASE
# ============================================================
def sauvegarder_supabase(appels_offres: list[dict]) -> tuple[int, int]:
    """Sauvegarde les AO dans Supabase. Retourne (insérés, ignorés)."""

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("❌ Variables SUPABASE_URL ou SUPABASE_SERVICE_KEY manquantes")
        return 0, 0

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    inseres = 0
    ignores = 0

    for ao in appels_offres:
        try:
            # Nettoyage des dates
            for champ_date in ['date_publication', 'date_limite']:
                if ao.get(champ_date):
                    try:
                        val = str(ao[champ_date])
                        # Normaliser au format YYYY-MM-DD
                        if len(val) >= 10:
                            ao[champ_date] = val[:10]
                        else:
                            ao[champ_date] = None
                    except Exception:
                        ao[champ_date] = None

            # Upsert (insert or update si même titre+organisme)
            result = supabase.table('appels_offres_france').upsert(
                ao,
                on_conflict='titre,organisme'
            ).execute()

            inseres += 1

        except Exception as e:
            logger.warning(f"  Erreur sauvegarde '{ao.get('titre', '')[:50]}': {e}")
            ignores += 1

    return inseres, ignores


# ============================================================
# DÉDOUBLONNAGE LOCAL
# ============================================================
def dedoublonner(resultats: list[dict]) -> list[dict]:
    """Supprime les doublons par (titre, organisme)."""
    vus = set()
    uniques = []

    for ao in resultats:
        cle = (ao['titre'][:100].lower(), ao['organisme'][:50].lower())
        if cle not in vus:
            vus.add(cle)
            uniques.append(ao)

    return uniques


# ============================================================
# MAIN
# ============================================================
def main():
    start = datetime.now()
    logger.info("=" * 60)
    logger.info(f"🚀 ErgoWatch France — démarrage {start.strftime('%Y-%m-%d %H:%M')}")
    logger.info("=" * 60)

    tous_resultats = []

    # 1. BOAMP (source principale)
    try:
        resultats_boamp = scraper_boamp()
        tous_resultats.extend(resultats_boamp)
    except Exception as e:
        logger.error(f"❌ BOAMP échoué: {e}")

    # 2. PLACE (marchés des ministères)
    try:
        resultats_place = scraper_place()
        tous_resultats.extend(resultats_place)
    except Exception as e:
        logger.error(f"❌ PLACE échoué: {e}")

    # 3. TED/JOUE
    try:
        resultats_ted = scraper_ted()
        tous_resultats.extend(resultats_ted)
    except Exception as e:
        logger.error(f"❌ TED échoué: {e}")

    # 4. Klekoon
    try:
        resultats_klekoon = scraper_klekoon()
        tous_resultats.extend(resultats_klekoon)
    except Exception as e:
        logger.error(f"❌ Klekoon échoué: {e}")

    # Dédoublonnage
    tous_resultats = dedoublonner(tous_resultats)

    # Tri par pertinence
    tous_resultats.sort(key=lambda x: x['pertinence'], reverse=True)

    logger.info(f"\n📊 Total après dédoublonnage : {len(tous_resultats)} appels d'offres")

    # Résumé par score
    tres_pertinents = [ao for ao in tous_resultats if ao['pertinence'] >= 80]
    pertinents = [ao for ao in tous_resultats if 50 <= ao['pertinence'] < 80]

    logger.info(f"   ⭐ Très pertinents (≥80) : {len(tres_pertinents)}")
    logger.info(f"   ✓  Pertinents (50-79)   : {len(pertinents)}")

    if tres_pertinents:
        logger.info(f"\n🎯 TOP AO du jour :")
        for ao in tres_pertinents[:5]:
            logger.info(f"   [{ao['pertinence']}%] {ao['titre'][:80]} — {ao['organisme'][:40]}")

    # Sauvegarde
    if tous_resultats:
        logger.info(f"\n💾 Sauvegarde Supabase ({len(tous_resultats)} AO)...")
        inseres, ignores = sauvegarder_supabase(tous_resultats)
        logger.info(f"   ✅ Insérés/mis à jour : {inseres}")
        logger.info(f"   ⏩ Ignorés (erreur)   : {ignores}")
    else:
        logger.info("⚠️ Aucun résultat à sauvegarder")

    duree = (datetime.now() - start).seconds
    logger.info(f"\n✅ Terminé en {duree}s")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
