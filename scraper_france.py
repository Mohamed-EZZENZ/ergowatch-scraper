"""
ErgoWatch France — Scraper automatique
INDIGO ERGONOMIE

Sources :
  1. BOAMP Open Data API (principal - officiel)
  2. TED / JOUE API (marchés européens)
  3. Klekoon (agrégateur)

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

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

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

TERMES_BOAMP = [
    'ergonomie', 'ergonome',
    'troubles musculo', 'TMS prévention',
    'conditions de travail', 'qualité de vie au travail',
    'DUERP', 'risques professionnels formation',
    'gestes et postures', 'facteurs humains',
]

def calculer_pertinence(titre, description, organisme=''):
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
    if any(mk.lower() in titre.lower() for mk in ['ergonomie', 'ergonome', 'TMS', 'ergonomiste']):
        score += 20
    return min(score, 100), mots_trouves[:8]

def scraper_boamp():
    logger.info("📡 BOAMP API — démarrage")
    resultats = []
    BASE_URL = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
    for terme in TERMES_BOAMP:
        try:
            params = {
                'where': f'search(objet, "{terme}")',
                'limit': 50,
                'order_by': 'dateparution desc',
                'select': 'idweb,objet,nomacheteur,dateparution,datelimitereponse,montant,urlsource,nature,typeavis,region,departement',
            }
            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            records = data.get('results', [])
            logger.info(f"  BOAMP '{terme}' → {len(records)} avis")
            for r in records:
                objet = r.get('objet', '')
                organisme = r.get('nomacheteur', '')
                if not objet:
                    continue
                score, mots = calculer_pertinence(objet, '', organisme)
                if score < 20:
                    continue
                budget = None
                montant = r.get('montant')
                if montant:
                    try:
                        val = float(str(montant).replace(',', '.'))
                        budget = f"{val:,.0f} €".replace(',', ' ')
                    except Exception:
                        budget = str(montant)
                region = r.get('region', '') or r.get('departement', '') or 'France'
                url_source = r.get('urlsource', '')
                if not url_source:
                    idweb = r.get('idweb', '')
                    if idweb:
                        url_source = f"https://www.boamp.fr/avis/detail/{idweb}"
                ao = {
                    'titre': objet[:500],
                    'organisme': organisme[:200] if organisme else 'Non précisé',
                    'date_publication': r.get('dateparution'),
                    'date_limite': r.get('datelimitereponse'),
                    'budget': budget,
                    'pertinence': score,
                    'mots_cles': mots,
                    'statut': 'Ouvert',
                    'source': 'BOAMP',
                    'url': url_source or 'https://www.boamp.fr',
                    'description': f"Marché public - {r.get('nature', '')} - {r.get('typeavis', '')}",
                    'wilaya': region[:100] if region else 'France',
                    'reference': r.get('idweb', '')[:50] if r.get('idweb') else '',
                }
                resultats.append(ao)
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"  Erreur BOAMP terme '{terme}': {e}")
    logger.info(f"✅ BOAMP — {len(resultats)} avis collectés")
    return resultats

def dedoublonner(resultats):
    vus = set()
    uniques = []
    for ao in resultats:
        cle = (ao['titre'][:100].lower(), ao['organisme'][:50].lower())
        if cle not in vus:
            vus.add(cle)
            uniques.append(ao)
    return uniques

def sauvegarder_supabase(appels_offres):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("❌ Variables manquantes")
        return 0, 0
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    inseres = 0
    ignores = 0
    for ao in appels_offres:
        try:
            for champ_date in ['date_publication', 'date_limite']:
                if ao.get(champ_date):
                    try:
                        val = str(ao[champ_date])
                        ao[champ_date] = val[:10] if len(val) >= 10 else None
                    except Exception:
                        ao[champ_date] = None
            supabase.table('appels_offres_france').upsert(ao, on_conflict='titre,organisme').execute()
            inseres += 1
        except Exception as e:
            logger.warning(f"  Erreur: {e}")
            ignores += 1
    return inseres, ignores

def main():
    start = datetime.now()
    logger.info(f"🚀 ErgoWatch France — {start.strftime('%Y-%m-%d %H:%M')}")
    tous_resultats = []
    try:
        tous_resultats.extend(scraper_boamp())
    except Exception as e:
        logger.error(f"❌ BOAMP: {e}")
    tous_resultats = dedoublonner(tous_resultats)
    tous_resultats.sort(key=lambda x: x['pertinence'], reverse=True)
    logger.info(f"📊 Total: {len(tous_resultats)} AO")
    if tous_resultats:
        inseres, ignores = sauvegarder_supabase(tous_resultats)
        logger.info(f"✅ Insérés: {inseres} / Ignorés: {ignores}")
    logger.info(f"✅ Terminé en {(datetime.now()-start).seconds}s")

if __name__ == '__main__':
    main()
