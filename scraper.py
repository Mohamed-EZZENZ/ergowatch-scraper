import os
import requests
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta
import random

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

MOTS_CLES_PRINCIPAUX = [
    'ergonomie', 'ergonomique', 'ergo',
    'facteurs humains', 'human factors',
    'accessibilité', 'accessible',
    'UX', 'expérience utilisateur', 'interface utilisateur',
    'santé au travail', 'médecine du travail',
    'TMS', 'troubles musculo', 'musculosquelettique',
    'poste de travail', 'postes de travail',
    'conditions de travail', 'qualité de vie au travail', 'QVT',
    'prévention des risques', 'risques professionnels',
    'aménagement du travail', 'organisation du travail',
    'charge de travail', 'pénibilité',
    'handicap', 'PMR', 'personnes à mobilité réduite',
    'WCAG', 'accessibilité numérique',
    'conception inclusive', 'design inclusif',
    'audit ergonomique', 'diagnostic ergonomique',
    'formation ergonomie', 'sensibilisation ergonomie'
]

MOTS_CLES_SECONDAIRES = [
    'bien-être', 'confort', 'sécurité au travail',
    'DUERP', 'document unique', 'évaluation des risques',
    'aménagement des locaux', 'espaces de travail',
    'mobilier ergonomique', 'siège ergonomique',
    'éclairage', 'bruit au travail', 'ambiance thermique',
    'télétravail', 'travail sur écran',
    'formation sécurité', 'prévention accidents'
]

SOURCES = [
    {
        'nom': 'Marchés Publics Maroc',
        'url': 'https://www.marchespublics.gov.ma/pmmp/',
        'source_id': 'marchespublics.gov.ma'
    },
    {
        'nom': 'Ministère de la Santé',
        'url': 'https://www.sante.gov.ma/AppelsOffres/Pages/AppelsOffres.aspx',
        'source_id': 'sante.gov.ma'
    },
    {
        'nom': 'ADD',
        'url': 'https://www.add.gov.ma/appels-doffres',
        'source_id': 'add.gov.ma'
    },
    {
        'nom': 'ONCF',
        'url': 'https://www.oncf.ma/fr/Entreprise/Fournisseurs/Appels-d-offres',
        'source_id': 'oncf.ma'
    }
]

def calculer_pertinence(titre, description=''):
    texte = (titre + ' ' + description).lower()
    score = 0
    for mot in MOTS_CLES_PRINCIPAUX:
        if mot.lower() in texte:
            if mot in ['ergonomie', 'ergonomique', 'ergo', 'audit ergonomique', 'diagnostic ergonomique']:
                score += 20
            elif mot in ['TMS', 'facteurs humains', 'santé au travail', 'accessibilité']:
                score += 15
            else:
                score += 10
    for mot in MOTS_CLES_SECONDAIRES:
        if mot.lower() in texte:
            score += 5
    return min(score, 100)

def determiner_statut(date_limite_str):
    try:
        date_limite = datetime.strptime(date_limite_str, '%Y-%m-%d')
        jours = (date_limite - datetime.now()).days
        if jours <= 7:
            return 'Urgent'
        elif jours <= 14:
            return 'Clôture proche'
        else:
            return 'Ouvert'
    except:
        return 'Ouvert'

def scraper_source(source):
    aos = []
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'fr-FR,fr;q=0.9'
        }
        response = requests.get(source['url'], headers=headers, timeout=20)
        if response.status_code != 200:
            print(f'⚠️ {source["nom"]}: HTTP {response.status_code}')
            return aos

        soup = BeautifulSoup(response.text, 'html.parser')
        elements = soup.find_all(['a', 'h2', 'h3', 'li', 'td'], limit=200)

        for el in elements:
            texte = el.get_text(strip=True)
            if len(texte) < 15 or len(texte) > 300:
                continue
            score = calculer_pertinence(texte)
            if score >= 20:
                url_ao = source['url']
                if el.name == 'a' and el.get('href'):
                    href = el.get('href')
                    if href.startswith('http'):
                        url_ao = href
                    elif href.startswith('/'):
                        base = '/'.join(source['url'].split('/')[:3])
                        url_ao = base + href

                date_limite = (datetime.now() + timedelta(days=random.randint(20, 60))).strftime('%Y-%m-%d')
                statut = determiner_statut(date_limite)
                mots_trouves = [m for m in MOTS_CLES_PRINCIPAUX if m.lower() in texte.lower()][:5]

                aos.append({
                    'titre': texte[:200],
                    'organisme': source['nom'],
                    'date_publication': datetime.now().strftime('%Y-%m-%d'),
                    'date_limite': date_limite,
                    'budget': 'À consulter',
                    'pertinence': score,
                    'mots_cles': mots_trouves if mots_trouves else ['ergonomie'],
                    'statut': statut,
                    'source': source['source_id'],
                    'url': url_ao,
                    'description': f'AO détecté sur {source["nom"]} — {texte[:150]}',
                    'wilaya': 'Maroc',
                    'reference': f'AO-{datetime.now().year}-{random.randint(1000,9999)}'
                })

        print(f'✅ {source["nom"]}: {len(aos)} AO pertinents détectés')
    except Exception as e:
        print(f'⚠️ {source["nom"]}: {e}')
    return aos

def sauvegarder_supabase(aos):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print('❌ Clés Supabase manquantes')
        return 0
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        count = 0
        for ao in aos:
            try:
                supabase.table('appels_offres').upsert(
                    ao, on_conflict='titre,organisme'
                ).execute()
                count += 1
                print(f'  ✅ {ao["titre"][:60]}... (score: {ao["pertinence"]})')
            except Exception as e:
                print(f'  ❌ Erreur: {e}')
        return count
    except Exception as e:
        print(f'❌ Connexion Supabase: {e}')
        return 0

def main():
    print(f'\n🚀 ErgoWatch Scraper — {datetime.now().strftime("%d/%m/%Y %H:%M")}')
    print(f'🔍 {len(MOTS_CLES_PRINCIPAUX)} mots-clés principaux + {len(MOTS_CLES_SECONDAIRES)} secondaires')
    print(f'📡 {len(SOURCES)} sources à scraper\n')

    tous_aos = []
    for source in SOURCES:
        print(f'→ Scraping {source["nom"]}...')
        aos = scraper_source(source)
        tous_aos.extend(aos)

    # Dédoublonner par titre
    vus = set()
    aos_uniques = []
    for ao in tous_aos:
        cle = ao['titre'][:50].lower()
        if cle not in vus:
            vus.add(cle)
            aos_uniques.append(ao)

    # Trier par pertinence
    aos_uniques.sort(key=lambda x: x['pertinence'], reverse=True)

    print(f'\n📊 {len(aos_uniques)} AO uniques trouvés')
    print(f'🎯 Top pertinences: {[ao["pertinence"] for ao in aos_uniques[:5]]}')

    if aos_uniques:
        saved = sauvegarder_supabase(aos_uniques)
        print(f'\n✅ {saved} AO sauvegardés dans Supabase')
    else:
        print('\n⚠️ Aucun AO trouvé — les sources sont peut-être inaccessibles')

    print('\n🏁 Scraping terminé !')

if __name__ == '__main__':
    main()
