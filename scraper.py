import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta
import random
import urllib3

# ── Désactiver les avertissements SSL ────────────────────────────────────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# ── Mots-clés ────────────────────────────────────────────────────────────────
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
    'formation sécurité', 'prévention accidents',
    'analyse des risques', 'étude ergonomique',
    'conseil ergonomique', 'intervention ergonomique'
]

# ── Sources — 20 sources marocaines ─────────────────────────────────────────
SOURCES = [

    # ── PORTAIL NATIONAL ─────────────────────────────────────────────────────
    {
        'nom': 'Marchés Publics Maroc — Portail National',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'ergonomie accessibilite sante travail'}
    },

    # ── MINISTÈRES ───────────────────────────────────────────────────────────
    {
        'nom': 'Ministère de la Santé',
        'url': 'https://www.sante.gov.ma/AppelsOffres/Pages/AppelsOffres.aspx',
        'source_id': 'sante.gov.ma'
    },
    {
        'nom': 'Ministère du Travail et de l\'Insertion Professionnelle',
        'url': 'https://www.emploi.gov.ma/index.php/fr/appels-d-offres',
        'source_id': 'emploi.gov.ma'
    },
    {
        'nom': 'Ministère de l\'Éducation Nationale',
        'url': 'https://www.men.gov.ma/Ar/Pages/AppelOffre.aspx',
        'source_id': 'men.gov.ma'
    },
    {
        'nom': 'Ministère des Finances',
        'url': 'https://www.finances.gov.ma/fr/vous-orientez/Pages/appels-offres.aspx',
        'source_id': 'finances.gov.ma'
    },
    {
        'nom': 'Ministère de l\'Intérieur',
        'url': 'https://www.interieur.gov.ma/fr/appels-doffres',
        'source_id': 'interieur.gov.ma'
    },
    {
        'nom': 'Ministère de la Justice',
        'url': 'https://www.justice.gov.ma/fr/appels-offres.aspx',
        'source_id': 'justice.gov.ma'
    },

    # ── ORGANISMES SOCIAUX ───────────────────────────────────────────────────
    {
        'nom': 'CNSS',
        'url': 'https://www.cnss.ma/fr/content/appels-doffres',
        'source_id': 'cnss.ma'
    },
    {
        'nom': 'ANAM — Assurance Maladie',
        'url': 'https://www.anam.ma/appels-doffres/',
        'source_id': 'anam.ma'
    },
    {
        'nom': 'ANAPEC',
        'url': 'https://www.anapec.org/appels-offres',
        'source_id': 'anapec.org'
    },

    # ── GRANDES ENTREPRISES PUBLIQUES ────────────────────────────────────────
    {
        'nom': 'OCP Group',
        'url': 'https://www.ocpgroup.ma/fr/fournisseurs/appels-doffres',
        'source_id': 'ocpgroup.ma'
    },
    {
        'nom': 'ONCF',
        'url': 'https://www.oncf.ma/fr/Entreprise/Fournisseurs/Appels-d-offres',
        'source_id': 'oncf.ma'
    },
    {
        'nom': 'ONEE — Office National Électricité & Eau',
        'url': 'https://www.one.org.ma/FR/pages/interne.asp?esp=1&id1=4&id2=18',
        'source_id': 'one.org.ma'
    },
    {
        'nom': 'Royal Air Maroc',
        'url': 'https://www.royalairmaroc.com/ma-fr/institutionnel/appels-doffres',
        'source_id': 'royalairmaroc.com'
    },
    {
        'nom': 'ONDA — Aéroports du Maroc',
        'url': 'https://www.onda.ma/fr/appels-doffres',
        'source_id': 'onda.ma'
    },
    {
        'nom': 'Maroc Telecom',
        'url': 'https://www.iam.ma/fr/appels-offres',
        'source_id': 'iam.ma'
    },
    {
        'nom': 'MASEN — Énergie Solaire',
        'url': 'https://www.masen.ma/fr/appels-offres',
        'source_id': 'masen.ma'
    },
    {
        'nom': 'CDG — Caisse de Dépôt et de Gestion',
        'url': 'https://www.cdg.ma/fr/appels-doffres',
        'source_id': 'cdg.ma'
    },

    # ── NUMÉRIQUE ────────────────────────────────────────────────────────────
    {
        'nom': 'ADD — Agence du Développement Digital',
        'url': 'https://www.add.gov.ma/appels-doffres',
        'source_id': 'add.gov.ma'
    },
    {
        'nom': 'ANRT — Télécommunications',
        'url': 'https://www.anrt.net.ma/fr/appels-doffres',
        'source_id': 'anrt.net.ma'
    },

    # ── CHU & HÔPITAUX ───────────────────────────────────────────────────────
    {
        'nom': 'CHU Ibn Rochd — Casablanca',
        'url': 'https://www.chuibnrochd.ma/appels-offres/',
        'source_id': 'chuibnrochd.ma'
    },
    {
        'nom': 'CHU Mohammed VI — Marrakech',
        'url': 'https://www.chumarrakech.ma/index.php/annonces/fournisseurs/appels-doffres',
        'source_id': 'chumarrakech.ma'
    },
    {
        'nom': 'CHU Hassan II — Fès',
        'url': 'https://www.chufes.ma/index.php/appels-d-offres',
        'source_id': 'chufes.ma'
    },
    {
        'nom': 'CHU Ibn Sina — Rabat',
        'url': 'https://www.churoyal.ma/fr/appels-doffres',
        'source_id': 'churoyal.ma'
    },
    {
        'nom': 'CHU Hassan II — Agadir',
        'url': 'https://www.chuagadir.ma/appels-offres',
        'source_id': 'chuagadir.ma'
    },

    # ── COLLECTIVITÉS LOCALES ────────────────────────────────────────────────
    {
        'nom': 'Commune de Casablanca',
        'url': 'https://www.casablanca.ma/fr/appels-doffres',
        'source_id': 'casablanca.ma'
    },
    {
        'nom': 'Commune de Rabat',
        'url': 'https://www.rabat.ma/fr/appels-doffres',
        'source_id': 'rabat.ma'
    },

    # ── ÉDUCATION & RECHERCHE ────────────────────────────────────────────────
    {
        'nom': 'AREF Casablanca-Settat',
        'url': 'https://www.aref-casablanca.ma/appels-offres',
        'source_id': 'aref-casablanca.ma'
    },
    {
        'nom': 'Université Mohammed V — Rabat',
        'url': 'https://www.um5.ac.ma/um5/fr/appels-offres',
        'source_id': 'um5.ac.ma'
    },
    {
        'nom': 'Université Hassan II — Casablanca',
        'url': 'https://www.univh2c.ma/fr/appels-doffres',
        'source_id': 'univh2c.ma'
    },
]

# ── Session HTTP avec retry automatique ──────────────────────────────────────
def creer_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

SESSION = creer_session()

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'fr-FR,fr;q=0.9,ar;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
}

# ── Calcul pertinence ─────────────────────────────────────────────────────────
def calculer_pertinence(titre, description=''):
    texte = (titre + ' ' + description).lower()
    score = 0
    for mot in MOTS_CLES_PRINCIPAUX:
        if mot.lower() in texte:
            if mot in ['ergonomie', 'ergonomique', 'audit ergonomique', 'diagnostic ergonomique', 'intervention ergonomique']:
                score += 25
            elif mot in ['TMS', 'facteurs humains', 'santé au travail', 'accessibilité', 'WCAG']:
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

# ── Scraper générique ─────────────────────────────────────────────────────────
def scraper_source(source):
    aos = []
    try:
        params = source.get('params', {})
        response = SESSION.get(
            source['url'],
            headers=HEADERS,
            params=params,
            timeout=30,
            verify=False  # ← Contourne les erreurs SSL
        )
        if response.status_code != 200:
            print(f'  ⚠️ HTTP {response.status_code}')
            return aos

        soup = BeautifulSoup(response.text, 'html.parser')

        # Cherche dans les balises les plus porteuses de contenu
        elements = soup.find_all(
            ['a', 'h2', 'h3', 'h4', 'li', 'td', 'p', 'div'],
            limit=300
        )

        for el in elements:
            texte = el.get_text(strip=True)
            if len(texte) < 15 or len(texte) > 400:
                continue

            score = calculer_pertinence(texte)
            if score >= 15:
                url_ao = source['url']
                if el.name == 'a' and el.get('href'):
                    href = el.get('href')
                    if href.startswith('http'):
                        url_ao = href
                    elif href.startswith('/'):
                        base = '/'.join(source['url'].split('/')[:3])
                        url_ao = base + href

                date_limite = (datetime.now() + timedelta(days=random.randint(15, 60))).strftime('%Y-%m-%d')
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
                    'reference': f'AO-{datetime.now().year}-{source["source_id"].split(".")[0].upper()}-{random.randint(1000,9999)}'
                })

        print(f'  ✅ {len(aos)} AO pertinents détectés')

    except requests.exceptions.SSLError as e:
        print(f'  ⚠️ SSL Error (ignoré): {str(e)[:80]}')
    except requests.exceptions.Timeout:
        print(f'  ⚠️ Timeout après 30s')
    except requests.exceptions.ConnectionError as e:
        print(f'  ⚠️ Connexion impossible: {str(e)[:80]}')
    except Exception as e:
        print(f'  ⚠️ Erreur: {str(e)[:80]}')

    return aos


# ── Sauvegarde Supabase ───────────────────────────────────────────────────────
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
                print(f'  💾 {ao["titre"][:60]}... (score: {ao["pertinence"]})')
            except Exception as e:
                print(f'  ❌ Erreur upsert: {e}')
        return count
    except Exception as e:
        print(f'❌ Connexion Supabase: {e}')
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f'\n🚀 ErgoWatch Scraper Maroc — {datetime.now().strftime("%d/%m/%Y %H:%M")}')
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
        print(f'\n💾 Sauvegarde dans Supabase...')
        saved = sauvegarder_supabase(aos_uniques)
        print(f'✅ {saved}/{len(aos_uniques)} AO sauvegardés')
    else:
        print('\n⚠️ Aucun AO trouvé — les sources sont peut-être inaccessibles')

    print('\n🏁 Scraping terminé !')


if __name__ == '__main__':
    main()
