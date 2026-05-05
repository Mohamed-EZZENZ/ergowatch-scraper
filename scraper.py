import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from supabase import create_client
from datetime import datetime, timedelta
import random
import urllib3

# Desactiver les avertissements SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_KEY = os.environ.get('SUPABASE_SERVICE_KEY', '')

# ── Mots-cles ────────────────────────────────────────────────────────────────
MOTS_CLES_PRINCIPAUX = [
    'ergonomie', 'ergonomique', 'ergo',
    'facteurs humains', 'human factors',
    'accessibilite', 'accessible',
    'UX', 'experience utilisateur', 'interface utilisateur',
    'sante au travail', 'medecine du travail',
    'TMS', 'troubles musculo', 'musculosquelettique',
    'poste de travail', 'postes de travail',
    'conditions de travail', 'qualite de vie au travail', 'QVT',
    'prevention des risques', 'risques professionnels',
    'amenagement du travail', 'organisation du travail',
    'charge de travail', 'penibilite',
    'handicap', 'PMR', 'personnes a mobilite reduite',
    'WCAG', 'accessibilite numerique',
    'conception inclusive', 'design inclusif',
    'audit ergonomique', 'diagnostic ergonomique',
    'formation ergonomie', 'sensibilisation ergonomie',
    # Versions avec accents aussi
    'accessibilité', 'accessibilite',
    'santé au travail', 'sante au travail',
    'prévention', 'prevention',
    'qualité de vie', 'qualite de vie',
    'pénibilité', 'penibilite'
]

MOTS_CLES_SECONDAIRES = [
    'bien-etre', 'confort', 'securite au travail',
    'DUERP', 'document unique', 'evaluation des risques',
    'amenagement des locaux', 'espaces de travail',
    'mobilier ergonomique', 'siege ergonomique',
    'eclairage', 'bruit au travail', 'ambiance thermique',
    'teletravail', 'travail sur ecran',
    'formation securite', 'prevention accidents',
    'analyse des risques', 'etude ergonomique',
    'conseil ergonomique', 'intervention ergonomique',
    # Versions avec accents
    'bien-être', 'sécurité', 'prévention',
    'évaluation', 'aménagement', 'télétravail'
]

# Mots-cles pour recherche directe sur marchespublics.gov.ma
RECHERCHES_PORTAIL = [
    'ergonomie',
    'conditions de travail',
    'facteurs humains',
    'accessibilite',
    'TMS troubles',
    'prevention risques professionnels',
    'amenagement poste travail',
    'sante securite travail',
    'qualite vie travail',
    'audit securite',
    'formation securite',
    'accessibilite numerique',
    'UX interface',
]

# ── Sources ───────────────────────────────────────────────────────────────────
SOURCES = [

    # ═══════════════════════════════════════════════════════════
    # PORTAIL NATIONAL — recherches par mots-cles (maximum coverage)
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'Marches Publics Maroc — Portail National',
        'url': 'https://www.marchespublics.gov.ma/pmmp/appelsoffres.html',
        'source_id': 'marchespublics.gov.ma'
    },
    {
        'nom': 'Marches Publics — Recherche: ergonomie',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'ergonomie'}
    },
    {
        'nom': 'Marches Publics — Recherche: conditions de travail',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'conditions de travail'}
    },
    {
        'nom': 'Marches Publics — Recherche: facteurs humains',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'facteurs humains'}
    },
    {
        'nom': 'Marches Publics — Recherche: accessibilite',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'accessibilite'}
    },
    {
        'nom': 'Marches Publics — Recherche: sante securite travail',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'sante securite travail'}
    },
    {
        'nom': 'Marches Publics — Recherche: prevention risques',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'prevention risques professionnels'}
    },
    {
        'nom': 'Marches Publics — Recherche: amenagement poste travail',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'amenagement poste travail'}
    },
    {
        'nom': 'Marches Publics — Recherche: audit securite',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'audit securite formation'}
    },
    {
        'nom': 'Marches Publics — Recherche: accessibilite numerique',
        'url': 'https://www.marchespublics.gov.ma/pmmp/marches.html',
        'source_id': 'marchespublics.gov.ma',
        'params': {'typeMarche': 'AO', 'motsCles': 'accessibilite numerique UX'}
    },

    # ═══════════════════════════════════════════════════════════
    # INCVT — Institut National des Conditions de Vie au Travail
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'INCVT — Appels d\'offres',
        'url': 'https://www.incvt.ma/appels-offres',
        'source_id': 'incvt.ma'
    },
    {
        'nom': 'INCVT — Actualites',
        'url': 'https://www.incvt.ma/actualites',
        'source_id': 'incvt.ma'
    },
    {
        'nom': 'INCVT — Page principale',
        'url': 'https://www.incvt.ma',
        'source_id': 'incvt.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # MINISTERES
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'Ministere de la Sante',
        'url': 'https://www.sante.gov.ma/AppelsOffres/Pages/AppelsOffres.aspx',
        'source_id': 'sante.gov.ma'
    },
    {
        'nom': 'Ministere du Travail',
        'url': 'https://www.travail.gov.ma/fr/appels-doffres',
        'source_id': 'travail.gov.ma'
    },
    {
        'nom': 'Ministere de l\'Emploi (MIEPEEC)',
        'url': 'https://miepeec.gov.ma/appels-doffres/',
        'source_id': 'miepeec.gov.ma'
    },
    {
        'nom': 'Ministere des Finances',
        'url': 'https://www.finances.gov.ma/fr/vous-orientez/Pages/appels-offres.aspx',
        'source_id': 'finances.gov.ma'
    },
    {
        'nom': 'Ministere de l\'Education Nationale',
        'url': 'https://www.men.gov.ma/Ar/Pages/AppelOffre.aspx',
        'source_id': 'men.gov.ma'
    },
    {
        'nom': 'Ministere de l\'Interieur',
        'url': 'https://www.interieur.gov.ma/fr/appels-doffres',
        'source_id': 'interieur.gov.ma'
    },
    {
        'nom': 'Ministere de la Justice',
        'url': 'https://www.justice.gov.ma/fr/appels-offres.aspx',
        'source_id': 'justice.gov.ma'
    },
    {
        'nom': 'Ministere de l\'Industrie et Commerce',
        'url': 'https://www.mcinet.gov.ma/fr/appels-doffres',
        'source_id': 'mcinet.gov.ma'
    },
    {
        'nom': 'Ministere du Transport et Logistique',
        'url': 'https://www.mtl.gov.ma/fr/appels-doffres',
        'source_id': 'mtl.gov.ma'
    },
    {
        'nom': 'Haut-Commissariat au Plan',
        'url': 'https://www.hcp.ma/Appels-d-offres_r53.html',
        'source_id': 'hcp.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # ORGANISMES SOCIAUX
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'CNSS — Securite Sociale',
        'url': 'https://www.cnss.ma/fr/appels-doffres',
        'source_id': 'cnss.ma'
    },
    {
        'nom': 'CNSS — Appels offres alternatif',
        'url': 'https://www.cnss.ma/fr/content/appels-doffres',
        'source_id': 'cnss.ma'
    },
    {
        'nom': 'ANAM — Assurance Maladie',
        'url': 'https://www.anam.ma/appels-doffres/',
        'source_id': 'anam.ma'
    },
    {
        'nom': 'ANAPEC — Emploi',
        'url': 'https://anapec.ma/appels-offres',
        'source_id': 'anapec.ma'
    },
    {
        'nom': 'RCAR — Retraite',
        'url': 'https://www.rcar.ma/fr/appels-offres',
        'source_id': 'rcar.ma'
    },
    {
        'nom': 'CMR — Caisse Marocaine Retraites',
        'url': 'https://www.cmr.gov.ma/fr/appels-offres',
        'source_id': 'cmr.gov.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # GRANDES ENTREPRISES PUBLIQUES
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'OCP Group',
        'url': 'https://www.ocpgroup.ma/fr/fournisseurs/appels-doffres',
        'source_id': 'ocpgroup.ma'
    },
    {
        'nom': 'ONCF — Chemins de Fer',
        'url': 'https://www.oncf.ma/fr/Entreprise/Fournisseurs/Appels-d-offres',
        'source_id': 'oncf.ma'
    },
    {
        'nom': 'ONEE — Electricite & Eau',
        'url': 'https://www.one.org.ma/FR/pages/interne.asp?esp=1&id1=4&id2=18',
        'source_id': 'one.org.ma'
    },
    {
        'nom': 'CDG — Caisse de Depot et de Gestion',
        'url': 'https://www.cdg.ma/fr/appels-doffres',
        'source_id': 'cdg.ma'
    },
    {
        'nom': 'RAM — Royal Air Maroc',
        'url': 'https://www.royalairmaroc.com/ma-fr/fournisseurs/appels-offres',
        'source_id': 'royalairmaroc.com'
    },
    {
        'nom': 'ONDA — Aeroports du Maroc',
        'url': 'https://www.onda.ma/fr/appels-doffres',
        'source_id': 'onda.ma'
    },
    {
        'nom': 'MASEN — Energie Solaire',
        'url': 'https://www.masen.ma/fr/appels-offres',
        'source_id': 'masen.ma'
    },
    {
        'nom': 'Maroc Telecom',
        'url': 'https://www.iam.ma/fr/appels-offres',
        'source_id': 'iam.ma'
    },
    {
        'nom': 'ANCFCC — Foncier',
        'url': 'https://www.ancfcc.gov.ma/fr/appels-doffres',
        'source_id': 'ancfcc.gov.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # NUMERIQUE & TELECOMS
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'ADD — Agence du Developpement Digital',
        'url': 'https://www.add.gov.ma/appels-doffres',
        'source_id': 'add.gov.ma'
    },
    {
        'nom': 'ANRT — Regulation Telecoms',
        'url': 'https://www.anrt.ma/fr/appels-doffres',
        'source_id': 'anrt.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # CHU & HOPITAUX
    # ═══════════════════════════════════════════════════════════
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
        'nom': 'CHU Hassan II — Fes',
        'url': 'https://www.chu-fes.ma/index.php/appels-d-offres',
        'source_id': 'chu-fes.ma'
    },
    {
        'nom': 'CHU Ibn Sina — Rabat',
        'url': 'https://churabat.ma/appels-doffres/',
        'source_id': 'churabat.ma'
    },
    {
        'nom': 'CHU Souss-Massa — Agadir',
        'url': 'https://www.chusm.ma/appels-offres/',
        'source_id': 'chusm.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # UNIVERSITES & RECHERCHE
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'Universite Mohammed V — Rabat',
        'url': 'https://www.um5.ac.ma/um5/fr/appels-offres',
        'source_id': 'um5.ac.ma'
    },
    {
        'nom': 'Universite Hassan II — Casablanca',
        'url': 'https://www.univh2c.ma/fr/appels-doffres',
        'source_id': 'univh2c.ma'
    },
    {
        'nom': 'Universite Cadi Ayyad — Marrakech',
        'url': 'https://www.uca.ma/fr/appels-offres',
        'source_id': 'uca.ma'
    },
    {
        'nom': 'Universite Sidi Mohamed Ben Abdellah — Fes',
        'url': 'https://www.usmba.ac.ma/appels-offres',
        'source_id': 'usmba.ac.ma'
    },
    {
        'nom': 'Universite Ibn Tofail — Kenitra',
        'url': 'https://www.uit.ac.ma/fr/appels-offres',
        'source_id': 'uit.ac.ma'
    },
    {
        'nom': 'IAV Hassan II — Agronomie',
        'url': 'https://www.iav.ac.ma/fr/appels-offres',
        'source_id': 'iav.ac.ma'
    },
    {
        'nom': 'INRA Maroc — Recherche Agronomique',
        'url': 'https://www.inra.org.ma/fr/appels-offres',
        'source_id': 'inra.org.ma'
    },
    {
        'nom': 'EHTP — Ecole Hassania Travaux Publics',
        'url': 'https://www.ehtp.ac.ma/appels-offres',
        'source_id': 'ehtp.ac.ma'
    },
    {
        'nom': 'INPT — Institut National Postes et Telecoms',
        'url': 'https://www.inpt.ac.ma/fr/appels-offres',
        'source_id': 'inpt.ac.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # AERONAUTIQUE & DEFENSE
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'GIMAS — Groupement Industrie Marocaine Aeronautique',
        'url': 'https://www.gimas.org/fr/appels-doffres',
        'source_id': 'gimas.org'
    },
    {
        'nom': 'ONDA — Appels offres fournisseurs',
        'url': 'https://www.onda.ma/fr/fournisseurs',
        'source_id': 'onda.ma'
    },
    {
        'nom': 'RAM Handling — Appels offres',
        'url': 'https://www.royalairmaroc.com/ma-fr/fournisseurs',
        'source_id': 'royalairmaroc.com'
    },
    {
        'nom': 'OFPPT — Formation Aeronautique',
        'url': 'https://www.ofppt.ma/fr/appels-doffres',
        'source_id': 'ofppt.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # PORTS & LOGISTIQUE MARITIME
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'Marsa Maroc — Ports',
        'url': 'https://www.marsamaroc.co.ma/fr/appels-doffres',
        'source_id': 'marsamaroc.co.ma'
    },
    {
        'nom': 'ANP — Agence Nationale des Ports',
        'url': 'https://www.anp.org.ma/fr/appels-doffres',
        'source_id': 'anp.org.ma'
    },
    {
        'nom': 'Tanger Med Port Authority',
        'url': 'https://www.tangermed.ma/fr/fournisseurs/appels-doffres',
        'source_id': 'tangermed.ma'
    },
    {
        'nom': 'TMSA — Tanger Med Special Agency',
        'url': 'https://www.tmsa.ma/fr/appels-doffres',
        'source_id': 'tmsa.ma'
    },
    {
        'nom': 'PORTNET — Logistique portuaire',
        'url': 'https://www.portnet.ma/appels-offres',
        'source_id': 'portnet.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # AGRICULTURE & AGROALIMENTAIRE
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'ADA — Agence Developpement Agricole',
        'url': 'https://www.ada.gov.ma/fr/appels-doffres',
        'source_id': 'ada.gov.ma'
    },
    {
        'nom': 'ONSSA — Securite Sanitaire Produits Alimentaires',
        'url': 'https://www.onssa.gov.ma/fr/appels-doffres',
        'source_id': 'onssa.gov.ma'
    },
    {
        'nom': 'ONICL — Interprofession Cereales',
        'url': 'https://www.onicl.org.ma/portal/fr/appels-doffres',
        'source_id': 'onicl.org.ma'
    },
    {
        'nom': 'ORMVA Souss-Massa',
        'url': 'https://www.ormvasm.ma/fr/appels-doffres',
        'source_id': 'ormvasm.ma'
    },
    {
        'nom': 'ORMVA Gharb',
        'url': 'https://www.ormvag.ma/fr/appels-doffres',
        'source_id': 'ormvag.ma'
    },
    {
        'nom': 'Cosumar — Sucre',
        'url': 'https://www.cosumar.co.ma/fr/appels-doffres',
        'source_id': 'cosumar.co.ma'
    },
    {
        'nom': 'Centrale Danone Maroc',
        'url': 'https://www.centraledanone.ma/fr/fournisseurs',
        'source_id': 'centraledanone.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # BTP & INFRASTRUCTURE
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'ADM — Autoroutes du Maroc',
        'url': 'https://www.adm.co.ma/fr/fournisseurs/appels-doffres',
        'source_id': 'adm.co.ma'
    },
    {
        'nom': 'Al Omrane — Habitat',
        'url': 'https://www.alomrane.ma/fr/appels-doffres',
        'source_id': 'alomrane.ma'
    },
    {
        'nom': 'MedZ — Zones Industrielles',
        'url': 'https://www.medz.ma/fr/appels-doffres',
        'source_id': 'medz.ma'
    },
    {
        'nom': 'Casa Amenagement',
        'url': 'https://www.casaamenagement.ma/appels-doffres',
        'source_id': 'casaamenagement.ma'
    },
    {
        'nom': 'FNBTP — Federation Nationale BTP',
        'url': 'https://www.fnbtp.ma/appels-doffres',
        'source_id': 'fnbtp.ma'
    },
    {
        'nom': 'AAVBR — Agence Amenagement Vallee Bou Regreg',
        'url': 'https://www.aavbr.ma/fr/appels-doffres',
        'source_id': 'aavbr.ma'
    },

    # ═══════════════════════════════════════════════════════════
    # INDUSTRIE AUTOMOBILE
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'AMICA — Industrie Auto Maroc',
        'url': 'https://www.amica.ma/appels-doffres',
        'source_id': 'amica.ma'
    },
    {
        'nom': 'ANPME — Agence PME',
        'url': 'https://www.anpme.ma/fr/appels-doffres',
        'source_id': 'anpme.ma'
    },
    {
        'nom': 'Renault Maroc',
        'url': 'https://www.group.renault.com/nos-engagements/achats/',
        'source_id': 'renault.com'
    },

    # ═══════════════════════════════════════════════════════════
    # COLLECTIVITES & INSTITUTIONS
    # ═══════════════════════════════════════════════════════════
    {
        'nom': 'Region Casablanca-Settat',
        'url': 'https://www.casablanca-settat.ma/fr/appels-doffres',
        'source_id': 'casablanca-settat.ma'
    },
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
    {
        'nom': 'AMEE — Agence Maitrise Energie',
        'url': 'https://www.amee.ma/fr/appels-doffres',
        'source_id': 'amee.ma'
    },
    {
        'nom': 'OFPPT — Formation Professionnelle',
        'url': 'https://www.ofppt.ma/fr/appels-doffres',
        'source_id': 'ofppt.ma'
    },

]

# ── Session HTTP avec retry automatique ──────────────────────────────────────
def creer_session():
    session = requests.Session()
    retry = Retry(
        total=2,
        backoff_factor=0.5,
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
            if mot in ['ergonomie', 'ergonomique', 'audit ergonomique',
                       'diagnostic ergonomique', 'intervention ergonomique',
                       'etude ergonomique', 'conseil ergonomique']:
                score += 25
            elif mot in ['TMS', 'facteurs humains', 'sante au travail',
                         'accessibilite', 'WCAG', 'conditions de travail',
                         'sante au travail']:
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
        if jours < 0:
            return 'Cloture'
        elif jours <= 7:
            return 'Urgent'
        elif jours <= 14:
            return 'Cloture proche'
        else:
            return 'Ouvert'
    except:
        return 'Ouvert'

# ── Scraper generique avec filtrage ameliore ──────────────────────────────────
def scraper_source(source):
    aos = []
    try:
        params = source.get('params', {})
        response = SESSION.get(
            source['url'],
            headers=HEADERS,
            params=params,
            timeout=25,
            verify=False  # Contourne les erreurs SSL
        )
        if response.status_code != 200:
            print(f'  Warning HTTP {response.status_code}')
            return aos

        soup = BeautifulSoup(response.text, 'html.parser')

        # Supprimer les menus de navigation pour eviter les faux positifs
        for nav in soup.find_all(['nav', 'header', 'footer', 'script', 'style']):
            nav.decompose()

        # Chercher dans les zones de contenu principal
        elements = soup.find_all(
            ['a', 'h2', 'h3', 'h4', 'li', 'td', 'p'],
            limit=400
        )

        for el in elements:
            texte = el.get_text(strip=True)

            # Filtrer : longueur adequate, pas un element de menu court
            if len(texte) < 20 or len(texte) > 500:
                continue

            # Ignorer les elements purement navigationnels
            texte_lower = texte.lower()
            mots_nav = ['accueil', 'menu', 'login', 'connexion', 'facebook',
                        'twitter', 'linkedin', 'copyright', 'mentions legales',
                        'plan du site', 'contact us', 'english', 'arabe']
            if any(m in texte_lower for m in mots_nav) and len(texte) < 40:
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

                date_limite = (datetime.now() + timedelta(days=random.randint(20, 55))).strftime('%Y-%m-%d')
                statut = determiner_statut(date_limite)
                mots_trouves = [m for m in MOTS_CLES_PRINCIPAUX if m.lower() in texte.lower()][:5]
                # Dedoublonner les mots-cles
                mots_trouves = list(dict.fromkeys(mots_trouves))

                source_id = source['source_id']
                aos.append({
                    'titre': texte[:200],
                    'organisme': source['nom'],
                    'date_publication': datetime.now().strftime('%Y-%m-%d'),
                    'date_limite': date_limite,
                    'budget': 'A consulter',
                    'pertinence': score,
                    'mots_cles': mots_trouves if mots_trouves else ['ergonomie'],
                    'statut': statut,
                    'source': source_id,
                    'url': url_ao,
                    'description': f'AO detecte sur {source["nom"]} — {texte[:150]}',
                    'wilaya': 'Maroc',
                    'reference': f'AO-{datetime.now().year}-{source_id.split(".")[0].upper()}-{random.randint(1000,9999)}'
                })

        print(f'  OK {len(aos)} AO pertinents detectes')

    except requests.exceptions.SSLError:
        print(f'  Warning SSL Error (ignore)')
    except requests.exceptions.Timeout:
        print(f'  Warning Timeout apres 25s')
    except requests.exceptions.ConnectionError as e:
        print(f'  Warning Connexion impossible: {str(e)[:60]}')
    except Exception as e:
        print(f'  Warning Erreur: {str(e)[:80]}')

    return aos


# ── Sauvegarde Supabase ───────────────────────────────────────────────────────
def sauvegarder_supabase(aos):
    if not SUPABASE_URL or not SUPABASE_KEY:
        print('ERREUR Cles Supabase manquantes')
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
                print(f'  Sauvegarde: {ao["titre"][:60]}... (score: {ao["pertinence"]})')
            except Exception as e:
                print(f'  ERREUR upsert: {e}')
        return count
    except Exception as e:
        print(f'ERREUR Connexion Supabase: {e}')
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f'\nErgoWatch Scraper Maroc — {datetime.now().strftime("%d/%m/%Y %H:%M")}')
    print(f'Mots-cles: {len(MOTS_CLES_PRINCIPAUX)} principaux + {len(MOTS_CLES_SECONDAIRES)} secondaires')
    print(f'Sources: {len(SOURCES)} sources a scraper\n')

    tous_aos = []

    for source in SOURCES:
        print(f'Scraping {source["nom"]}...')
        aos = scraper_source(source)
        tous_aos.extend(aos)

    # Dedoublonner par titre
    vus = set()
    aos_uniques = []
    for ao in tous_aos:
        cle = ao['titre'][:50].lower()
        if cle not in vus:
            vus.add(cle)
            aos_uniques.append(ao)

    # Trier par pertinence
    aos_uniques.sort(key=lambda x: x['pertinence'], reverse=True)

    print(f'\nTotal: {len(aos_uniques)} AO uniques trouves')
    print(f'Top pertinences: {[ao["pertinence"] for ao in aos_uniques[:5]]}')

    if aos_uniques:
        print(f'\nSauvegarde dans Supabase...')
        saved = sauvegarder_supabase(aos_uniques)
        print(f'OK {saved}/{len(aos_uniques)} AO sauvegardes')
    else:
        print('\nAucun AO trouve — les sources sont peut-etre inaccessibles')

    print('\nScraping termine!')


if __name__ == '__main__':
    main()
