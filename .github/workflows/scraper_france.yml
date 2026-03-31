name: ErgoWatch France — Scraper automatique

on:
  schedule:
    - cron: '0 6 * * *'
    - cron: '0 17 * * *'
  workflow_dispatch:

jobs:
  scraper-france:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - name: Récupérer le code
        uses: actions/checkout@v4

      - name: Configurer Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Installer les dépendances
        run: pip install -r requirements.txt

      - name: Lancer le scraper France
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL_FRANCE }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY_FRANCE }}
        run: python scraper_france.py
