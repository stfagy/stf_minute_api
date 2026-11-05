# Cloudflare Worker — /fretboard

API autonome sans accès PostgreSQL. Le Worker conserve le contrat de sortie `board` et `scale_notes`.

## Installation
Prérequis : compte Cloudflare, Node.js et `uv`.
Dans `cloudflare/` : `uv run pywrangler dev`, puis tester `http://127.0.0.1:8787/fretboard?root=C&pattern=0&pattern=4&pattern=7&tuning=E&tuning=A&tuning=D&tuning=G&tuning=B&tuning=E&frets=12`.
Pour déployer avec le compte Cloudflare autorisé : `uv run pywrangler deploy`.

Ne pas remplacer l'URL du visualiseur avant de comparer la sortie du Worker à celle de Render et de mesurer les temps de réponse.
L'ancien service Render est conservé comme solution de repli.
