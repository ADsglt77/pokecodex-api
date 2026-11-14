-- PokéCodex — importateur TCGdex : symboles des extensions (la petite icône imprimée sur les cartes).
--
-- Uniquement des ajouts de colonnes : aucune table n'est reconstruite, aucun index (la table `sets`
-- ne compte que quelques centaines de lignes). Comme les logos (`0002_media.sql`), ces copies
-- réutilisent le type de tâche `image` (la contrainte CHECK de `import_jobs.job_type` reste intacte),
-- avec leur propre `dedupe_key` `symbol:<langue>:<extension>` et la charge utile `{"media":"symbol"}`.
--
-- Symbole de l'extension (`<symbol_url>.webp`) : `cards/<langue>/<extension>/_symbol.webp`.
-- Même vocabulaire que `logo_status` (NULL = jamais traité) : `downloaded` (objet présent dans R2),
-- `not_found_on_cdn` (404 confirmé), `missing_api_url` (aucune URL source fiable), `download_error`,
-- `invalid_image`. Le site lit `symbol_status` / `symbol_r2_key` : `downloaded` est le seul statut
-- qui signifie « présent » (garanti par la contrainte CHECK).
ALTER TABLE sets ADD COLUMN symbol_status TEXT CHECK (symbol_status IN (
  'downloaded', 'missing_api_url', 'not_found_on_cdn', 'download_error', 'invalid_image'));
ALTER TABLE sets ADD COLUMN symbol_r2_key TEXT;
ALTER TABLE sets ADD COLUMN symbol_checked_at TEXT;
-- URL réellement testée : un symbole absent est revérifié tout de suite si l'API en donne une autre.
ALTER TABLE sets ADD COLUMN symbol_source_url TEXT;
ALTER TABLE sets ADD COLUMN symbol_file_size INTEGER;
ALTER TABLE sets ADD COLUMN symbol_sha256 TEXT;
ALTER TABLE sets ADD COLUMN symbol_failure_reason TEXT;
