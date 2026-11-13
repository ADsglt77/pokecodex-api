-- PokéCodex — importateur TCGdex : petites images des cartes (`low.webp`) et logos des extensions.
--
-- Uniquement des ajouts : aucune table n'est reconstruite (la contrainte CHECK de
-- `import_jobs.job_type` reste intacte ; ces copies réutilisent le type de tâche `image`, avec leurs
-- propres `dedupe_key` `image_low:<langue>:<carte>` et `logo:<langue>:<extension>`).
--
-- Statuts (NULL = jamais traité) : `downloaded` (objet présent dans R2), `not_found_on_cdn` (404
-- confirmé), `missing_api_url` (aucune URL source fiable), `download_error`, `invalid_image`.
-- Le site lit `image_low_status` / `image_low_r2_key` et `logo_status` / `logo_r2_key` :
-- `downloaded` est le seul statut qui signifie « présent » (garanti par les contraintes CHECK).

-- Petite image de la carte : `cards/<langue>/<extension>/<carte>.low.webp`, ou la clé de l'image
-- principale quand celle-ci est déjà en qualité `low` (même objet, alors `image_low_file_size` NULL :
-- ses octets sont déjà comptés par `image_file_size`).
ALTER TABLE cards ADD COLUMN image_low_status TEXT CHECK (image_low_status IN (
  'downloaded', 'missing_api_url', 'not_found_on_cdn', 'download_error', 'invalid_image'));
ALTER TABLE cards ADD COLUMN image_low_r2_key TEXT;
ALTER TABLE cards ADD COLUMN image_low_checked_at TEXT;
ALTER TABLE cards ADD COLUMN image_low_file_size INTEGER;
ALTER TABLE cards ADD COLUMN image_low_sha256 TEXT;
ALTER TABLE cards ADD COLUMN image_low_failure_reason TEXT;

-- Logo de l'extension (`<logo_url>.webp`) : `cards/<langue>/<extension>/_logo.webp`.
ALTER TABLE sets ADD COLUMN logo_status TEXT CHECK (logo_status IN (
  'downloaded', 'missing_api_url', 'not_found_on_cdn', 'download_error', 'invalid_image'));
ALTER TABLE sets ADD COLUMN logo_r2_key TEXT;
ALTER TABLE sets ADD COLUMN logo_checked_at TEXT;
-- URL réellement testée : un logo absent est revérifié tout de suite si l'API en donne une autre.
ALTER TABLE sets ADD COLUMN logo_source_url TEXT;
ALTER TABLE sets ADD COLUMN logo_file_size INTEGER;
ALTER TABLE sets ADD COLUMN logo_sha256 TEXT;
ALTER TABLE sets ADD COLUMN logo_failure_reason TEXT;

-- Index couvrant du volume des petites images : le plafond de stockage (MAX_STORED_BYTES) est
-- contrôlé à chaque exécution active, sans relire toute la table `cards` (comme `cards_stored`).
-- Simple création d'index : aucune reconstruction de table.
CREATE INDEX cards_low_stored ON cards (image_low_status, image_low_file_size);
