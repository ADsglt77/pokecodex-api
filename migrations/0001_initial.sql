-- PokéCodex — importateur TCGdex : schéma initial de la base D1 `pokecodex-cards`.
--
-- Conventions :
--   * les identifiants internes sont `<langue>:<id TCGdex>` (ex. `fr:swsh3-136`, `ja:sv1a`) :
--     une carte française et une carte japonaise ne se confondent jamais ;
--   * toutes les dates sont des chaînes ISO 8601 UTC au format fixe `YYYY-MM-DDTHH:MM:SS.sssZ`,
--     qui se comparent correctement par ordre lexicographique ;
--   * `content_hash` / `brief_hash` / `detail_hash` : SHA-256 du JSON source, pour ne réécrire une
--     ligne que si la donnée TCGdex a réellement changé (économie d'écritures D1).

-- Séries (ex. « Épée et Bouclier »), par langue.
CREATE TABLE series (
  id            TEXT PRIMARY KEY,
  tcgdex_id     TEXT NOT NULL,
  language      TEXT NOT NULL CHECK (language IN ('fr', 'ja')),
  name          TEXT NOT NULL,
  logo_url      TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  content_hash  TEXT,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  UNIQUE (language, tcgdex_id)
);

-- Extensions (ex. « Ténèbres Embrasées »), par langue.
CREATE TABLE sets (
  id                TEXT PRIMARY KEY,
  tcgdex_id         TEXT NOT NULL,
  series_id         TEXT REFERENCES series (id),
  language          TEXT NOT NULL CHECK (language IN ('fr', 'ja')),
  name              TEXT NOT NULL,
  release_date      TEXT,
  total_cards       INTEGER,
  official_cards    INTEGER,
  logo_url          TEXT,
  symbol_url        TEXT,
  metadata_json     TEXT NOT NULL DEFAULT '{}',
  content_hash      TEXT,
  detail_fetched_at TEXT,
  created_at        TEXT NOT NULL,
  updated_at        TEXT NOT NULL,
  UNIQUE (language, tcgdex_id)
);
CREATE INDEX sets_series ON sets (series_id);

-- Cartes, par langue. Les colonnes `image_*` suivent la copie de l'image vers R2.
CREATE TABLE cards (
  id                   TEXT PRIMARY KEY,
  tcgdex_id            TEXT NOT NULL,
  language             TEXT NOT NULL CHECK (language IN ('fr', 'ja')),
  name                 TEXT NOT NULL,
  local_id             TEXT NOT NULL,
  set_id               TEXT REFERENCES sets (id),
  -- Id TCGdex de la série (ex. `swsh`, sans préfixe de langue), dénormalisé pour le chemin du CDN :
  -- vient du détail de l'extension ou, à défaut, de l'URL du logo de l'extension ; pas de clé étrangère.
  series_id            TEXT,
  rarity               TEXT,
  category             TEXT,
  illustrator          TEXT,
  -- Base d'URL donnée par l'API (sans qualité ni extension), NULL si l'API n'en donne pas.
  api_image_url        TEXT,
  -- URL complète réellement téléchargée.
  image_source_url     TEXT,
  -- `api` : URL officielle ; `cdn_reconstructed` : URL reconstruite et trouvée sur le CDN.
  image_source         TEXT CHECK (image_source IN ('api', 'cdn_reconstructed')),
  image_r2_key         TEXT,
  image_status         TEXT NOT NULL DEFAULT 'pending' CHECK (image_status IN (
                         'pending', 'downloaded', 'recovered_from_cdn', 'already_present',
                         'missing_api_url', 'not_found_on_cdn', 'download_error', 'invalid_image')),
  image_quality        TEXT,
  image_file_size      INTEGER,
  image_sha256         TEXT,
  image_downloaded_at  TEXT,
  -- Dernière vérification (réussie ou non), URL testées (JSON [{url, status}]), dernier code HTTP
  -- et motif d'échec : de quoi identifier précisément chaque image manquante.
  image_checked_at     TEXT,
  image_tested_urls    TEXT,
  image_http_status    INTEGER,
  image_failure_reason TEXT,
  brief_hash           TEXT,
  detail_hash          TEXT,
  detail_status        TEXT NOT NULL DEFAULT 'pending' CHECK (detail_status IN (
                         'pending', 'fetched', 'not_found', 'error')),
  detail_fetched_at    TEXT,
  metadata_json        TEXT NOT NULL DEFAULT '{}',
  -- La carte n'est plus listée par TCGdex (elle reste en base, avec son image).
  removed_at           TEXT,
  created_at           TEXT NOT NULL,
  updated_at           TEXT NOT NULL,
  UNIQUE (language, tcgdex_id)
);
CREATE INDEX cards_set ON cards (set_id);
CREATE INDEX cards_language_status ON cards (language, image_status);
-- Index couvrant du volume stocké (contrôle du plafond à chaque exécution active, sans relire la table).
CREATE INDEX cards_stored ON cards (image_status, image_file_size);

-- Synchronisations du catalogue (initiale, hebdomadaire ou manuelle), une ligne par langue.
CREATE TABLE sync_runs (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  language               TEXT NOT NULL CHECK (language IN ('fr', 'ja')),
  kind                   TEXT NOT NULL CHECK (kind IN ('initial', 'weekly', 'manual')),
  scope_json             TEXT,
  started_at             TEXT NOT NULL,
  finished_at            TEXT,
  status                 TEXT NOT NULL DEFAULT 'running' CHECK (status IN (
                           'running', 'completed', 'cancelled')),
  series_found           INTEGER,
  sets_found             INTEGER,
  cards_found            INTEGER,
  cards_processed        INTEGER,
  images_downloaded      INTEGER,
  images_recovered       INTEGER,
  images_already_present INTEGER,
  images_missing         INTEGER,
  images_failed          INTEGER,
  bytes_stored           INTEGER
);
CREATE INDEX sync_runs_status ON sync_runs (status, language);
-- Une seule synchronisation en cours par langue, garantie par la base (Cron et admin concurrents).
CREATE UNIQUE INDEX sync_runs_one_running ON sync_runs (language) WHERE status = 'running';

-- File des tâches persistantes. Une ligne par cible (`dedupe_key` unique) : relancer une
-- synchronisation réutilise la ligne au lieu d'en créer une nouvelle.
CREATE TABLE import_jobs (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  job_type         TEXT NOT NULL CHECK (job_type IN (
                     'catalog', 'set', 'card_index', 'card', 'image', 'report')),
  language         TEXT CHECK (language IN ('fr', 'ja')),
  target_id        TEXT,
  card_id          TEXT,
  dedupe_key       TEXT NOT NULL UNIQUE,
  priority         INTEGER NOT NULL,
  status           TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
                     'pending', 'running', 'done', 'failed', 'cancelled')),
  attempts         INTEGER NOT NULL DEFAULT 0,
  max_attempts     INTEGER NOT NULL,
  -- Nombre de fois où la tâche a été rendue sans consommer de tentative (429, dépendance pas
  -- prête) ; au-delà d'un plafond, un rendu consomme une tentative : jamais de boucle infinie.
  releases         INTEGER NOT NULL DEFAULT 0,
  last_error       TEXT,
  last_http_status INTEGER,
  next_attempt_at  TEXT NOT NULL,
  locked_until     TEXT,
  lock_token       TEXT,
  run_id           INTEGER REFERENCES sync_runs (id),
  payload_json     TEXT,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  completed_at     TEXT
);
-- Index partiels : seules les tâches en attente ou en cours y figurent, ce qui limite les lignes
-- lues à chaque réservation et les écritures d'index des tâches terminées.
CREATE INDEX import_jobs_ready ON import_jobs (priority, next_attempt_at, id) WHERE status = 'pending';
CREATE INDEX import_jobs_locked ON import_jobs (locked_until) WHERE status = 'running';
CREATE INDEX import_jobs_active_run ON import_jobs (run_id) WHERE status IN ('pending', 'running');

-- Garde-fou des écritures d'une tâche : chaque batch d'écritures commence par une insertion dans
-- cette table, qui n'insère rien si la tâche est toujours verrouillée par l'exécution qui écrit, et
-- insère `0` sinon : la contrainte CHECK échoue alors et D1 annule tout le batch. Une exécution qui
-- a perdu son verrou ne peut donc rien écrire. La table reste toujours vide.
CREATE TABLE job_lock_guard (
  ok INTEGER NOT NULL CHECK (ok = 1)
);

-- Exécutions du traitement (Cron ou déclenchement manuel) qui ont eu du travail.
CREATE TABLE tick_runs (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  trigger       TEXT NOT NULL,
  started_at    TEXT NOT NULL,
  finished_at   TEXT,
  jobs_claimed  INTEGER NOT NULL DEFAULT 0,
  jobs_done     INTEGER NOT NULL DEFAULT 0,
  jobs_retried  INTEGER NOT NULL DEFAULT 0,
  jobs_failed   INTEGER NOT NULL DEFAULT 0,
  jobs_released INTEGER NOT NULL DEFAULT 0,
  jobs_lost     INTEGER NOT NULL DEFAULT 0,
  http_requests INTEGER NOT NULL DEFAULT 0,
  d1_queries    INTEGER NOT NULL DEFAULT 0,
  bytes_stored  INTEGER NOT NULL DEFAULT 0,
  stop_reason   TEXT,
  error         TEXT
);
CREATE INDEX tick_runs_started ON tick_runs (started_at);

-- État d'exécution clé → valeur : verrou de l'exécuteur, pause, attente imposée (429),
-- compteurs quotidiens d'opérations, dernière exécution.
CREATE TABLE runtime_state (
  key        TEXT PRIMARY KEY,
  value      TEXT,
  expires_at TEXT,
  updated_at TEXT NOT NULL
);

-- Préparation future (prix et liens marketplaces) : NON alimentée par cette version.
-- Aucune donnée de marché n'est inventée ni collectée ici.
CREATE TABLE card_marketplace_links (
  id                     INTEGER PRIMARY KEY AUTOINCREMENT,
  card_id                TEXT NOT NULL REFERENCES cards (id),
  marketplace            TEXT NOT NULL,
  marketplace_product_id TEXT,
  marketplace_url        TEXT,
  -- Prix en centimes d'euro (entier exact, pas de flottant).
  price_eur_cents        INTEGER,
  price_updated_at       TEXT,
  created_at             TEXT NOT NULL,
  updated_at             TEXT NOT NULL,
  UNIQUE (card_id, marketplace)
);
