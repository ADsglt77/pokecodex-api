# PokéCodex — importateur TCGdex FR/JP

Worker Cloudflare autonome qui construit la base de cartes de PokéCodex à partir de
[TCGdex](https://tcgdex.dev) : toutes les séries, extensions et cartes **françaises** et
**japonaises** répertoriées par TCGdex, avec leurs images `high.webp` copiées directement de TCGdex
vers R2 (TCGdex → Worker → R2, sans passer par un ordinateur). Pour l'affichage du site, il copie
aussi la petite image `low.webp` de chaque carte (grilles), ainsi que le logo et le symbole (la petite
icône imprimée sur les cartes) de chaque extension qui en a un.

L'application est **indépendante** du site `pokecodex.fr` : son propre Worker (`pokecodex-importer`),
sa propre base D1 (`pokecodex-cards`), aucune route ni domaine du site. Elle réutilise seulement le
bucket R2 existant `pokedex-data`, où elle n'écrit que sous `cards/` (images) et `reports/` (rapports).

> Les pourcentages de couverture portent uniquement sur les cartes répertoriées par TCGdex : TCGdex
> ne contient pas forcément toutes les cartes Pokémon jamais publiées.

## Architecture

```
            Cron */5 min ──► scheduled() ──► runTick()  ◄── POST /admin/tick (manuel)
            Cron lundi 03:07 UTC ──► startSync('weekly')
                                          │
          ┌───────────────────────────────┼────────────────────────────────┐
          ▼                               ▼                                ▼
   D1 pokecodex-cards               api.tcgdex.net/v2                assets.tcgdex.net
   (catalogue, tâches,              (séries, extensions,             (high.webp, low.webp,
    verrous, bilans)                 cartes)                          logos, symboles)
                                                                            │
                                                                            ▼
                                                              R2 pokedex-data / cards/<langue>/…
```

- **`src/index.ts`** : handlers `fetch` (administration) et `scheduled` (Cron).
- **`src/services/runner.ts`** : l'exécuteur. Une exécution prend un verrou (une seule à la fois),
  récupère les tâches dont le verrou a expiré, réserve des lots de tâches, les traite (4 voies en
  parallèle, mais 250 ms minimum entre deux requêtes vers TCGdex, soit ≈ 4 requêtes/s au plus), applique les écritures de chaque lot dans un batch
  D1 (transaction) qui renouvelle aussi son verrou et compte les opérations, puis s'arrête avant ses
  budgets (4 min, 900 tâches, 850 requêtes D1, 5 000 sous-requêtes ; la taille des lots s'ajuste à
  ce qui reste). Le Cron suivant reprend la suite.
- **Tâches** (`import_jobs`), par ordre de priorité :
  `catalog` (séries + extensions d'une langue) → `set` (détail d'une extension et de ses cartes) →
  `card_index` (cartes de `/cards` absentes des extensions) → `report` → `image` (copie vers R2) →
  `card` (détail : rareté, catégorie, illustrateur, JSON brut) → copies secondaires (petite image
  `low.webp` d'une carte, logo et symbole d'une extension). Les images passent avant les détails pour
  que les premières copies arrivent dès le début ; les copies secondaires passent après tout le reste
  (priorité 6). Ce sont des tâches de type `image` (la contrainte CHECK de `job_type` n'est pas
  reconstruite), reconnues à leur charge utile `{"media":"low"}` / `{"media":"logo"}` /
  `{"media":"symbol"}` et à leur `dedupe_key` : `image_low:<langue>:<carte>`,
  `logo:<langue>:<extension>`, `symbol:<langue>:<extension>`.
- **`src/services/downloader.ts`** : la copie d'une image ; **`src/services/media.ts`** : petites
  images, logos et symboles ; **`src/services/catalog.ts`**, **`cards.ts`** : catalogue et détails ;
  **`reports.ts`**, **`stats.ts`** : audit, exports, statistiques ; **`sync.ts`** : démarrer, pause,
  reprise, relance, rattrapage des petites images, logos et symboles.
- **`src/repositories/`** : accès D1 (requêtes paramétrées ; écritures en masse par un seul
  paramètre JSON `json_each`, sous la limite de 100 paramètres de D1).
- **`src/tcgdex/`** : client HTTP (débit, délais, relances courtes), parseurs validés, URL d'images
  et validation WebP.

## Configuration Cloudflare

| Ressource | Nom | Rôle |
| --- | --- | --- |
| Worker | `pokecodex-importer` | administration + Cron, adresse `*.workers.dev` uniquement |
| D1 | `pokecodex-cards` (WEUR, `7a534301-31ff-4d62-b42c-f5d1e3800e51`) | catalogue, tâches, bilans |
| R2 | `pokedex-data` (existant, **privé**) | `cards/…` (images, petites images, logos, symboles), `reports/…` (rapports) |
| Cron | `*/5 * * * *` | traitement des tâches |
| Cron | `7 3 * * MON` | synchronisation hebdomadaire du catalogue (hors des multiples de 5) |
| Secret | `ADMIN_TOKEN` | jeton de l'API d'administration |

Bindings (`wrangler.jsonc`) : `DB` (D1), `BUCKET` (R2). Toutes les limites applicatives sont des
`vars` documentées dans `wrangler.jsonc` (débit, budgets, tentatives, plafonds de coût).

> Chez Cloudflare, les jours numériques des Cron vont de 1 = dimanche à 7 = samedi : on écrit `MON`.
> Les variables `PROCESS_CRON` et `SYNC_CRON` doivent rester identiques aux `triggers.crons`.

## Installation locale

Prérequis : Node.js ≥ 22 (développé avec Node 24 ARM64 sous Windows ; `workerd` tourne en émulation
x64). Wrangler, TypeScript et Vitest sont des dépendances locales.

```bash
npm install
cp .dev.vars.example .dev.vars   # puis mettre un ADMIN_TOKEN local d'au moins 32 caractères
npm run db:migrate:local
npm run dev                      # wrangler dev --test-scheduled
```

Sous npm 11, les scripts d'installation de `workerd` et `esbuild` doivent être autorisés une fois
(`npm install-scripts approve workerd esbuild`), sinon le binaire `workerd` n'est pas posé.

## Commandes npm

| Commande | Effet |
| --- | --- |
| `npm run typecheck` | TypeScript strict (`tsc --noEmit`) |
| `npm test` | tests Vitest dans workerd (D1/R2 simulés, TCGdex simulé, aucun réseau) |
| `npm run test:live` | test d'intégration réel contre l'API et le CDN de TCGdex (≈ 15 requêtes, aucune écriture) |
| `npm run check` | typecheck + tests |
| `npm run db:migrate:local` / `db:migrate:remote` | migrations D1 |
| `npm run deploy` | migrations distantes puis `wrangler deploy` |

## Déploiement

**Workers Builds** (Cloudflare relié au dépôt GitHub, comme `pokedex` et `pokedex-data`) déploie le
Worker à chaque push sur `main` : `npm run ci` (TypeScript strict et tous les tests ; un échec
arrête là, la version d'avant continue), puis `npm run deploy` (les migrations D1 distantes, puis
le Worker). Réglages du tableau de bord (Workers & Pages › `pokecodex-importer` › Settings ›
Build) : dépôt `ADsglt77/pokecodex-api`, branche `main`, construction `npm run ci`, déploiement
`npm run deploy`, variable de construction `NODE_VERSION` = `24` ; le token de construction doit
avoir « D1 : Edit » (les migrations). Une migration part donc avec le push qui l'ajoute : seulement
des ajouts (`ADD COLUMN`, index), que l'ancien code supporte.

À la main, si besoin (et la première fois, le jeton) :

```bash
npx wrangler login                       # une fois
npm run check
npm run deploy                           # migrations D1 distantes + déploiement du Worker
npx wrangler secret put ADMIN_TOKEN      # jeton aléatoire d'au moins 32 caractères
```

Le jeton d'administration de ce déploiement est rangé localement dans
`.secrets/admin-token.txt` (ignoré par Git). Ne jamais le committer ni le coller dans un ticket.

Petites images et logos (migration `0002_media.sql`, uniquement des `ALTER TABLE … ADD COLUMN` et un
index) : appliquer la migration **avant** de déployer le code qui lit ces colonnes, entre deux Cron
(hors d'une exécution en cours, voir `GET /admin/status`), puis appeler une fois
`POST /admin/sync/backfill-media` pour le catalogue déjà importé.

Symboles d'extension (migration `0003_set_symbols.sql`, uniquement des `ALTER TABLE sets ADD COLUMN`) :
même procédure. `npm run deploy` applique la migration avant `wrangler deploy` ; la faire entre deux
Cron, puis appeler une fois `POST /admin/sync/backfill-media` (la réponse compte les symboles dans
`symbols` ; une copie déjà faite n'est jamais reprise).

## D1 (`pokecodex-cards`)

Migrations dans `migrations/`. Tables :

- `series`, `sets`, `cards` : une ligne par langue (`id` = `<langue>:<id TCGdex>`, ex. `fr:swsh3-136`,
  unicité `(language, tcgdex_id)`) ; une carte FR et une carte JP ne sont jamais confondues.
  `metadata_json` garde le JSON brut de TCGdex **sans les prix** (données de marché volatiles) mais
  avec les identifiants produits (`thirdParty.cardmarket`), utiles pour la suite.
- `cards.image_*` : statut, clé R2, qualité, taille, SHA-256, date, URL testées, code HTTP et motif
  d'échec.
- `cards.image_low_*` (migration `0002_media.sql`) : petite image `low.webp` — `image_low_status`,
  `image_low_r2_key`, `image_low_checked_at`, `image_low_file_size` (NULL quand la petite image
  réutilise l'objet principal), `image_low_sha256`, `image_low_failure_reason`.
- `sets.logo_*` (même migration) : logo de l'extension — `logo_status`, `logo_r2_key`,
  `logo_checked_at`, `logo_source_url` (URL testée), `logo_file_size`, `logo_sha256`,
  `logo_failure_reason`.
- `sets.symbol_*` (migration `0003_set_symbols.sql`) : symbole de l'extension, mêmes colonnes —
  `symbol_status`, `symbol_r2_key`, `symbol_checked_at`, `symbol_source_url` (URL testée),
  `symbol_file_size`, `symbol_sha256`, `symbol_failure_reason`.
- Contrat avec le site (API de données) : il lit `image_low_status`, `image_low_r2_key`,
  `logo_status`, `logo_r2_key`, `symbol_status` et `symbol_r2_key` ; **`downloaded` est le seul
  statut qui signifie « présent »** (contraintes CHECK : `downloaded`, `not_found_on_cdn`,
  `missing_api_url`, `download_error`, `invalid_image` ; NULL = jamais traité).
- `import_jobs` : la file persistante (une ligne par cible, `dedupe_key` unique, verrou expirant).
- `sync_runs` : une synchronisation par langue, avec son bilan réel à la clôture.
- `tick_runs` : les exécutions qui ont eu du travail ; `runtime_state` : verrou, pause, attente
  imposée, compteurs quotidiens.
- `card_marketplace_links` : structure **vide** pour les futurs prix et liens marketplaces
  (`marketplace`, `marketplace_product_id`, `marketplace_url`, `price_eur_cents`, `price_updated_at`).

Les écritures ne réécrivent une ligne que si son empreinte (`*_hash`) a changé ; relancer une
synchronisation ne crée aucun doublon.

## R2 (`pokedex-data`)

```
pokedex-data/
├── cards/
│   ├── fr/<extension>/<carte>.webp      ex. cards/fr/swsh3/swsh3-136.webp       (high.webp)
│   ├── fr/<extension>/<carte>.low.webp  ex. cards/fr/swsh3/swsh3-136.low.webp   (low.webp)
│   ├── fr/<extension>/_logo.webp        ex. cards/fr/swsh3/_logo.webp           (logo)
│   ├── fr/<extension>/_symbol.webp      ex. cards/fr/sv03.5/_symbol.webp        (symbole)
│   └── ja/<extension>/<carte>.webp      ex. cards/ja/SV1a/SV1a-001.webp (et .low.webp, _symbol.webp)
└── reports/
    ├── latest/{summary.json, summary.csv, missing_fr.csv, missing_ja.csv, errors.csv}
    └── history/<date>/summary.{json,csv}
```

Les segments de clé sont encodés de façon sûre et réversible : seuls `A-Z a-z 0-9 . -` restent tels
quels, tout autre octet devient `_XX` (ex. `exu-%3F` → `exu-_253F`, `SM1+` → `SM1_2B`). La clé de
la petite image est celle de l'image principale, `.webp` remplacé par `.low.webp` ; si l'image
principale est déjà en qualité `low` (secours), la petite image réutilise la même clé, sans copie.
Les segments `_logo` et `_symbol` ne peuvent pas venir d'une carte : l'encodage n'écrit `_` que
suivi de deux chiffres hexadécimaux majuscules (ni `l` ni `s` n'en sont).

Chaque image est lue en flux dans une limite de taille, gardée en mémoire le temps de la validation
(≈ 90 Ko en moyenne), validée (statut 200, type `image/webp`, signature `RIFF…WEBP`, taille RIFF,
longueur annoncée, **MD5 = ETag** du CDN), puis écrite dans R2 avec le contrôle `sha256` de R2
(toujours avec le type `image/webp`). Seule exception au type : le CDN sert les symboles
d'extension en `application/octet-stream` (sondé le 14/11/2026), accepté pour eux seuls ; la
signature, les tailles et le MD5 restent exigés. D1
n'est mis à jour qu'une fois l'écriture confirmée. Aucun téléchargement corrompu ne peut donc écraser
un objet. Les images ne sont ni recompressées ni modifiées, sauf les logos et symboles copiés par
`POST /admin/import/logo` et `/admin/import/symbol` reçus en PNG (ou de plus de 2 048 px pour un logo,
512 px pour un symbole), que la liaison Images convertit en WebP sans perte : pour eux, `logo_sha256` /
`symbol_sha256` est l'empreinte du WebP stocké, celle du fichier d'origine est dans la métadonnée R2
`sourceSha256`. Le bucket reste **privé** : aucun domaine
public n'est configuré (les images sont des œuvres protégées).

## Images manquantes

1. **URL officielle** (champ `image` de l'API) : `high.webp`, puis `low.webp` en secours (qualité
   réellement obtenue enregistrée).
2. **Pas de champ `image`** : URL reconstruite
   `https://assets.tcgdex.net/{langue}/{série}/{extension}/{encodeURIComponent(numéro)}/high.webp`,
   avec la casse exacte de l'API ; une seule URL par carte, uniquement si série, extension et numéro
   sont connus de façon fiable. Le sondage du CDN (13/11/2026) n'a trouvé aucune variante utile
   (zéros de tête, casse, autre langue) : aucun balayage du CDN.
3. **Introuvable** : statut `not_found_on_cdn` (404 confirmé) ou `missing_api_url` (reconstruction
   impossible), avec l'URL testée, le code HTTP et le motif. Revérifié au plus tous les 30 jours.

Statuts : `pending`, `downloaded`, `recovered_from_cdn`, `already_present`, `missing_api_url`,
`not_found_on_cdn`, `download_error`, `invalid_image`.

## Petites images, logos et symboles

- **Petite image** (pour les grilles du site) : pour chaque carte dont l'image principale est valide
  (`downloaded`, `recovered_from_cdn`, `already_present`), `low.webp` de la même base que
  `image_source_url` (qualité `low` au lieu de `high`), copiée à `….low.webp`. Image principale
  déjà en `low` : enregistrée telle quelle comme petite image (même clé, aucun téléchargement).
- **Logo** : pour chaque extension dont l'API donne un `logo` (beaucoup d'extensions FR, aucune JP),
  `<logo>.webp`, copié à `cards/<langue>/<extension>/_logo.webp` ; seulement sur assets.tcgdex.net,
  dans la langue de l'extension (ou `univ`).
- **Symbole** (la petite icône imprimée sur les cartes) : pour chaque extension dont l'API donne un
  `symbol` (153 extensions FR et 4 JP au 14/11/2026, souvent sous `univ`, ex.
  `https://assets.tcgdex.net/univ/sv/sv03.5/symbol`), `<symbol>.webp`, copié à
  `cards/<langue>/<extension>/_symbol.webp` ; mêmes règles d'URL que le logo. Le CDN le sert en
  `application/octet-stream` : ce type est accepté pour les symboles seulement (jamais pour les
  autres images), la signature RIFF…WEBP, les tailles et le MD5 = ETag restent exigés.
- **Mêmes garanties que l'image principale** : même client (≤ 4 requêtes/s au total, https,
  aucune redirection), même validation (statut, `image/webp`, RIFF…WEBP, tailles, MD5 = ETag),
  écriture R2 avec contrôle SHA-256 puis D1 ; un objet valide n'est jamais écrasé (déjà présent :
  réconcilié en `downloaded` ; objet d'une autre cible à la même clé : jamais remplacé) ; 404 →
  `not_found_on_cdn` sans nouvelle tentative ; 429, 5xx, réseau : comme l'image principale. Les
  plafonds de coût les comptent : requêtes et écritures R2 dans `MAX_DAILY_OPERATIONS`, octets
  dans `MAX_STORED_BYTES`.
- **Création des tâches** (priorité la plus basse) :
  1. par chaque tâche `set` (import et synchronisation hebdomadaire) : petite image de chaque carte
     dont l'image principale est valide ou cherchée par cette même tâche, logo et symbole de
     l'extension ; seulement si la copie manque : jamais traitée, ou absente / en erreur depuis plus
     de `MISSING_RECHECK_DAYS` (30) jours, ou (logo, symbole) URL changée. Une petite image
     n'attend pas son image principale (des rendus répétés pendant une longue file d'images principales useraient
     `MAX_RELEASES`) : sans image principale valide, sa tâche se termine aussitôt, sans requête ni
     écriture ; c'est la copie réussie de l'image principale (téléchargée ou `already_present`) qui
     remet en attente sa tâche de petite image terminée, sans jamais en créer. Seule exception :
     réservée dans le même lot que son image principale en cours de copie, la tâche est rendue
     1 min (sinon cette remise en attente, écrite avant sa clôture, serait perdue) ;
  2. par `POST /admin/sync/backfill-media` : toutes les petites images, logos et symboles manquants
     (même règle) des cartes et extensions existantes, par pages de 500 (`json_each`), sans doublon
     de tâche active. À appeler une fois après le déploiement, pour le catalogue déjà importé.
- Statuts : `downloaded` (présent), `not_found_on_cdn`, `missing_api_url` (aucune URL source
  fiable), `download_error`, `invalid_image` ; NULL = pas encore traité.

## Gestion des erreurs

| Cas | Traitement |
| --- | --- |
| HTTP 404 / 410 | absence confirmée pour l'URL testée, pas de nouvelle tentative |
| HTTP 429 | tâche rendue sans consommer de tentative ; attente globale selon `Retry-After` (respecté jusqu'à 24 h ; sans en-tête : 2 min, doublée à chaque 429 consécutif, plafond 1 h). Une tâche rendue plus de 48 fois consomme ensuite ses tentatives : jamais de boucle infinie |
| HTTP 5xx, erreur réseau, délai dépassé | 2 relances courtes (2 s, 8 s) dans la même exécution, puis nouvelle tentative différée (1 min → 6 h, ±20 %), au plus 5 tentatives, puis `download_error` |
| 10 erreurs serveur/réseau consécutives | pause automatique de 15 min |
| Transfert corrompu (MD5 ≠ ETag, fichier tronqué) | nouvelle tentative |
| Fichier non WebP, trop petit ou trop gros | `invalid_image` |
| Erreur D1 ou R2 | nouvelle tentative, rien n'est perdu (écritures D1 transactionnelles) |
| Requêtes vers d'autres hôtes que `api.tcgdex.net` / `assets.tcgdex.net`, ou en http | refusées ; aucune redirection n'est suivie (une redirection est traitée comme une erreur) |
| URL d'image officielle d'une autre langue | ignorée (jamais de mélange FR/JP) |

## Reprise automatique

- Chaque tâche réservée porte un verrou avec jeton et expiration (10 min). Une exécution interrompue
  (redéploiement, plantage) laisse des verrous qui expirent : l'exécution suivante remet ces tâches en
  attente, ou les fait échouer si elles ont épuisé leurs tentatives (jamais de boucle infinie).
- Un garde-fou (`job_lock_guard`) annule tout le batch d'écritures d'une tâche dont le verrou a été
  perdu : deux exécutions ne peuvent pas écrire pour la même tâche.
- Le verrou de l'exécuteur (6 min au moins, toujours plus long que l'exécution, renouvelé à chaque
  lot) empêche deux Cron de travailler en même temps (doublons ou retards de Cron) ; une exécution qui
  perdrait son verrou s'arrête.
- Une synchronisation et sa première tâche sont créées dans la même transaction, et jamais deux
  synchronisations à la fois pour une langue (index unique). Une synchronisation bloquée plus de
  6 jours est abandonnée au démarrage suivant (`POST /admin/sync/cancel` pour le faire à la main).
- Si l'image est déjà dans R2 mais que D1 n'a pas été mis à jour, la tâche suivante la réconcilie en
  `already_present` sans la retélécharger ; l'audit des rapports compare aussi R2 et D1.

## Synchronisation du catalogue

Le lundi à 03:07 UTC, une synchronisation `weekly` par langue :

- relit séries, extensions et le détail de chaque extension (≈ 410 requêtes) ;
- détecte nouvelles séries, extensions et cartes, noms ou images modifiés, cartes retirées (marquées
  `removed_at`, jamais supprimées ; une liste vide ou une disparition de plus de 20 % d'une extension
  est traitée comme une anomalie) ; les cartes de l'index `/cards` absentes de toute extension sont
  créées et importées quand même ;
- relance la recherche des images manquantes de plus de 30 jours (le CDN reçoit parfois des images
  après coup) ; de même pour les petites images, les logos et les symboles (un logo ou un symbole
  dont l'URL change est revérifié tout de suite) ;
- rafraîchit le détail des cartes de plus de 90 jours (TCGdex demande de ménager son API) ;
- ne retélécharge jamais une image valide ;
- produit un nouvel audit et de nouveaux rapports à la fin.

## Administration

Toutes les routes `/admin/*` exigent `Authorization: Bearer <ADMIN_TOKEN>` (comparaison en temps
constant ; sans secret configuré, l'administration est refusée). Adresse :
`https://pokecodex-importer.<sous-domaine>.workers.dev` (copie locale dans `.secrets/importer-url.txt`).

Le plus simple : le petit client `scripts/admin.mjs`, qui lit le jeton et l'adresse dans `.secrets/`
sans les afficher (sous Git Bash, écrire le chemin sans barre initiale : `admin/status`).

```bash
node scripts/admin.mjs GET admin/status
```

```bash
node scripts/admin.mjs POST "admin/tick?max_jobs=50"
```

Ou avec curl : `curl -s -H "Authorization: Bearer $(cat .secrets/admin-token.txt)" "$(cat .secrets/importer-url.txt)/admin/status"`.

| Route | Effet |
| --- | --- |
| `GET /admin/status` | rapport texte (`?format=json` pour du JSON), petites images, logos et symboles compris |
| `GET /admin/stats` | statistiques détaillées (`?sets=1` : par extension) |
| `GET /admin/missing` | cartes sans image (`?language=fr` facultatif, `limit`, `after`) |
| `GET /admin/errors` | images en erreur et tâches en échec |
| `GET /admin/reports`, `GET /admin/reports/latest/summary.csv` | rapports enregistrés (`?prefix=latest/`, `?cursor=`) |
| `GET /admin/r2?prefix=cards/fr/swsh3/` | objets R2 de l'importateur et leurs métadonnées (jamais le contenu) |
| `POST /admin/sync/start` | démarre une synchronisation (`?language=fr,ja`, `?sets=swsh3,SV1a`) |
| `POST /admin/sync/pause`, `POST /admin/sync/resume` | pause, reprise (la reprise lève aussi une attente imposée et le signale) |
| `POST /admin/sync/cancel` | abandonne les synchronisations en cours (`?language=`) ; les tâches continuent |
| `POST /admin/sync/retry-missing` | relance images manquantes/en erreur et tâches en échec (petites images, logos et symboles compris pour les tâches en échec) |
| `POST /admin/sync/backfill-media` | crée les tâches des petites images, logos et symboles manquants (`?language=fr,ja`) ; rend `{ lowImages, logos, symbols, complete }` (`complete: false` : rappeler la route) |
| `POST /admin/reports/generate` | nouvel audit et nouveaux exports |
| `POST /admin/tick` | une exécution courte tout de suite (20 s au plus, `?max_jobs=`) |
| `POST /admin/selftest` | vérifie D1, R2 et l'accès à TCGdex |
| `POST /admin/import/catalog` | ajoute séries, extensions et cartes d'une autre source (corps JSON, voir « Ajouts hors TCGdex ») ; ne modifie jamais une ligne existante |
| `POST /admin/import/image` | envoie l'image d'une carte ou le logo d'une extension (corps WebP ; `?kind=main\|low\|logo&language=&target=&source=`) ; n'écrase jamais rien |
| `POST /admin/import/symbol` | copie le symbole d'une extension qui n'en a pas : le Worker télécharge lui-même `source` (TCGdex `univ/…/symbol.png\|webp` ou Pokécardex `symboles[_jp]/<code>.png`), le convertit en WebP (liaison Images) ; `?language=&target=&source=` ; 424 si la source n'a pas le fichier, 502 si elle est en panne |
| `POST /admin/import/logo` | copie le logo d'une extension qui n'en a pas, de la même façon : `source` = logo TCGdex d'une autre langue (`<langue>/<série>/<ext>/logo.webp\|png`) ou PNG de Poképédia, 52poke ou Bulbagarden (fichiers MediaWiki) ; mêmes paramètres et codes de retour |

## Ajouts hors TCGdex (copie Pokécardex)

Les cartes, extensions et logos absents de TCGdex peuvent être ajoutés depuis la copie Pokécardex
du 28/10/2026 (faite sous l'ancien accord ; l'utilisateur a levé le 23/11/2026 sa décision du 13/11
d'écarter Pokécardex). Rien n'est téléchargé : tout part de la copie locale.

- **Migration `0004_manual_sources.sql`** (ajouts de colonnes seulement) : `series.data_source`,
  `sets.data_source`, `sets.logo_provider`, `cards.data_source`, `cards.image_provider` ; NULL =
  TCGdex, `'pokecardex'` = ajout. Le site ne lit pas ces colonnes.
- **`POST /admin/import/catalog`** : `{ language, series?: [{id, name}], sets?: [{id, name, series,
  releaseDate, officialCards, totalCards, metadata}], cards?: [{id, set, localId, name, rarity,
  category, illustrator, metadata}] }`. Contrôles du contrat du site : id d'extension
  `[A-Za-z0-9._-]{1,32}` unique par langue au sens de `setKey` (majuscules, sans `-`), numéro
  `^[a-z]*\d+[a-z]*$` unique dans l'extension au sens de `numberKey` (`017` ≡ `17`), catégorie dans le
  vocabulaire TCGdex de la langue, `metadata` JSON (`dexId` et `variants_detailed` en tableaux,
  `variants` en objet), série existante de la même langue (jamais `tcgp`), date à venir ignorée.
  Une ligne existante (TCGdex ou import précédent) n'est jamais modifiée ; chaque refus est rendu avec
  son motif. Les cartes ajoutées gardent `brief_hash` NULL : la synchronisation TCGdex ne les retire
  jamais, et si TCGdex publie un jour la même carte sous le même id, son upsert la reprend
  (`data_source` NULL, image conservée).
- **`POST /admin/import/image`** : `main` (image principale d'une carte sans image valide, clé
  `cards/<l>/<ext>/<carte>.webp`), `low` (petite image d'une carte dont l'image principale vient de la
  même source, `….low.webp`), `logo` (extension sans logo, `cards/<l>/<ext>/_logo.webp`). WebP validé
  comme pour TCGdex ; métadonnées R2 `source: pokecardex`, `sourceUrl`, `sha256` et l'identité
  attendue par les tâches (`tcgdexId`, `setId`) ; écriture conditionnelle (`onlyIf`) : un objet déjà
  présent n'est jamais remplacé (même contenu : réconcilié).
- **Symboles d'extension** (`src/services/set-media-import.ts`, migration `0005_set_symbol_provider.sql` :
  `sets.symbol_provider`, NULL = TCGdex, `'pokecardex'`) : seule exception au « rien n'est
  téléchargé », autorisée par l'utilisateur le 24/11/2026 (la copie du 28/10 ne contient aucun
  symbole). `POST /admin/import/symbol` fait télécharger le fichier par le Worker lui-même, sur deux
  hôtes seulement, au chemin exact attendu : `assets.tcgdex.net/univ/<série>/<ext>/symbol.png|webp`
  (PNG quand le WebP de l'API manque, symbole de l'extension mère ou de l'extension internationale
  équivalente) et `pokecardex.b-cdn.net/assets/images/symboles[_jp]/<code>.png`. Contrôles : type,
  signature PNG ou WebP, longueur annoncée, MD5 = ETag chez TCGdex, côté de 8 px au moins ; un PNG (ou
  une image de plus de 512 px) est converti en WebP par la liaison Cloudflare Images (`IMAGES`, rien
  n'est stocké chez Images), puis écrit à `cards/<l>/<ext>/_symbol.webp` (métadonnées `setId`,
  `sourceUrl`, `sourceSha256`). Une extension qui a déjà un symbole n'est jamais touchée (aucune
  requête) ; un échec de la source ne modifie pas D1 ; la tâche `symbol` de la synchronisation ne
  remplace jamais un symbole copié.
- **Logos d'une autre langue** (même module, 25/11/2026) : `POST /admin/import/logo` copie de la même
  façon le logo d'une extension qui n'en a pas, pour les extensions dont l'API ne donne aucun logo
  dans leur langue (TCG Pocket en français : logo anglais). Seul hôte : `assets.tcgdex.net`, chemin
  `/<langue>/<série>/<ext>/logo.webp|png` (MD5 = ETag) ; `logo_provider` NULL ; un PNG ou un logo de
  plus de 2 048 px est converti en WebP sans perte ; écrit à `cards/<l>/<ext>/_logo.webp`.
- **Scripts** : `scripts/pokecardex/prepare.mjs` construit les lots depuis la copie locale et les
  fichiers d'analyse (dans `.local/pokecardex/`, ignoré par Git) ; `scripts/import-manual.mjs`
  (`catalog`, `logos`, `images`) les envoie, avec journal de reprise ; `symbols <plan.json>` et
  `logo-urls <plan.json>` demandent au Worker chaque symbole ou logo, une adresse à la fois (3 s
  d'écart chez Pokécardex, 0,3 s chez TCGdex).
- **Filigranes** : les scans de Pokécardex portent souvent un filigrane intégré (« POKÉCARDEX » en
  bande à mi-hauteur, en diagonale, ou « pokecardex.com ») ; les rendus numériques officiels sont
  propres, mais dans beaucoup d'extensions japonaises les cartes secrètes (UR dorées) sont des scans
  filigranés. Aucune image n'est envoyée sans vérification carte par carte (bande centrale à contraste
  renforcé) ; en cas de doute, l'utilisateur tranche. Les détecteurs automatiques essayés (corrélation
  avec le motif moyen) ne séparent pas assez bien les cartes propres des filigranées.
- **Import du 24/11/2026** : 2 séries (JA `DP`, `DPt`), 277 extensions, 16 442 cartes, 504 logos et
  13 997 images (FR 524, JA 13 473) ; 618 images filigranées et 7 cartes BWP portant un texte ajouté
  (« Carte jamais… ») écartées. Les cartes ajoutées sans image propre sont en `missing_api_url`
  (motif « aucune image propre disponible »).
- **Symboles du 24/11/2026** : 534 copiés (48 depuis TCGdex, 486 depuis Pokécardex), soit 650
  extensions sur 684 avec un symbole. Les 34 restantes n'ont aucune source retenue (pas de symbole
  imprimé, extensions vides, TCG Pocket). Plan, journal et liste des 106 symboles à revoir à l'œil
  (tailles anormales, icônes communes, équivalences déduites) : `.local/pokecardex/symboles/`.

## Estimation des coûts

Abonnement Workers Paid (5 $/mois) déjà en place ; aucune autre souscription. Estimation pour
≈ 35 500 cartes et ≈ 24 000 à 27 000 images de ≈ 85 Ko :

| Poste | Import initial | Quota inclus | Surcoût estimé |
| --- | --- | --- | --- |
| Stockage R2 | ≈ 2 à 2,5 Go | 10 Go-mois | 0 |
| R2 classe A (écritures, listes) | ≈ 30 000 | 1 M/mois | 0 |
| R2 classe B (`head`) | ≈ 36 000 | 10 M/mois | 0 |
| Lignes D1 écrites | ≈ 1 M | 50 M/mois | 0 |
| Stockage D1 | ≈ 150 Mo | 5 Go | 0 |
| Requêtes Worker (Cron + admin) | ≈ 9 000/mois | 10 M/mois | 0 |
| CPU Worker | < 1 M ms | 30 M ms/mois | 0 |

Requêtes vers TCGdex : ≈ 71 000 pour l'import initial (mesuré le 13/11 : ≈ 800 tâches par exécution
de 4 min avec 2 voies, soit ≈ 7 à 8 h au total ; 4 voies depuis, plafond global inchangé),
puis ≈ 3 000 à 6 000 par semaine. Petites images, logos et symboles : en plus, une requête et une
écriture R2 par carte à image valide (≈ 24 000 à 27 000, soit ≈ 2 à 3 h de Cron), par extension à
logo (quelques centaines) et par extension à symbole (157 ; ≈ 350 octets chacun, mesuré sur
sv03.5 : moins de 100 Ko au total), pour un volume bien inférieur à celui des images `high` (non
mesuré : suivre « Volume stocké » dans `GET /admin/status`). Garde-fous configurables : pause automatique au-delà de 20 Go
stockés, arrêt au-delà de 150 000 opérations par jour UTC. Ces limites réduisent le risque mais **ne
garantissent pas** à elles seules un plafond de facture : surveillez l'alerte budgétaire du compte.

## Future intégration à PokéCodex.fr

- Le site pourra lire `pokecodex-cards` (liaison D1 en lecture) ou un export, et servir les images de
  `cards/…` par son propre Worker (le bucket reste privé ; aucun domaine public sans validation des
  droits). Son API de données lit `image_low_status` / `image_low_r2_key` (grilles),
  `logo_status` / `logo_r2_key` et `symbol_status` / `symbol_r2_key` (extensions) : `downloaded` =
  présent ; sinon (pas encore copiée, introuvable sur le CDN), le site doit prévoir un repli (image
  principale `image_r2_key`, pas de logo, pas de symbole).
- Les prix et liens marketplaces iront dans `card_marketplace_links` ; les identifiants produits
  Cardmarket de TCGdex sont déjà dans `metadata_json` (`variants_detailed[].thirdParty.cardmarket`).
  Aucune API payante, aucun scraping de Cardmarket, aucune donnée de marché inventée.

## Droits et mentions

- Données TCGdex : licence MIT (© TCGdex). TCGdex n'est ni produit, ni approuvé par Nintendo ou The
  Pokémon Company.
- Les images Pokémon sont des œuvres protégées ; TCGdex ne donne aucune licence de redistribution.
  Elles sont stockées pour préparer le catalogue, dans un bucket privé. Avant toute diffusion
  publique, valider les droits (et, par courtoisie, prévenir TCGdex).
