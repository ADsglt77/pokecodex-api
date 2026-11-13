# PokéCodex — importateur TCGdex FR/JP

Worker Cloudflare autonome qui construit la base de cartes de PokéCodex à partir de
[TCGdex](https://tcgdex.dev) : toutes les séries, extensions et cartes **françaises** et
**japonaises** répertoriées par TCGdex, avec leurs images `high.webp` copiées directement de TCGdex
vers R2 (TCGdex → Worker → R2, sans passer par un ordinateur).

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
   (catalogue, tâches,              (séries, extensions,             (images high.webp)
    verrous, bilans)                 cartes)                                │
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
  `card` (détail : rareté, catégorie, illustrateur, JSON brut). Les images passent avant les détails
  pour que les premières copies arrivent dès le début.
- **`src/services/downloader.ts`** : la copie d'une image ; **`src/services/catalog.ts`**,
  **`cards.ts`** : catalogue et détails ; **`reports.ts`**, **`stats.ts`** : audit, exports,
  statistiques ; **`sync.ts`** : démarrer, pause, reprise, relance.
- **`src/repositories/`** : accès D1 (requêtes paramétrées ; écritures en masse par un seul
  paramètre JSON `json_each`, sous la limite de 100 paramètres de D1).
- **`src/tcgdex/`** : client HTTP (débit, délais, relances courtes), parseurs validés, URL d'images
  et validation WebP.

## Configuration Cloudflare

| Ressource | Nom | Rôle |
| --- | --- | --- |
| Worker | `pokecodex-importer` | administration + Cron, adresse `*.workers.dev` uniquement |
| D1 | `pokecodex-cards` (WEUR, `7a534301-31ff-4d62-b42c-f5d1e3800e51`) | catalogue, tâches, bilans |
| R2 | `pokedex-data` (existant, **privé**) | `cards/…` (images), `reports/…` (rapports) |
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

```bash
npx wrangler login                       # une fois
npm run check
npm run deploy                           # migrations D1 distantes + déploiement du Worker
npx wrangler secret put ADMIN_TOKEN      # jeton aléatoire d'au moins 32 caractères
```

Le jeton d'administration de ce déploiement est rangé localement dans
`.secrets/admin-token.txt` (ignoré par Git). Ne jamais le committer ni le coller dans un ticket.

## D1 (`pokecodex-cards`)

Migrations dans `migrations/`. Tables :

- `series`, `sets`, `cards` : une ligne par langue (`id` = `<langue>:<id TCGdex>`, ex. `fr:swsh3-136`,
  unicité `(language, tcgdex_id)`) ; une carte FR et une carte JP ne sont jamais confondues.
  `metadata_json` garde le JSON brut de TCGdex **sans les prix** (données de marché volatiles) mais
  avec les identifiants produits (`thirdParty.cardmarket`), utiles pour la suite.
- `cards.image_*` : statut, clé R2, qualité, taille, SHA-256, date, URL testées, code HTTP et motif
  d'échec.
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
│   ├── fr/<extension>/<carte>.webp      ex. cards/fr/swsh3/swsh3-136.webp
│   └── ja/<extension>/<carte>.webp      ex. cards/ja/SV1a/SV1a-001.webp
└── reports/
    ├── latest/{summary.json, summary.csv, missing_fr.csv, missing_ja.csv, errors.csv}
    └── history/<date>/summary.{json,csv}
```

Les segments de clé sont encodés de façon sûre et réversible : seuls `A-Z a-z 0-9 . -` restent tels
quels, tout autre octet devient `_XX` (ex. `exu-%3F` → `exu-_253F`, `SM1+` → `SM1_2B`).

Chaque image est lue en flux dans une limite de taille, gardée en mémoire le temps de la validation
(≈ 90 Ko en moyenne), validée (statut 200, type `image/webp`, signature `RIFF…WEBP`, taille RIFF,
longueur annoncée, **MD5 = ETag** du CDN), puis écrite dans R2 avec le contrôle `sha256` de R2. D1
n'est mis à jour qu'une fois l'écriture confirmée. Aucun téléchargement corrompu ne peut donc écraser
un objet. Les images ne sont ni recompressées ni modifiées. Le bucket reste **privé** : aucun domaine
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
  après coup) ;
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
| `GET /admin/status` | rapport texte (`?format=json` pour du JSON) |
| `GET /admin/stats` | statistiques détaillées (`?sets=1` : par extension) |
| `GET /admin/missing` | cartes sans image (`?language=fr` facultatif, `limit`, `after`) |
| `GET /admin/errors` | images en erreur et tâches en échec |
| `GET /admin/reports`, `GET /admin/reports/latest/summary.csv` | rapports enregistrés (`?prefix=latest/`, `?cursor=`) |
| `GET /admin/r2?prefix=cards/fr/swsh3/` | objets R2 de l'importateur et leurs métadonnées (jamais le contenu) |
| `POST /admin/sync/start` | démarre une synchronisation (`?language=fr,ja`, `?sets=swsh3,SV1a`) |
| `POST /admin/sync/pause`, `POST /admin/sync/resume` | pause, reprise (la reprise lève aussi une attente imposée et le signale) |
| `POST /admin/sync/cancel` | abandonne les synchronisations en cours (`?language=`) ; les tâches continuent |
| `POST /admin/sync/retry-missing` | relance images manquantes/en erreur et tâches en échec |
| `POST /admin/reports/generate` | nouvel audit et nouveaux exports |
| `POST /admin/tick` | une exécution courte tout de suite (20 s au plus, `?max_jobs=`) |
| `POST /admin/selftest` | vérifie D1, R2 et l'accès à TCGdex |

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
puis ≈ 3 000 à 6 000 par semaine. Garde-fous configurables : pause automatique au-delà de 20 Go
stockés, arrêt au-delà de 150 000 opérations par jour UTC. Ces limites réduisent le risque mais **ne
garantissent pas** à elles seules un plafond de facture : surveillez l'alerte budgétaire du compte.

## Future intégration à PokéCodex.fr

- Le site pourra lire `pokecodex-cards` (liaison D1 en lecture) ou un export, et servir les images de
  `cards/…` par son propre Worker (le bucket reste privé ; aucun domaine public sans validation des
  droits).
- Les prix et liens marketplaces iront dans `card_marketplace_links` ; les identifiants produits
  Cardmarket de TCGdex sont déjà dans `metadata_json` (`variants_detailed[].thirdParty.cardmarket`).
  Aucune API payante, aucun scraping de Cardmarket, aucune donnée de marché inventée.

## Droits et mentions

- Données TCGdex : licence MIT (© TCGdex). TCGdex n'est ni produit, ni approuvé par Nintendo ou The
  Pokémon Company.
- Les images Pokémon sont des œuvres protégées ; TCGdex ne donne aucune licence de redistribution.
  Elles sont stockées pour préparer le catalogue, dans un bucket privé. Avant toute diffusion
  publique, valider les droits (et, par courtoisie, prévenir TCGdex).
