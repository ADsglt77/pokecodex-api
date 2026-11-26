# pokecodex-importer — notes pour Claude Code

Importateur autonome TCGdex FR/JP → D1 `pokecodex-cards` + R2 `pokedex-data` (`cards/`, `reports/`),
sur un Worker Cloudflare indépendant (`pokecodex-importer`). Tout le fonctionnement est décrit dans
`README.md` ; ici, ce qu'il faut savoir pour reprendre le projet sans casser quoi que ce soit.

## Règles absolues

- Répondre en français (vouvoiement).
- Ne jamais toucher aux ressources de production du site : Workers `pokedex` et `pokedex-data`,
  bases D1 `pokedex-data` et `pokecodex`, domaine `pokecodex.fr`, DNS, routes.
- Le bucket R2 `pokedex-data` est **partagé** avec la production (préfixes `images/`, `snapshots/`) :
  l'importateur n'écrit et ne lit que sous `cards/` et `reports/` (constantes `IMAGES_PREFIX`,
  `REPORTS_PREFIX` de `src/config.ts`). Aucune suppression massive, jamais. Le bucket reste privé.
- Aucun secret dans Git : `ADMIN_TOKEN` est un secret Wrangler ; sa copie locale est
  `.secrets/admin-token.txt` (ignoré par Git). Ne jamais l'afficher dans une réponse.
- Aucune donnée de marché : les objets `pricing` de TCGdex sont retirés avant stockage.
- Ajouts hors TCGdex (copie Pokécardex du 28/10, autorisée par l'utilisateur le 23/11/2026) :
  uniquement par `POST /admin/import/*`, qui n'écrase jamais rien. **Aucune image filigranée** :
  chaque image est vérifiée carte par carte avant envoi ; en cas de doute, demander à l'utilisateur
  (il a signalé des faux positifs). Aucun nom de source dans les ids ni les clés. Seules exceptions
  au « rien n'est téléchargé » : les symboles d'extension (`POST /admin/import/symbol`, autorisé le
  24/11/2026), que le Worker télécharge lui-même chez TCGdex ou Pokécardex, les logos
  (`POST /admin/import/logo`, 25/11/2026) et les images de cartes (`POST /admin/import/card`,
  25/11/2026 : Poképédia et assets.pokemon.com, après vérification des planches de miniatures) ;
  code : `src/services/set-media-import.ts`. Images reconstituées (`POST /admin/import/card-overlay`,
  26/11/2026) : image d'une carte d'origine déjà dans R2 (ou, si `origin` est une adresse de
  `CARD_SOURCES`, image Poképédia téléchargée par le Worker, ex. Énergie de base) + tampon de deck,
  composées dans le Worker (`composeOverlay` de `wasm-images.ts`) ; `image_source_url` =
  `reconstitution:<l>/<origine>` ou `reconstitution:<adresse>`.
  Remplacement d'une image existante (`POST /admin/import/card-replace` puis `card-replace-apply`,
  26/11/2026) : nouvelle clé `<carte>.p<sha>.webp`, bascule conditionnée par `expect`, ancien objet gardé.

## Environnement

- Windows ARM64, Node 24 ARM64 ; Wrangler, Vitest, TypeScript sous Node (jamais `bun --bun`).
- `workerd` tourne en émulation x64 ; sous npm 11, ses scripts d'installation sont autorisés dans
  `package.json` (`allowScripts`).
- Compte Cloudflare « [Ami] AD » (`c1a392d3b0fccd556dd597bf47afe3b4`), Workers Paid.
- Types des liaisons : `npx wrangler types` (régénérer `worker-configuration.d.ts` après tout
  changement de `wrangler.jsonc`).

## Commandes

```bash
npm run check          # tsc strict + tests Vitest (workerd, aucun réseau)
npm run test:live      # test réel contre TCGdex (~15 requêtes, aucune écriture)
npm run ci             # ce que lance Workers Builds avant de déployer (= npm run check)
npm run deploy         # migrations D1 distantes + wrangler deploy (Workers Builds, à chaque push sur main)
npx wrangler tail pokecodex-importer --format json   # journaux en direct
```

Administration (jeton : `.secrets/admin-token.txt`, adresse : `.secrets/importer-url.txt`) :
`node scripts/admin.mjs GET admin/status` (sous Git Bash, chemin sans barre initiale : MSYS
convertirait `/admin/…` en chemin Windows).

## Points de conception à respecter

- **Limites Cloudflare** : 1 000 requêtes D1 par invocation (un batch compte pour 1), 10 000
  sous-requêtes, 6 connexions, 30 s de CPU pour un Cron de moins d'une heure, 15 min de durée,
  100 paramètres liés par requête (d'où `json_each(?)`), 100 Ko par requête SQL. L'exécuteur compte
  tout (`Usage`) et s'arrête avant (`D1_QUERY_BUDGET`, `SUBREQUEST_BUDGET`, `RUN_TIME_BUDGET_MS`).
- **Une tâche ne modifie jamais D1 elle-même** : elle rend ses écritures, que l'exécuteur applique
  dans un batch précédé du garde-fou `job_lock_guard` (verrou perdu → batch annulé).
- **Cron** : `PROCESS_CRON`/`SYNC_CRON` (vars) doivent égaler `triggers.crons` (`*/5 * * * *`,
  `7 3 * * MON`). Jours Cloudflare : 1 = dimanche ; utiliser `MON`. Pas de déclenchement manuel en
  production : `POST /admin/tick` (20 s au plus, protégé par `ctx.waitUntil`).
- **Verrous** : `parseConfig` garantit verrou de l'exécuteur ≥ budget + dépassement possible, et
  verrou de tâche ≥ verrou de l'exécuteur ; le verrou de l'exécuteur est renouvelé dans le batch de
  chaque lot. Une tâche rendue (429, dépendance) l'est `MAX_RELEASES` fois au plus.
- **Réseau** : aucune redirection suivie (`redirect: 'manual'`), seuls `api.tcgdex.net` et
  `assets.tcgdex.net` en https ; une URL d'image officielle d'une autre langue est ignorée par la
  synchronisation. Exceptions, une adresse par appel et au chemin exact (`SET_MEDIA_SOURCES`) :
  `POST /admin/import/symbol` contacte aussi `pokecardex.b-cdn.net`
  (`/assets/images/symboles[_jp]/<code>.png`), et `POST /admin/import/logo` accepte un logo TCGdex
  d'une autre langue (`/<langue>/<série>/<ext>/logo.webp|png`) ou un PNG de Poképédia, 52poke ou
  Bulbagarden (autorisés le 25/11/2026) ; `POST /admin/import/card` accepte `www.pokepedia.fr`
  (`/images/<h>/<hh>/<fichier>.png|jpg`) et `assets.pokemon.com` (`CARD_SOURCES`). Ajouter un hôte =
  accord de l'utilisateur d'abord.
- **Conversion d'images** : dans le Worker lui-même, par les codecs WebAssembly jSquash
  (`src/services/wasm-images.ts` : PNG, JPEG, WebP, redimensionnement lanczos3, `ImageData` simulé),
  depuis que le quota gratuit de la liaison Cloudflare Images (5 000 transformations par mois) a été
  épuisé le 25/11/2026. La liaison `IMAGES` et `imagesBindingAdapter` restent déclarées mais ne sont
  plus utilisées. Un PNG peut faire planter le codec (`RuntimeError: unreachable`, `fr:tk-ex-latia-7`) :
  un isolat n'a que 128 Mo et la mémoire d'un codec ne rétrécit jamais ; un codec planté le reste
  pour tout l'isolat. D'où `MAX_PIXELS` (6 Mpx, lu dans l'en-tête) et la réduction préalable en
  JavaScript à 1 024 px de côté (`areaShrink`) de toute image décodée plus grande.
  Après tout changement de `wrangler.jsonc`, `npx wrangler types`.
- **TCGdex** (sondé le 13/11/2026) : pas de limite publiée mais « be considerate » (≤ 4 requêtes/s) ;
  encoder chaque segment d'URL (`exu-%3F` → `exu-%253F`, `SM1+` → `SM1%2B`) ; l'API renvoie parfois
  des 503 de quelques secondes ; le CDN `assets.tcgdex.net` (nginx + S3, pas Cloudflare) renvoie de
  vrais 404 (marqués `immutable`, d'où `cache: 'no-store'`) et un ETag = MD5 du contenu ; la seule
  reconstruction valable est `/{langue}/{série}/{extension}/{localId}/high.webp` (casse exacte) ;
  `rarity: "None"` en japonais = inconnue ; les cartes `pl4-AR*` sont listées dans `pl3` (l'extension
  qui liste la carte prime sur celle de son détail) ; les symboles d'extension
  (`<symbol_url>.webp`, souvent sous `univ`) sont servis en `application/octet-stream` (sondé le
  14/11/2026 sur `univ/sv/sv03.5` : 352 octets, RIFF…WEBP, ETag = MD5).
- **Idempotence** : `dedupe_key` unique par tâche ; upserts conditionnés par `*_hash` ; une image
  valide n'est jamais retéléchargée ni écrasée.
- **Petites images, logos et symboles** (`src/services/media.ts`, migrations `0002_media.sql` et
  `0003_set_symbols.sql`) : tâches `image` de priorité 6 (`MEDIA_PRIORITY`, après tout le reste),
  charge utile `{"media":"low"|"logo"|"symbol"}`, `dedupe_key` `image_low:<langue>:<carte>` /
  `logo:<langue>:<extension>` / `symbol:<langue>:<extension>` ; ne jamais ajouter de type à
  `import_jobs.job_type` (CHECK : reconstruction de table interdite). Clés R2 :
  `cards/<langue>/<ext>/<carte>.low.webp` (clé principale, `.webp` → `.low.webp` ; même clé si la
  principale est déjà `low`), `cards/<langue>/<ext>/_logo.webp` et `cards/<langue>/<ext>/_symbol.webp`.
  Logo et symbole partagent le même traitement (`runSetMediaJob`, colonnes `logo_*` / `symbol_*`) ;
  seul le symbole accepte `application/octet-stream` (`checkWebp(…, { allowOctetStream: true })`),
  jamais les autres images. Le site lit `image_low_status`, `image_low_r2_key`, `logo_status`,
  `logo_r2_key`, `symbol_status`, `symbol_r2_key` : `downloaded` est le seul statut « présent »
  (CHECK). Créées par la tâche `set` et par `POST /admin/sync/backfill-media` (réponse
  `{ lowImages, logos, symbols, complete }`) ; revérifiées après 30 jours comme les images
  principales (logo et symbole : tout de suite si l'URL change) ; comptées dans
  `MAX_DAILY_OPERATIONS` et `MAX_STORED_BYTES` (index couvrant `cards_low_stored`). Une petite
  image n'attend jamais son image principale (pas de
  rendu tant qu'elle est en file : `MAX_RELEASES` serait usé) : sans image principale valide, tâche
  `done` sans écriture ; la réussite de `runImageJob` la remet en attente (`reviveLowImageStmt`,
  jamais de création). Seul rendu : même lot que l'image principale en cours de copie (même jeton).

## Fichiers clés

`src/services/runner.ts` (exécuteur), `src/services/downloader.ts` (images), `src/services/media.ts`
(petites images, logos et symboles), `src/services/catalog.ts` (catalogue, extensions, index),
`src/services/cards.ts` (détails), `src/services/reports.ts` (audit et exports),
`src/repositories/jobs.ts` (file de tâches), `src/services/manual-import.ts` (ajouts hors TCGdex),
`src/services/set-media-import.ts` (logos, symboles et images de cartes copiés par l'API d'import),
`src/services/wasm-images.ts` (conversion WebP en WebAssembly),
`migrations/` (schéma : `0001_initial.sql`, `0002_media.sql`, `0003_set_symbols.sql`,
`0004_manual_sources.sql`, `0005_set_symbol_provider.sql`), `tests/helpers.ts` (faux TCGdex, horloge
simulée), `tests/media.test.ts`,
`tests/manual-import.test.ts`, `scripts/pokecardex/prepare.mjs` et `scripts/import-manual.mjs`.
