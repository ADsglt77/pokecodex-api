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
npm run deploy         # migrations D1 distantes + wrangler deploy
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
  `assets.tcgdex.net` en https ; une URL d'image officielle d'une autre langue est ignorée.
- **TCGdex** (sondé le 13/11/2026) : pas de limite publiée mais « be considerate » (≤ 4 requêtes/s) ;
  encoder chaque segment d'URL (`exu-%3F` → `exu-%253F`, `SM1+` → `SM1%2B`) ; l'API renvoie parfois
  des 503 de quelques secondes ; le CDN `assets.tcgdex.net` (nginx + S3, pas Cloudflare) renvoie de
  vrais 404 (marqués `immutable`, d'où `cache: 'no-store'`) et un ETag = MD5 du contenu ; la seule
  reconstruction valable est `/{langue}/{série}/{extension}/{localId}/high.webp` (casse exacte) ;
  `rarity: "None"` en japonais = inconnue ; les cartes `pl4-AR*` sont listées dans `pl3` (l'extension
  qui liste la carte prime sur celle de son détail).
- **Idempotence** : `dedupe_key` unique par tâche ; upserts conditionnés par `*_hash` ; une image
  valide n'est jamais retéléchargée ni écrasée.

## Fichiers clés

`src/services/runner.ts` (exécuteur), `src/services/downloader.ts` (images), `src/services/catalog.ts`
(catalogue, extensions, index), `src/services/cards.ts` (détails), `src/services/reports.ts` (audit et
exports), `src/repositories/jobs.ts` (file de tâches), `migrations/0001_initial.sql` (schéma),
`tests/helpers.ts` (faux TCGdex, horloge simulée).
