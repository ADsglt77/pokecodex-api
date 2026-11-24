-- PokéCodex — importateur : symboles d'extension copiés par l'API d'administration
-- (`POST /admin/import/symbol`), hors de la tâche `symbol` de la synchronisation TCGdex.
--
-- Ajout de colonne seulement (aucune table reconstruite). Même règle que `logo_provider`
-- (`0004_manual_sources.sql`) : NULL = TCGdex (tâche de synchronisation, ou adresse de TCGdex donnée
-- à la main : PNG à la place du WebP absent, symbole de l'extension mère) ; 'pokecardex' = fichier
-- de Pokécardex. Colonne non lue par le site.
ALTER TABLE sets ADD COLUMN symbol_provider TEXT CHECK (symbol_provider IN ('pokecardex'));
