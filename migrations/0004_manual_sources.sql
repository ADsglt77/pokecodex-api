-- PokéCodex — importateur : lignes et images venues d'une autre source que TCGdex.
--
-- Uniquement des ajouts de colonnes : aucune table n'est reconstruite, aucun index.
-- NULL = TCGdex (toutes les lignes existantes) ; 'pokecardex' = ajouté par l'API d'administration
-- (`POST /admin/import/*`) depuis la copie Pokécardex. Ces valeurs ne sont jamais lues par le site :
-- elles servent au suivi (statistiques) et à ne jamais confondre les deux origines.
--
-- Une carte ajoutée ainsi garde `brief_hash` NULL : la synchronisation TCGdex ne la marque jamais
-- retirée ; si TCGdex la liste un jour sous le même id, son upsert la reprend (`data_source` NULL).

-- Origine de la ligne de la série, de l'extension, du logo, de la carte et de son image (principale et petite).
ALTER TABLE series ADD COLUMN data_source TEXT CHECK (data_source IN ('pokecardex'));
ALTER TABLE sets ADD COLUMN data_source TEXT CHECK (data_source IN ('pokecardex'));
ALTER TABLE sets ADD COLUMN logo_provider TEXT CHECK (logo_provider IN ('pokecardex'));
ALTER TABLE cards ADD COLUMN data_source TEXT CHECK (data_source IN ('pokecardex'));
ALTER TABLE cards ADD COLUMN image_provider TEXT CHECK (image_provider IN ('pokecardex'));
