PRAGMA foreign_keys=OFF;

BEGIN TRANSACTION;

DROP TABLE IF EXISTS chefs;
CREATE TABLE chefs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name STRING NOT NULL UNIQUE,
  native_name STRING,
  cuisine STRING,
  nationality STRING
);

DROP TABLE IF EXISTS episodes;
CREATE TABLE episodes (
  id INTEGER PRIMARY KEY,
  air_date DATE NOT NULL
);

DROP TABLE IF EXISTS battles;
CREATE TABLE battles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  episode_id INTEGER NOT NULL,
  battle_number INTEGER NOT NULL DEFAULT 1,
  theme_ingredient STRING NOT NULL,
  FOREIGN KEY(episode_id) REFERENCES episodes(id),
  UNIQUE(episode_id, battle_number)
);

DROP TABLE IF EXISTS iron_chefs_battles;
CREATE TABLE iron_chefs_battles (
  iron_chef_id INTEGER NOT NULL,
  battle_id INTEGER NOT NULL,
  FOREIGN KEY(iron_chef_id) REFERENCES chefs(id),
  FOREIGN KEY(battle_id) REFERENCES battles(id)
);

DROP TABLE IF EXISTS challengers_battles;
CREATE TABLE challengers_battles (
  challenger_id INTEGER NOT NULL,
  battle_id INTEGER NOT NULL,
  FOREIGN KEY(challenger_id) REFERENCES chefs(id),
  FOREIGN KEY(battle_id) REFERENCES battles(id)
);

-- winners is also many-to-many
DROP TABLE IF EXISTS winners_battles;
CREATE TABLE winners_battles (
  winner_id INTEGER NOT NULL,
  battle_id INTEGER NOT NULL,
  FOREIGN KEY(winner_id) REFERENCES chefs(id),
  FOREIGN KEY(battle_id) REFERENCES battles(id)
);

DROP TABLE IF EXISTS movie_files;
CREATE TABLE movie_files (
  path STRING PRIMARY KEY,
  episode_id INTEGER NOT NULL,
  dubbed BOOLEAN NOT NULL,
  FOREIGN KEY(episode_id) REFERENCES episodes(id)
);


COMMIT;
