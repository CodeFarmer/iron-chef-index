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
  air_date DATE NOT NULL,
  theme_ingredient STRING NOT NULL
);

DROP TABLE IF EXISTS iron_chefs_episodes;
CREATE TABLE iron_chefs_episodes (
  iron_chef_id INTEGER NOT NULL,
  episode_id INTEGER NOT NULL,
  FOREIGN KEY(iron_chef_id) REFERENCES chefs(id),
  FOREIGN KEY(episode_id) REFERENCES episodes(id)
);

DROP TABLE IF EXISTS challengers_episodes;
CREATE TABLE challengers_episodes (
  challenger_id INTEGER NOT NULL,
  episode_id INTEGER NOT NULL,
  FOREIGN KEY(challenger_id) REFERENCES chefs(id),
  FOREIGN KEY(episode_id) REFERENCES episodes(id)
);

-- winners is also many-to-many
DROP TABLE IF EXISTS winners_episodes;
CREATE TABLE winners_episodes (
  winner_id INTEGER NOT NULL,
  episode_id INTEGER NOT NULL,
  FOREIGN KEY(winner_id) REFERENCES chefs(id),
  FOREIGN KEY(episode_id) REFERENCES episodes(id)
);

DROP TABLE IF EXISTS movie_files;
CREATE TABLE movie_files (
  path STRING PRIMARY KEY,
  episode_id INTEGER NOT NULL,
  dubbed BOOLEAN NOT NULL,
  FOREIGN KEY(episode_id) REFERENCES episodes(id)
);


COMMIT;
