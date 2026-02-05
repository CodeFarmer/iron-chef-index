# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Clojure program that parses the Wikipedia "List of Iron Chef episodes" HTML page into a SQLite database. The database indexes all 295 episodes (1-291 numbered + 4 specials numbered 292-295) by challenger, Iron Chef, theme ingredient, and air date. Intended to be combined with Internet Archive video files for a comprehensive episode index.

## Commands

- `./run-tests.sh` - Reset test database and run all tests (preferred way to run tests)
- `lein test` - Run tests without resetting the database
- `lein test :only iron-chef-index.core-test/chef-test` - Run a single test by name
- `lein repl` - Start REPL (drops into `iron-chef-index.core` namespace)
- `sqlite3 index.sqlite < index.sql` - Reset main database to empty schema

## Database Schema

Six tables with a battles-based join model:

- **chefs** - All chefs (Iron Chefs and challengers). Fields: `id`, `name` (UNIQUE), `native_name`, `cuisine`, `nationality`
- **episodes** - Episode records. `id` is the episode number (1-295), plus `air_date`
- **battles** - Individual battles within episodes. Fields: `episode_id`, `battle_number` (default 1), `theme_ingredient`. Episodes can have multiple battles (tournament/special episodes)
- **iron_chefs_battles** / **challengers_battles** / **winners_battles** - Join tables linking chefs to specific battles

## Architecture

Single-namespace app in `src/iron_chef_index/core.clj`. Data flows through four layers:

1. **HTML Parsing** - `parse-html-file` reads the Wikipedia HTML; `get-tables` finds all 16 `wikitable` elements via hickory (HTML parser). Each table corresponds to a year or group of episodes.
2. **Table-to-Map Conversion** - `table-to-maps` converts HTML rows into Clojure maps keyed by column header (e.g., `{"Episode #" [...], "Iron Chef" [...], ...}`). `zipmulti` handles duplicate column names (tables with multiple "Challenger" columns). `spanned-rows-to-map` handles `rowspan` attributes.
3. **Row Processing** - `process-row-map!` is the main workhorse: extracts episode number, date, ingredient, and chef names from a row-map, then writes everything to SQLite via `next.jdbc`.
4. **Special Episode Handlers** - ~20 episodes can't be parsed generically (team battles, tournaments, rowspan issues, ties). Each has a dedicated `write-episode-N!` function with hardcoded data.

### Table Index Mapping

The `execute!` function processes tables by 0-based index from the parsed HTML:

| Tables | Years | Processing |
|--------|-------|------------|
| 0-1 | 1993-1994 | Generic `process-table!` |
| 2 | Episode 60 | `process-stupid-table!` (rowspan edge case) |
| 3 | 1995 | `process-1995-table!` (skips special episodes) |
| 6 | 1996 | `process-1996-table!` (skips special episodes) |
| 8 | 1997 | `process-1997-table!` (skips special episodes) |
| 9 | 1998 | `process-1998-table!` (skips special episodes) |
| 10 | 1999 | Generic `process-table!` |
| 11+ | 2000-2002 | Fully hardcoded (specials 292-295) |

Year-specific `process-YYYY-table!` functions use `take`/`drop` with hardcoded row offsets to skip rows for special episodes, then call dedicated `write-episode-N!` functions.

### Episodes with Special Handling

61, 73, 99, 101-102, 110, 111, 124, 149, 160, 163, 164, 190, 193, 194, 198, 239, 292-295. These have dedicated `write-episode-N!` functions because of team battles, tournaments, irregular HTML, ties, or no-contests.

## Testing

Tests are in `test/iron_chef_index/core_test.clj`. Each test uses `jdbc/with-transaction` with `{:rollback-only true}` so database state is isolated between tests without resetting.

The `execute-test` is a full integration test that runs `execute!` and verifies 295 episodes with correct chefs. Many tests verify specific battle participants and winners for special episodes.

`run-tests.sh` resets `unit-tests.sqlite` via `index.sql` before running, which is necessary because the database file must exist with the correct schema for transactions to work.

## Key Dependencies

- **hickory** - HTML parsing (Clojure equivalent of BeautifulSoup)
- **next.jdbc** - JDBC database access
- **SQLite JDBC** - Database driver
