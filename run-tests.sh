#! /usr/bin/env bash

sqlite3 unit-tests.sqlite < index.sql
sqlite3 index-tests.sqlite < index.sql
lein test
