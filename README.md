# iron-chef-index

A Clojure program to convert the HTML data from [the Wikipedia list of Iron Chef episodes](https://en.wikipedia.org/wiki/List_of_Iron_Chef_episodes) into a sqlite database. The end goal is to combine that with the [Internet Archive files for Iron Chef](https://archive.org/details/iron-chef) to provide a comprehensive index of episodes by challenger, season, Iron Chef and ingredient.

I got tired writing all the edge cases at some point and the project quietly died off... then Claude and similar tools arrived, one of whose best use cases was "writing code in situations that human developers find mind-numbingly tedious". So the project was revived and finished over a couple of nights' worth of tokens.

## Usage

To reset the database to its empty state:
> sqlite3 index.sqlite < index.sql

To run the unit tests (including recreating an empty testing database):
> ./run-tests.sh

To build the database (from a REPL):
> (iron-chef-index.core/execute!)

## Also

* [Wikipedia list of Iron Chef episodes](https://en.wikipedia.org/wiki/List_of_Iron_Chef_episodes)
* [Iron Chef on the Internet Archive](https://archive.org/details/iron-chef)
* [Iron Chef Exchange](https://nylon.net/ironchef/)
* [Iron Chef DB](https://ironchefdb.com/)

## License

Copyright © 2023 Joel Gluth

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
