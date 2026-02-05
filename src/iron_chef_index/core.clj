(ns iron-chef-index.core
  (:require [clojure.string :as s]
            [clojure.pprint :refer :all]
            [next.jdbc :as jdbc]
            [hickory.core :as h]
            [hickory.select :as hs]))

(defn get-all-chefs
  "Retrieve all chefs from the database"
  [conx]
  (jdbc/execute! conx ["select * from chefs"]))

(defn get-all-episodes
  "Retrieve all episodes from the database"
  [conx]
  (jdbc/execute! conx ["select * from episodes"]))


(defn create-episode!
  "Create an episode with just id and air_date"
  [conx id air-date]
  (jdbc/execute-one! conx
                     ["insert into episodes(id, air_date) values(?, ?)"
                      id
                      air-date]))

(defn create-battle!
  "Create a battle within an episode. Returns the battle id."
  ([conx episode-id theme-ingredient]
   (create-battle! conx episode-id 1 theme-ingredient))
  ([conx episode-id battle-number theme-ingredient]
   (jdbc/execute-one! conx
                      ["insert into battles(episode_id, battle_number, theme_ingredient) values(?, ?, ?)"
                       episode-id
                       battle-number
                       theme-ingredient])
   ;; Return the battle id
   (:battles/id (jdbc/execute-one! conx
                                   ["select id from battles where episode_id = ? and battle_number = ?"
                                    episode-id
                                    battle-number]))))

(defn create-chef!
  "Insert a new chef into the database"
  ([conx name cuisine]
   (jdbc/execute-one! conx ["insert into chefs(name, cuisine) values(?, ?)" name cuisine]))
  ([conx name cuisine nationality]
   (jdbc/execute-one! conx ["insert into chefs(name, cuisine, nationality) values(?, ?, ?)" name cuisine nationality])))


(def fixup-names {"Koji Kobayashi" "Kōji Kobayashi"})
(defn get-chef-by-name
  [conx name]
  (let [name (get fixup-names name name)]
    (jdbc/execute-one! conx ["select * from chefs where name = ?" name])))

;; html wrangling

(defn parse-html-file []
  (h/as-hickory
   (h/parse (slurp "List of Iron Chef episodes - Wikipedia.html"))))

(defn get-tables [html-doc]
  (hs/select (hs/class "wikitable") html-doc))

(defn get-rows [html-table]
  (hs/select (hs/tag "tr") html-table))

(defn get-first-row [html-table]
  (first (get-rows html-table)))

(defn get-column-headers [html-row]
  (hs/select (hs/tag "th") html-row))

(defn get-column-fields [html-row]
  (hs/select (hs/tag "td") html-row))

(defn remove-bracket-text [astring]
  (s/replace astring #"\(.+\)" ""))

(defn row-data [table-row]
  (->> table-row
       (map :content)
       (map last)
       (map str)
       (map s/trim)))

(defn element-text [html-element]
  (cond
    (string? html-element) html-element
    (= :sup (:tag html-element)) ""
    :else (->> html-element
               (:content)
               (map element-text)
               (apply str)
               (remove-bracket-text)
               (s/trim))))

;; (get-headers (first (get-tables (parse-html-file))))
(defn get-headers [html-table]
  (row-data
   (get-column-headers
    (get-first-row html-table))))

(defn assoc-multi [a k v]
  (assoc a k (conj (get a k []) v)))

(defn zipmulti
  "Like zipmap, but allow repeated keys by collecting their values into value vectors"
  ([key-seq val-seq]
   (zipmulti {} key-seq val-seq))
  ([acc key-seq val-seq]
   (if-let [k (first key-seq)]
     (recur (assoc-multi acc k (first val-seq))
            (rest key-seq)
            (rest val-seq))
     acc)))

(defn table-to-maps [html-table]
  (let [headers (get-headers html-table)
        rows (map get-column-fields (rest (get-rows html-table)))]
    (map #(zipmulti headers %) rows)))

(defn modify-map-values [amap f]
  (reduce (fn [a [k v]] (assoc a k (f v)))
          {}
          amap))

(defn challenger-nationality
  "Get the first available challenger nationality from the row (ignoring others)"
  [table-row]
  (get-in table-row ["Challenger" 0 :content 0 :content 0 :attrs :title]))

;; processing

(defn get-or-create-chef-id!
  ([conx name cuisine]
   (get-or-create-chef-id! conx name cuisine nil))
  ([conx name cuisine nationality]
   (if-let [row (get-chef-by-name conx name)]
     (:chefs/id row)
     (do
       (create-chef! conx name cuisine nationality)
       (:chefs/id (get-chef-by-name conx name))))))

;; This is necessary because the Iron Chefs' specialties is not
;; mentioned in the Wikipedia episodes page
(defn bootstrap-iron-chefs! [conx]
  (create-chef! conx "Yutaka Ishinabe"    "French")
  (create-chef! conx "Rokusaburo Michiba" "Japanese")
  (create-chef! conx "Chen Kenichi"       "Chinese")
  (create-chef! conx "Hiroyuki Sakai"     "French")
  (create-chef! conx "Komei Nakamura"     "Japanese"))

(defn add-iron-chef-to-battle! [conx iron-chef-id battle-id]
  (jdbc/execute-one! conx ["insert into iron_chefs_battles(iron_chef_id, battle_id) values (?, ?)" iron-chef-id battle-id]))

(defn add-challenger-to-battle! [conx challenger-id battle-id]
  (jdbc/execute-one! conx ["insert into challengers_battles(challenger_id, battle_id) values (?, ?)" challenger-id battle-id]))

(defn add-winner-to-battle! [conx winner-id battle-id]
  (jdbc/execute-one! conx ["insert into winners_battles(winner_id, battle_id) values (?, ?)" winner-id battle-id]))

;; Backwards compatibility wrappers - for single-battle episodes
(defn create-episode-with-battle!
  "Create an episode with a single battle. Returns battle-id for adding chefs/winners."
  [conx episode-id air-date theme-ingredient]
  (create-episode! conx episode-id air-date)
  (create-battle! conx episode-id 1 theme-ingredient))

(defn add-iron-chef-to-episode! [conx iron-chef-id episode-id]
  ;; Find the first battle for this episode and add chef to it
  (let [battle-id (:battles/id (jdbc/execute-one! conx ["select id from battles where episode_id = ? order by battle_number limit 1" episode-id]))]
    (add-iron-chef-to-battle! conx iron-chef-id battle-id)))

(defn add-challenger-to-episode! [conx challenger-id episode-id]
  (let [battle-id (:battles/id (jdbc/execute-one! conx ["select id from battles where episode_id = ? order by battle_number limit 1" episode-id]))]
    (add-challenger-to-battle! conx challenger-id battle-id)))

(defn add-winner-to-episode! [conx winner-id episode-id]
  (let [battle-id (:battles/id (jdbc/execute-one! conx ["select id from battles where episode_id = ? order by battle_number limit 1" episode-id]))]
    (add-winner-to-battle! conx winner-id battle-id)))

(defn get-all-iron-chefs [conx]
  (jdbc/execute! conx ["select * from chefs where id in (select distinct iron_chef_id from iron_chefs_battles)"]))

(defn get-all-challengers [conx]
  (jdbc/execute! conx ["select * from chefs where id in (select distinct challenger_id from challengers_battles)"]))


;; ROW PROCESSING

(defn make-text-map
  "From a row map, make an easy map of the column names to the text of the first field with that column name"
  [table-row-map]
  (modify-map-values table-row-map #(element-text (first %))))

(defn get-iron-chef-names [text-map]
  (if-let [chef-names (get text-map "Iron Chef")]
    (s/split chef-names
             #"\s+&\s+")
    []))

(defn write-iron-chefs! [conx episode-id table-row-map]
  (let [text-map (make-text-map table-row-map)
        chef-names (get-iron-chef-names text-map)]
    (doseq [name chef-names]
      (add-iron-chef-to-episode! conx
                                 (get-or-create-chef-id! conx name nil)
                                 episode-id))))


(defn get-challenger-names-multi [row-map]
  ;; FIXME this is totally broken, why did I think it worked?
  (flatten (map #(s/split (remove-bracket-text (element-text %)) #"\s+&\s+")
                (get row-map "Challenger"))))


(defn get-winner-names-multi [row-map]
  (flatten (map #(s/split (remove-bracket-text (element-text %)) #"\s+&\s+")
                (get row-map "Winner"))))

(defn write-challengers! [conx episode-id table-row-map]

  (let [text-map (make-text-map table-row-map)
        challenger-nationality (challenger-nationality table-row-map)
        [first-chef-name & chef-names] (get-challenger-names-multi table-row-map)]

    (add-challenger-to-episode! conx
                                (get-or-create-chef-id! conx
                                                        first-chef-name
                                                        (get text-map "Challenger Specialty")
                                                        challenger-nationality)
                                episode-id)

    (doseq [chef-name chef-names]
      (add-challenger-to-episode! conx
                                (get-or-create-chef-id! conx
                                                        chef-name
                                                        nil
                                                        nil)
                                episode-id))))

(defn write-winners! [conx episode-id table-row-map]
  (let [text-map (make-text-map table-row-map)
        chef-names (get-winner-names-multi table-row-map)]
    (doseq [name chef-names]
      ;; Winner should already exist (as Iron Chef or challenger)
      ;; Use get-or-create-chef-id! as defensive measure in case of name mismatch
      (add-winner-to-episode! conx
                              (get-or-create-chef-id! conx name nil)
                              episode-id))))

(defn process-row-map! [conx table-row-map]
  ;; TODO stop recreating the text-map everywhere
  (let [text-map (make-text-map table-row-map)
        episode-id (get text-map "Episode #")]

    ;; Skip malformed rows (e.g., from rowspan issues where episode # is not numeric)
    (when (and episode-id (re-matches #"\d+" episode-id))
      (create-episode-with-battle! conx
                                   episode-id
                                   (get text-map "Original airdate")
                                   (get text-map "Theme Ingredient"))

      (write-iron-chefs! conx episode-id table-row-map)
      (write-challengers! conx episode-id table-row-map)
      (write-winners! conx episode-id table-row-map))))

(defn process-table! [conx html-table]
  (doseq [row (table-to-maps html-table)]
    (process-row-map! conx row)))

(defn spanned-rows-to-map
  "Given two table rows, with the first row having td elements that span both rows, convert into a single multi-value map with headings as keys"
  ([headings row-a row-b]
   (spanned-rows-to-map {} headings row-a row-b))
  ([acc headings row-a row-b]
   (if-let [k (first headings)]
     (let [e1 (first row-a)
           rowspan (get-in e1 [:attrs :rowspan])
           acc' (assoc-multi acc k (element-text e1))]

       (recur (if rowspan  ;; assumes this is only ever "2"
                acc'
                (assoc-multi acc' k (element-text (first row-b))))
              (rest headings)
              (rest row-a)
              (if rowspan
                row-b
                (rest row-b))))
     acc)))

(defn process-stupid-table! [conx html-table]
  (let [headers (get-headers html-table)
        rows (get-rows html-table)
        combined-row (apply (partial spanned-rows-to-map headers)
                            (map get-column-fields (rest rows)))]
    (process-row-map! conx combined-row)))

;; 1995 has a few weirdnesses, like the first episode having Toshiro
;; Kandagawa listed as an Iron Chef *and* being a double-wide. This is
;; the point where I lost patience and started brute forcing things,
;; this whole project having gone on about 50 times longer than I
;; intended and not actually being necessary for any reason

(defn write-episode-61! [conx]
  ;; Episode 61: 1995 New Year Special - 1994 Mr. Iron Chef (January 2, 1995)
  ;; Battle 1: Kandagawa vs Shimizu (Abalone) - Kandagawa wins (preliminaries final, challenger vs challenger)
  ;; Battle 2: Michiba vs Kandagawa (Yellowtail) - Michiba wins (finals)

  (create-episode! conx 61 "January 2, 1995")

  ;; Battle 1: Kandagawa vs Shimizu (Abalone) - preliminaries final, no Iron Chef
  (let [battle1-id (create-battle! conx 61 1 "Abalone")
        kandagawa-id (:chefs/id (get-chef-by-name conx "Toshirō Kandagawa"))
        shimizu-id (get-or-create-chef-id! conx "Tadaaki Shimizu" "Japanese" "Japan")]
    (add-challenger-to-battle! conx kandagawa-id battle1-id)
    (add-challenger-to-battle! conx shimizu-id battle1-id)
    (add-winner-to-battle! conx kandagawa-id battle1-id))

  ;; Battle 2: Michiba vs Kandagawa (Yellowtail) - finals
  (let [battle2-id (create-battle! conx 61 2 "Yellowtail")
        michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        kandagawa-id (:chefs/id (get-chef-by-name conx "Toshirō Kandagawa"))]
    (add-iron-chef-to-battle! conx michiba-id battle2-id)
    (add-challenger-to-battle! conx kandagawa-id battle2-id)
    (add-winner-to-battle! conx michiba-id battle2-id)))

(defn write-episode-73!
  ;; Episode 73: Hong Kong Special (March 31, 1995)
  ;; Battle 1: Chen vs Leung Waikei (Pork) - Chen wins
  ;; Battle 2: Michiba vs Chow Chung (Spiny lobster) - Michiba wins
  [conx]
  (create-chef! conx "Leung Waikei" "Chinese (Cantonese)" "Hong Kong")
  (create-chef! conx "Chow Chung"   "Chinese (Cantonese)" "Hong Kong")

  (create-episode! conx 73 "March 31, 1995")

  ;; Battle 1: Chen vs Leung Waikei (Pork)
  (let [battle1-id (create-battle! conx 73 1 "Pork")
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))
        leung-id (:chefs/id (get-chef-by-name conx "Leung Waikei"))]
    (add-iron-chef-to-battle! conx chen-id battle1-id)
    (add-challenger-to-battle! conx leung-id battle1-id)
    (add-winner-to-battle! conx chen-id battle1-id))

  ;; Battle 2: Michiba vs Chow Chung (Spiny lobster)
  (let [battle2-id (create-battle! conx 73 2 "Spiny lobster")
        michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        chow-id (:chefs/id (get-chef-by-name conx "Chow Chung"))]
    (add-iron-chef-to-battle! conx michiba-id battle2-id)
    (add-challenger-to-battle! conx chow-id battle2-id)
    (add-winner-to-battle! conx michiba-id battle2-id)))

(defn write-episode-99! [conx]
  ;; Episode 99: 1995 Iron Chef World Cup at Ariake Coliseum (October 6, 1995)
  ;; Single-elimination tournament: French, Italian, Japanese, Chinese cuisines
  ;; Battle 1: Gagnaire vs Vissani (Tuna) - Vissani wins (semifinal, challenger vs challenger)
  ;; Battle 2: Michiba vs Hsu Cheng (Squid) - Michiba wins (semifinal)
  ;; Battle 3: Michiba vs Vissani (Duck) - Michiba wins (final)
  (create-chef! conx "Pierre Gagnaire" "French" "France")
  (create-chef! conx "Gianfranco Vissani" "Italian" "Italy")
  (create-chef! conx "Hsu Cheng" "Chinese (Cantonese)" "Hong Kong")

  (create-episode! conx 99 "October 6, 1995")

  ;; Battle 1: Gagnaire vs Vissani (Tuna) - Vissani wins (semifinal, no Iron Chef)
  (let [battle1-id (create-battle! conx 99 1 "Tuna")
        gagnaire-id (:chefs/id (get-chef-by-name conx "Pierre Gagnaire"))
        vissani-id (:chefs/id (get-chef-by-name conx "Gianfranco Vissani"))]
    (add-challenger-to-battle! conx gagnaire-id battle1-id)
    (add-challenger-to-battle! conx vissani-id battle1-id)
    (add-winner-to-battle! conx vissani-id battle1-id))

  ;; Battle 2: Michiba vs Hsu Cheng (Squid) - Michiba wins (semifinal)
  (let [battle2-id (create-battle! conx 99 2 "Squid")
        michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        hsu-id (:chefs/id (get-chef-by-name conx "Hsu Cheng"))]
    (add-iron-chef-to-battle! conx michiba-id battle2-id)
    (add-challenger-to-battle! conx hsu-id battle2-id)
    (add-winner-to-battle! conx michiba-id battle2-id))

  ;; Battle 3: Michiba vs Vissani (Duck) - Michiba wins (final)
  (let [battle3-id (create-battle! conx 99 3 "Duck")
        michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        vissani-id (:chefs/id (get-chef-by-name conx "Gianfranco Vissani"))]
    (add-iron-chef-to-battle! conx michiba-id battle3-id)
    (add-challenger-to-battle! conx vissani-id battle3-id)
    (add-winner-to-battle! conx michiba-id battle3-id)))

(defn write-episode-101-102! [conx]
  (create-chef! conx "Lin Kunbi" "Chinese (Fujian)" "China")
  (create-episode-with-battle! conx 101 "October 20, 1995" "Potato")
  (create-episode-with-battle! conx 102 "October 27, 1995" "Sweet potato")
  (let [michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        lin-id (:chefs/id (get-chef-by-name conx "Lin Kunbi"))]
    (add-iron-chef-to-episode! conx michiba-id 101)
    (add-challenger-to-episode! conx lin-id 101)
    (add-iron-chef-to-episode! conx michiba-id 102)
    (add-challenger-to-episode! conx lin-id 102)
    (add-winner-to-episode! conx michiba-id 102)))

(defn write-episode-110! [conx]
  (create-episode-with-battle! conx 110 "December 22, 1995" "Chicken")
  (let [sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))]
    (add-iron-chef-to-episode! conx sakai-id 110)
    (add-iron-chef-to-episode! conx chen-id 110)
    (add-winner-to-episode! conx chen-id 110)))

(defn write-episode-111! [conx]
  (create-episode-with-battle! conx 111 "January 3, 1996" "Beef")
  (let [michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))]
    (add-iron-chef-to-episode! conx michiba-id 111)
    (add-iron-chef-to-episode! conx chen-id 111)
    (add-winner-to-episode! conx michiba-id 111)))

(defn process-1995-table! [conx html-table]
  (let [headers (get-headers html-table)
        rows (get-rows html-table)
        row-maps (table-to-maps html-table)]
    (write-episode-61! conx)
    (doseq [row (take 11 (drop 2 row-maps))]
      (process-row-map! conx row))
    (write-episode-73! conx)
    (doseq [row (take 25 (drop 15 row-maps))]
      (process-row-map! conx row))
    (write-episode-99! conx)
    (process-row-map! conx (nth row-maps 43))
    (write-episode-101-102! conx)
    (doseq [row (drop 46 row-maps)]
      (process-row-map! conx row)))
  )

(defn write-episode-124! [conx]
  ;; Episode 124: France Special at Château de Brissac (April 12, 1996)
  ;; Battle 1: Nakamura vs Bernard Leprince (Salmon) - Leprince wins
  ;; Battle 2: Sakai vs Pierre Gagnaire (Lobster) - Gagnaire wins
  (create-chef! conx "Bernard Leprince" "French" "France")

  (create-episode! conx 124 "April 12, 1996")

  ;; Battle 1: Nakamura vs Leprince (Salmon)
  (let [battle1-id (create-battle! conx 124 1 "Salmon")
        nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))
        leprince-id (:chefs/id (get-chef-by-name conx "Bernard Leprince"))]
    (add-iron-chef-to-battle! conx nakamura-id battle1-id)
    (add-challenger-to-battle! conx leprince-id battle1-id)
    (add-winner-to-battle! conx leprince-id battle1-id))

  ;; Battle 2: Sakai vs Gagnaire (Lobster)
  (let [battle2-id (create-battle! conx 124 2 "Lobster")
        sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))
        gagnaire-id (:chefs/id (get-chef-by-name conx "Pierre Gagnaire"))]
    (add-iron-chef-to-battle! conx sakai-id battle2-id)
    (add-challenger-to-battle! conx gagnaire-id battle2-id)
    (add-winner-to-battle! conx gagnaire-id battle2-id)))

(defn write-episode-149! [conx]
  ;; Episode 149: China vs Japan Special (October 11, 1996)
  ;; Battle 1: Chen Kenichi vs Sun Liping, Su Dexing, Zhuang Weijia (Chicken) - Tie
  ;; Battle 2: Chen Kenichi vs Sun Liping (Shark fin) - Tie-breaker, both win

  ;; Create the three Chinese challengers (each with different regional cuisine)
  (create-chef! conx "Sun Liping" "Chinese (Beijing)" "China")
  (create-chef! conx "Su Dexing" "Chinese (Shanghai)" "China")
  (create-chef! conx "Zhuang Weijia" "Chinese (Cantonese)" "China")

  (create-episode! conx 149 "October 11, 1996")

  ;; Battle 1: Chen vs Team China (Chicken) - Tie
  (let [battle1-id (create-battle! conx 149 1 "Chicken")
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))]
    (add-iron-chef-to-battle! conx chen-id battle1-id)
    (doseq [name ["Sun Liping" "Su Dexing" "Zhuang Weijia"]]
      (add-challenger-to-battle! conx
                                 (:chefs/id (get-chef-by-name conx name))
                                 battle1-id))
    ;; Tie - both Chen and Sun Liping win
    (add-winner-to-battle! conx chen-id battle1-id)
    (add-winner-to-battle! conx (:chefs/id (get-chef-by-name conx "Sun Liping")) battle1-id))

  ;; Battle 2: Chen vs Sun Liping (Shark fin) - Tiebreaker, Chen wins
  (let [battle2-id (create-battle! conx 149 2 "Shark fin")
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))
        sun-id (:chefs/id (get-chef-by-name conx "Sun Liping"))]
    (add-iron-chef-to-battle! conx chen-id battle2-id)
    (add-challenger-to-battle! conx sun-id battle2-id)
    (add-winner-to-battle! conx chen-id battle2-id)))

(defn write-episode-160! [conx]
  ;; Episode 160 is a special New Year's Eve (Omisoka) episode
  ;; Iron Chef vs Iron Chef battle (no challenger)
  ;; Komei Nakamura vs Rokusaburo Michiba

  (create-episode-with-battle! conx 160 "December 31, 1996" "Osechi (Pork, sweet potato, & octopus)")

  ;; Add both Iron Chefs to the episode
  (let [nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))
        michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))]
    (add-iron-chef-to-episode! conx nakamura-id 160)
    (add-iron-chef-to-episode! conx michiba-id 160)
    ;; Komei Nakamura wins
    (add-winner-to-episode! conx nakamura-id 160)))

(defn write-episode-190! [conx]
  ;; Episode 190 is a special Iron Chef team battle
  ;; Team 1: Chen Kenichi & Komei Nakamura
  ;; Team 2: Hiroyuki Sakai & Masahiko Kobe
  ;; All 4 participants are Iron Chefs competing in teams
  ;; Theme: Watermelon
  ;; Winners: Hiroyuki Sakai & Masahiko Kobe

  ;; Create Masahiko Kobe if he doesn't exist (later Iron Chef Italian)
  (when-not (get-chef-by-name conx "Masahiko Kobe")
    (create-chef! conx "Masahiko Kobe" "Italian"))

  (create-episode-with-battle! conx 190 "August 8, 1997" "Watermelon")

  ;; Add all four Iron Chefs to the episode
  (let [chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))
        nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))
        sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))
        kobe-id (:chefs/id (get-chef-by-name conx "Masahiko Kobe"))]
    (add-iron-chef-to-episode! conx chen-id 190)
    (add-iron-chef-to-episode! conx nakamura-id 190)
    (add-iron-chef-to-episode! conx sakai-id 190)
    (add-iron-chef-to-episode! conx kobe-id 190)
    ;; Winners: Sakai and Kobe
    (add-winner-to-episode! conx sakai-id 190)
    (add-winner-to-episode! conx kobe-id 190)))

(defn write-episode-163! [conx]
  ;; Episode 163: Hiroyuki Sakai vs Rory Kennedy
  ;; Theme: European rabbit
  ;; Result: Tie (both winners)

  (create-episode-with-battle! conx 163 "January 24, 1997" "European rabbit")

  ;; Add Iron Chef Sakai
  (let [sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))]
    (add-iron-chef-to-episode! conx sakai-id 163)
    ;; Sakai is a winner (tie)
    (add-winner-to-episode! conx sakai-id 163))

  ;; Add challenger Rory Kennedy
  (let [kennedy-id (get-or-create-chef-id! conx "Rory Kennedy" "Gibier" nil)]
    (add-challenger-to-episode! conx kennedy-id 163)
    ;; Kennedy is also a winner (tie)
    (add-winner-to-episode! conx kennedy-id 163)))

(defn write-episode-164! [conx]
  ;; Episode 164: Rematch - Hiroyuki Sakai vs Rory Kennedy
  ;; Theme: European pigeon
  ;; Wikipedia table has rowspan issues causing malformed data

  (create-episode-with-battle! conx 164 "January 31, 1997" "European pigeon")

  ;; Add Iron Chef Sakai
  (let [sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))]
    (add-iron-chef-to-episode! conx sakai-id 164)
    ;; Sakai wins this time
    (add-winner-to-episode! conx sakai-id 164))

  ;; Add challenger Rory Kennedy
  (let [kennedy-id (:chefs/id (get-chef-by-name conx "Rory Kennedy"))]
    (add-challenger-to-episode! conx kennedy-id 164)))

(defn write-episode-193! [conx]
  ;; Episode 193: Komei Nakamura vs Yoshinori Kojima
  ;; Theme: Potato
  ;; Result: No Contest

  (create-episode-with-battle! conx 193 "August 29, 1997" "Potato")

  ;; Add Iron Chef Nakamura
  (let [nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))]
    (add-iron-chef-to-episode! conx nakamura-id 193))

  ;; Add challenger Yoshinori Kojima
  (let [kojima-id (get-or-create-chef-id! conx "Yoshinori Kojima" "French" nil)]
    (add-challenger-to-episode! conx kojima-id 193))
  ;; No Contest - no winner added
  )

(defn write-episode-194! [conx]
  ;; Episode 194: Rematch - Komei Nakamura vs Yoshinori Kojima
  ;; Theme: Marbled sole
  ;; Wikipedia table has rowspan issues causing malformed data

  (create-episode-with-battle! conx 194 "September 5, 1997" "Marbled sole")

  ;; Add Iron Chef Nakamura
  (let [nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))]
    (add-iron-chef-to-episode! conx nakamura-id 194)
    ;; Nakamura wins the rematch
    (add-winner-to-episode! conx nakamura-id 194))

  ;; Add challenger Yoshinori Kojima
  (let [kojima-id (:chefs/id (get-chef-by-name conx "Yoshinori Kojima"))]
    (add-challenger-to-episode! conx kojima-id 194)))

(defn write-episode-198! [conx]
  ;; Episode 198: 1997 Iron Chef World Cup (October 10, 1997)
  ;; Battle 1: Nakamura vs Liu Xikun (Beef) - Nakamura wins
  ;; Battle 2: Passard vs Patrick Clark (Lobster) - Passard wins (challenger vs challenger)
  ;; Battle 3: Nakamura vs Passard (Foie gras) - Draw (no winner)

  (when-not (get-chef-by-name conx "Liu Xikun")
    (create-chef! conx "Liu Xikun" "Chinese (Cantonese)" "China"))
  (when-not (get-chef-by-name conx "Alain Passard")
    (create-chef! conx "Alain Passard" "French" "France"))
  (when-not (get-chef-by-name conx "Patrick Clark")
    (create-chef! conx "Patrick Clark" "New American" "United States"))

  ;; Create episode
  (create-episode! conx 198 "October 10, 1997")

  ;; Battle 1: Nakamura vs Liu Xikun (Beef)
  (let [battle1-id (create-battle! conx 198 1 "Beef")
        nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))
        liu-id (:chefs/id (get-chef-by-name conx "Liu Xikun"))]
    (add-iron-chef-to-battle! conx nakamura-id battle1-id)
    (add-challenger-to-battle! conx liu-id battle1-id)
    (add-winner-to-battle! conx nakamura-id battle1-id))

  ;; Battle 2: Passard vs Patrick Clark (Lobster) - challenger vs challenger
  (let [battle2-id (create-battle! conx 198 2 "Lobster")
        passard-id (:chefs/id (get-chef-by-name conx "Alain Passard"))
        clark-id (:chefs/id (get-chef-by-name conx "Patrick Clark"))]
    (add-challenger-to-battle! conx passard-id battle2-id)
    (add-challenger-to-battle! conx clark-id battle2-id)
    (add-winner-to-battle! conx passard-id battle2-id))

  ;; Battle 3: Nakamura vs Passard (Foie gras) - Draw (no winner)
  (let [battle3-id (create-battle! conx 198 3 "Foie gras")
        nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))
        passard-id (:chefs/id (get-chef-by-name conx "Alain Passard"))]
    (add-iron-chef-to-battle! conx nakamura-id battle3-id)
    (add-challenger-to-battle! conx passard-id battle3-id)
    ;; No winner - this was a draw
    ))

(defn process-1997-table! [conx html-table]
  ;; Table 8: Episodes 161-208 (1997)
  ;; Episodes 163-164, 190, 193-194, 198 have special handling
  (let [row-maps (table-to-maps html-table)]
    ;; Process rows 0-1 (episodes 161-162)
    (doseq [row (take 2 row-maps)]
      (process-row-map! conx row))
    ;; Episodes 163-164 - special handling due to rowspan/tie
    (write-episode-163! conx)
    (write-episode-164! conx)
    ;; Process rows 4-28 (episodes 165-189)
    (doseq [row (take 25 (drop 4 row-maps))]
      (process-row-map! conx row))
    ;; Episode 190 - special team battle
    (write-episode-190! conx)
    ;; Process rows 30-31 (episodes 191-192)
    (doseq [row (take 2 (drop 30 row-maps))]
      (process-row-map! conx row))
    ;; Episodes 193-194 - special handling due to rowspan/no contest
    (write-episode-193! conx)
    (write-episode-194! conx)
    ;; Process rows 34-36 (episodes 195-197)
    (doseq [row (take 3 (drop 34 row-maps))]
      (process-row-map! conx row))
    ;; Episode 198 - special 3-battle episode (rows 37-39 are malformed)
    (write-episode-198! conx)
    ;; Process rows 40-49 (episodes 199-208)
    (doseq [row (drop 40 row-maps)]
      (process-row-map! conx row))))

(defn process-1996-table! [conx html-table]
  (let [headers (get-headers html-table)
        rows (get-rows html-table)
        row-maps (table-to-maps html-table)]
    (doseq [row (take 12 row-maps)]
      (process-row-map! conx row))
    (write-episode-124! conx)
    (doseq [row (take 24 (drop 14 row-maps))]
      (process-row-map! conx row))
    (write-episode-149! conx)
    ;; Process remaining rows for 1996 episodes
    ;; Skip rows 38-39 (episode 149 handled above due to rowspan issues)
    (doseq [row (drop 40 row-maps)]
      (process-row-map! conx row))
    (println (get-all-episodes conx))
    ))

;; === 2000-2002 Special Episodes (Table 11) ===
;; These are special events without numeric episode IDs
;; We assign IDs 292-295 continuing the sequence after episode 291

(defn write-millennium-cup! [conx]
  ;; Millennium Cup (January 5, 2000) - Episode 292
  ;; Battle 1: Chen Kenichi vs Zhao Renliang (Abalone) - Chen wins
  ;; Battle 2: Rokusaburo Michiba vs Dominique Bouchet (Kobe beef) - Michiba wins

  (when-not (get-chef-by-name conx "Zhao Renliang")
    (create-chef! conx "Zhao Renliang" "Chinese" "China"))
  (when-not (get-chef-by-name conx "Dominique Bouchet")
    (create-chef! conx "Dominique Bouchet" "French" "France"))

  (create-episode! conx 292 "January 5, 2000")

  ;; Battle 1: Chen vs Zhao (Abalone)
  (let [battle1-id (create-battle! conx 292 1 "Abalone")
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))
        zhao-id (:chefs/id (get-chef-by-name conx "Zhao Renliang"))]
    (add-iron-chef-to-battle! conx chen-id battle1-id)
    (add-challenger-to-battle! conx zhao-id battle1-id)
    (add-winner-to-battle! conx chen-id battle1-id))

  ;; Battle 2: Michiba vs Bouchet (Kobe beef)
  (let [battle2-id (create-battle! conx 292 2 "Kobe beef")
        michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        bouchet-id (:chefs/id (get-chef-by-name conx "Dominique Bouchet"))]
    (add-iron-chef-to-battle! conx michiba-id battle2-id)
    (add-challenger-to-battle! conx bouchet-id battle2-id)
    (add-winner-to-battle! conx michiba-id battle2-id)))

(defn write-new-york-special! [conx]
  ;; New York Special (March 28, 2000) - Episode 293
  ;; Masaharu Morimoto vs Bobby Flay (Rock crab) - Morimoto wins

  (when-not (get-chef-by-name conx "Masaharu Morimoto")
    (create-chef! conx "Masaharu Morimoto" "Japanese"))
  (when-not (get-chef-by-name conx "Bobby Flay")
    (create-chef! conx "Bobby Flay" "Southwestern" "United States"))

  (create-episode! conx 293 "March 28, 2000")

  (let [battle-id (create-battle! conx 293 1 "Rock crab")
        morimoto-id (:chefs/id (get-chef-by-name conx "Masaharu Morimoto"))
        flay-id (:chefs/id (get-chef-by-name conx "Bobby Flay"))]
    (add-iron-chef-to-battle! conx morimoto-id battle-id)
    (add-challenger-to-battle! conx flay-id battle-id)
    (add-winner-to-battle! conx morimoto-id battle-id)))

(defn write-21st-century-battles! [conx]
  ;; 21st Century Battles (January 2, 2001) - Episode 294
  ;; Battle 1: Hiroyuki Sakai vs Toshirō Kandagawa (Red snapper) - Kandagawa wins
  ;; Battle 2: Masaharu Morimoto vs Bobby Flay (Spiny lobster) - Bobby Flay wins

  (create-episode! conx 294 "January 2, 2001")

  ;; Battle 1: Sakai vs Kandagawa (Red snapper)
  (let [battle1-id (create-battle! conx 294 1 "Red snapper")
        sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))
        kandagawa-id (:chefs/id (get-chef-by-name conx "Toshirō Kandagawa"))]
    (add-iron-chef-to-battle! conx sakai-id battle1-id)
    (add-challenger-to-battle! conx kandagawa-id battle1-id)
    (add-winner-to-battle! conx kandagawa-id battle1-id))

  ;; Battle 2: Morimoto vs Flay (Spiny lobster)
  (let [battle2-id (create-battle! conx 294 2 "Spiny lobster")
        morimoto-id (:chefs/id (get-chef-by-name conx "Masaharu Morimoto"))
        flay-id (:chefs/id (get-chef-by-name conx "Bobby Flay"))]
    (add-iron-chef-to-battle! conx morimoto-id battle2-id)
    (add-challenger-to-battle! conx flay-id battle2-id)
    (add-winner-to-battle! conx flay-id battle2-id)))

(defn write-japan-cup! [conx]
  ;; Japan Cup (January 2, 2002) - Episode 295
  ;; Battle 1: Chen Kenichi vs Yūichirō Ebisu (King crab) - Chen wins
  ;; Battle 2: Nonaga vs Tanabe (Pacific bluefin tuna) - Nonaga wins (challenger vs challenger)
  ;; Battle 3: Chen Kenichi vs Kimio Nonaga (Ingii chicken) - Nonaga wins

  (when-not (get-chef-by-name conx "Yūichirō Ebisu")
    (create-chef! conx "Yūichirō Ebisu" "Italian" "Japan"))
  (when-not (get-chef-by-name conx "Kimio Nonaga")
    (create-chef! conx "Kimio Nonaga" "Japanese" "Japan"))
  (when-not (get-chef-by-name conx "Takeshi Tanabe")
    (create-chef! conx "Takeshi Tanabe" "French" "Japan"))

  (create-episode! conx 295 "January 2, 2002")

  ;; Battle 1: Chen vs Ebisu (King crab)
  (let [battle1-id (create-battle! conx 295 1 "King crab")
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))
        ebisu-id (:chefs/id (get-chef-by-name conx "Yūichirō Ebisu"))]
    (add-iron-chef-to-battle! conx chen-id battle1-id)
    (add-challenger-to-battle! conx ebisu-id battle1-id)
    (add-winner-to-battle! conx chen-id battle1-id))

  ;; Battle 2: Nonaga vs Tanabe (Pacific bluefin tuna) - challenger vs challenger
  (let [battle2-id (create-battle! conx 295 2 "Pacific bluefin tuna")
        nonaga-id (:chefs/id (get-chef-by-name conx "Kimio Nonaga"))
        tanabe-id (:chefs/id (get-chef-by-name conx "Takeshi Tanabe"))]
    (add-challenger-to-battle! conx nonaga-id battle2-id)
    (add-challenger-to-battle! conx tanabe-id battle2-id)
    (add-winner-to-battle! conx nonaga-id battle2-id))

  ;; Battle 3: Chen vs Nonaga (Ingii chicken)
  (let [battle3-id (create-battle! conx 295 3 "Ingii chicken")
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))
        nonaga-id (:chefs/id (get-chef-by-name conx "Kimio Nonaga"))]
    (add-iron-chef-to-battle! conx chen-id battle3-id)
    (add-challenger-to-battle! conx nonaga-id battle3-id)
    (add-winner-to-battle! conx nonaga-id battle3-id)))

(defn process-2000-2002-specials! [conx]
  ;; Process 2000-2002 special episodes (Table 11)
  ;; These are manually written due to complex rowspan structure
  (write-millennium-cup! conx)
  (write-new-york-special! conx)
  (write-21st-century-battles! conx)
  (write-japan-cup! conx))

(defn execute! [conx]
  (let [html-tables (get-tables (parse-html-file))]
    (bootstrap-iron-chefs! conx)
    (process-table! conx (first html-tables))
    (process-table! conx (second html-tables))
    (process-stupid-table! conx (nth html-tables 2))
    (process-1995-table! conx (nth html-tables 3))
    (write-episode-110! conx)
    (write-episode-111! conx)
    (process-1996-table! conx (nth html-tables 6))
    (write-episode-160! conx)
    ;; 1997 episodes (table 8)
    (process-1997-table! conx (nth html-tables 8))
    ;; 1998 episodes (table 9)
    (process-table! conx (nth html-tables 9))
    ;; 1999 episodes (table 10)
    (process-table! conx (nth html-tables 10))
    ;; 2000-2002 special episodes (table 11)
    (process-2000-2002-specials! conx))
  (comment
    (println (get-all-episodes conx))))

(defn main [argv]
  (with-open [conx (jdbc/get-connection (jdbc/get-datasource {:dbtype "sqlite" :dbname "index.sqlite"}))]
    (execute! conx)))
