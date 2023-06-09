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
  [conx id air-date theme-ingredient]
  (jdbc/execute-one! conx
                     ["insert into episodes(id, air_date, theme_ingredient) values(?, ?, ?)"
                      id
                      air-date
                      theme-ingredient]))

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

(defn add-iron-chef-to-episode! [conx iron-chef-id episode-id]
  (jdbc/execute-one! conx ["insert into iron_chefs_episodes(iron_chef_id, episode_id) values (?, ?)" iron-chef-id episode-id]))

(defn add-challenger-to-episode! [conx challenger-id episode-id]
  (jdbc/execute-one! conx ["insert into challengers_episodes(challenger_id, episode_id) values (?, ?)" challenger-id episode-id]))

(defn add-winner-to-episode! [conx winner-id episode-id]
  (jdbc/execute-one! conx ["insert into winners_episodes(winner_id, episode_id) values (?, ?)" winner-id episode-id]))

(defn get-all-iron-chefs [conx]
  (jdbc/execute! conx ["select * from chefs where id in (select distinct iron_chef_id from iron_chefs_episodes)"]))

(defn get-all-challengers [conx]
  (jdbc/execute! conx ["select * from chefs where id in (select distinct challenger_id from challengers_episodes)"]))


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
      (add-winner-to-episode! conx
                              (:chefs/id (get-chef-by-name conx name))
                              episode-id))))

(defn process-row-map! [conx table-row-map]
  ;; TODO stop recreating the text-map everywhere
  (let [text-map (make-text-map table-row-map)
        episode-id (get text-map "Episode #")]

    (create-episode! conx
                     episode-id
                     (get text-map "Original airdate")
                     (get text-map "Theme Ingredient"))

    (write-iron-chefs! conx episode-id table-row-map)
    (write-challengers! conx episode-id table-row-map)
    (write-winners! conx episode-id table-row-map)))

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
  ;; FIXME 1: This is almost the same as code in process-row, move out
  ;; FIXME 2: Now not all episodes have a single theme
  ;; ingredient. Isn't that great?
  (create-episode! conx 61 "January 2, 1995" "Abalone/Yellowtail")
  
  (doseq [name ["Toshirō Kandagawa" "Tadaaki Shimizu"]]
    (add-challenger-to-episode! conx
                                (:chefs/id (get-chef-by-name conx name))
                                61))
  (let [michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))]
    (add-iron-chef-to-episode! conx
                               michiba-id
                               61)
    (add-winner-to-episode! conx
                            michiba-id
                            61)))

(defn write-episode-73!
  ;; Episode 73 has two iron chefs and two new challengers with the
  ;; same specialty. I gave up at this point.
  [conx]
  (create-chef! conx "Leung Waikei" "Chinese (Cantonese)" "Hong Kong")
  (create-chef! conx "Chow Chung"   "Chinese (Cantonese)" "Hong Kong")
  (create-episode! conx 73 "March 31, 1995" "Pork/Spiny Lobster")
  (add-challenger-to-episode! conx (get-chef-by-name conx "Leung Waikei") 73)
  (add-challenger-to-episode! conx (get-chef-by-name conx "Chow Chung") 73)
  (let [michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))]
    (add-iron-chef-to-episode! conx
                               michiba-id
                               73)
    (add-winner-to-episode! conx
                            michiba-id
                            73))
  (let [chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))]
    (add-iron-chef-to-episode! conx
                               chen-id
                               73)
    (add-winner-to-episode! conx
                            chen-id
                            73)))

(defn write-episode-99! [conx]
  (create-chef! conx "Pierre Gagnaire" "French" "France")
  (create-chef! conx "Gianfranco Vissani" "Italian" "Italy")
  (create-chef! conx "Hsu Cheng" "Chinese (Cantonese)" "Hong Kong")
  (create-episode! conx 99 "October 6, 1995" "Tuna/Squid/Duck")
  (add-challenger-to-episode! conx (get-chef-by-name conx "Pierre Gagnaire") 99)
  (add-challenger-to-episode! conx (get-chef-by-name conx "Gianfranco Vissani") 99)
  (add-challenger-to-episode! conx (get-chef-by-name conx "Hsu Cheng") 99)
  (add-winner-to-episode! conx (get-chef-by-name conx "Gianfranco Vissani") 99)
  (let [michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))]
    (add-iron-chef-to-episode! conx michiba-id 99)
    (add-winner-to-episode! conx michiba-id 99)))

(defn write-episode-101-102! [conx]
  (create-chef! conx "Lin Kunbi" "Chinese (Fujian)" "China")
  (create-episode! conx 101 "October 20, 1995" "Potato")
  (create-episode! conx 102 "October 27, 1995" "Sweet potato")
  (let [michiba-id (:chefs/id (get-chef-by-name conx "Rokusaburo Michiba"))
        lin-id (:chefs/id (get-chef-by-name conx "Lin Kunbi"))]
    (add-iron-chef-to-episode! conx michiba-id 101)
    (add-challenger-to-episode! conx lin-id 101)
    (add-iron-chef-to-episode! conx michiba-id 102)
    (add-challenger-to-episode! conx lin-id 102)
    (add-winner-to-episode! conx michiba-id 102)))

(defn write-episode-110! [conx]
  (create-episode! conx 110 "December 22, 1995" "Chicken")
  (let [sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))
        chen-id (:chefs/id (get-chef-by-name conx "Chen Kenichi"))]
    (add-iron-chef-to-episode! conx sakai-id 110)
    (add-iron-chef-to-episode! conx chen-id 110)
    (add-winner-to-episode! conx chen-id 110)))

(defn write-episode-111! [conx]
  (create-episode! conx 111 "January 3, 1996" "Beef")
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
  (create-chef! conx "Bernard Leprince" "French" "France")
  (create-episode! conx 124 "April 12, 1996" "Salmon/Lobster")
  (let [gagnaire-id (:chefs/id (get-chef-by-name conx "Pierre Gagnaire"))
        leprince-id (:chefs/id (get-chef-by-name conx "Bernard Leprince"))
        sakai-id (:chefs/id (get-chef-by-name conx "Hiroyuki Sakai"))
        nakamura-id (:chefs/id (get-chef-by-name conx "Komei Nakamura"))]
    (add-iron-chef-to-episode! conx sakai-id 124)
    (add-iron-chef-to-episode! conx nakamura-id 124)
    (add-challenger-to-episode! conx gagnaire-id 124)
    (add-challenger-to-episode! conx leprince-id 124)
    (add-winner-to-episode! conx gagnaire-id 124)
    (add-winner-to-episode! conx leprince-id 124))
  )

(defn write-episode-149! [conx]
  )

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
    (println (get-all-episodes conx))
    ))

(defn execute! [conx]
  (let [html-tables (get-tables (parse-html-file))]
    (bootstrap-iron-chefs! conx)
    (process-table! conx (first html-tables))
    (process-table! conx (second html-tables))
    (process-stupid-table! conx (nth html-tables 2))
    (process-1995-table! conx (nth html-tables 3))
    (write-episode-110! conx)
    (write-episode-111! conx)
    (process-1996-table! conx (nth html-tables 6)))
  (comment
    (println (get-all-episodes conx))))

(defn main [argv]
  (with-open [conx (jdbc/get-connection (jdbc/get-datasource {:dbtype "sqlite" :dbname "index.sqlite"}))]
    (execute! conx)))
