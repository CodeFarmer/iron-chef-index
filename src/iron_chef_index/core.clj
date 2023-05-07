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
  (create-chef! conx "Hiroyuki Sakai"     "French"))

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

    (jdbc/execute-one! conx ["insert into episodes(id, air_date, theme_ingredient) values(?, ?, ?)"
                             episode-id
                             (get text-map "Original airdate")
                             (get text-map "Theme Ingredient")])

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

(defn execute! [conx]
  (let [html-tables (get-tables (parse-html-file))]
    (bootstrap-iron-chefs! conx)
    (process-table! conx (first html-tables))
    (process-table! conx (second html-tables))
    (process-stupid-table! conx (nth html-tables 2))))

(defn main [argv]
  (with-open [conx (jdbc/get-connection (jdbc/get-datasource {:dbtype "sqlite" :dbname "index.sqlite"}))]
    (execute! conx)))
