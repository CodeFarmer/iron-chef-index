(ns iron-chef-index.core
  (:require [clojure.string :as s]
            [next.jdbc :as jdbc]
            [hickory.core :as h]
            [hickory.select :as hs]))

(defn get-all-chefs
  "Retrieve all chefs from the database"
  [conx]
  (jdbc/execute! conx ["select * from chefs"]))

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

(defn table-to-maps [html-table]
  (let [headers (get-headers html-table)
        rows (map get-column-fields (rest (get-rows html-table)))]
    (map #(zipmap headers %) rows)))

(defn modify-map-values [amap f]
  (reduce (fn [a [k v]] (assoc a k (f v)))
          {}
          amap))

(defn challenger-nationality [table-row]
  (get-in table-row ["Challenger" :content 0 :content 0 :attrs :title]))

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


(defn process-row-map! [conx table-row-map]
  (let [text-map (modify-map-values table-row-map element-text)
        episode-id (get text-map "Episode #")
        challenger-nationality (challenger-nationality table-row-map)
        iron-chef-id (get-or-create-chef-id! conx (get text-map "Iron Chef") nil)
        challenger-id (get-or-create-chef-id!
                       conx
                       (remove-bracket-text (get text-map "Challenger"))
                       (get text-map "Challenger Specialty")
                       challenger-nationality)
        winner-id (:chefs/id (get-chef-by-name conx (get text-map "Winner")))]
    
    (jdbc/execute-one! conx ["insert into episodes(id, air_date, theme_ingredient) values(?, ?, ?)"
                             episode-id
                             (get text-map "Original airdate")
                             (get text-map "Theme Ingredient")])

    (jdbc/execute-one! conx ["insert into iron_chefs_episodes(iron_chef_id, episode_id) values (?, ?)" iron-chef-id episode-id])
    (jdbc/execute-one! conx ["insert into challengers_episodes(challenger_id, episode_id) values (?, ?)" challenger-id episode-id])
    (jdbc/execute-one! conx ["insert into winners_episodes(winner_id, episode_id) values (?, ?)" winner-id episode-id])))

(defn process-table! [conx html-table]
  (doseq [row (table-to-maps html-table)]
    (process-row-map! conx row)))

(defn execute! []
  (let [ds (jdbc/get-datasource {:dbtype "sqlite" :dbname "index.sqlite"})
        html-tables (get-tables (parse-html-file))]
    (with-open [conx (jdbc/get-connection ds)]
      (bootstrap-iron-chefs! conx)
      (process-table! conx (first html-tables))
      (process-table! conx (second html-tables)))))
