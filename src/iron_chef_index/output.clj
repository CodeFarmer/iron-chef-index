(ns iron-chef-index.output
  (:require [clojure.string :as s]
            [next.jdbc :as jdbc]))

(def ^:private seasons
  "Season boundaries as [label first-ep last-ep]."
  [["Season 1" 1 10]
   ["Season 2" 11 60]
   ["Season 3" 61 110]
   ["Season 4" 111 162]
   ["Season 5" 163 210]
   ["Season 6" 211 291]
   ["Specials" 292 295]])

(defn- escape-html [text]
  (-> (str text)
      (s/replace "&" "&amp;")
      (s/replace "<" "&lt;")
      (s/replace ">" "&gt;")
      (s/replace "\"" "&quot;")))

(defn- parse-names
  "Split a comma-separated name string into a set of trimmed names."
  [s]
  (if s
    (->> (s/split s #",")
         (map s/trim)
         (remove empty?)
         set)
    #{}))

(defn- classify-result
  "Classify a battle result as :iron-chef-win, :challenger-win, or :other."
  [iron-chefs challengers winners]
  (let [ic-names (parse-names iron-chefs)
        ch-names (parse-names challengers)
        win-names (parse-names winners)]
    (cond
      (empty? win-names) :other
      (and (seq ic-names) (every? ic-names win-names)) :iron-chef-win
      (and (seq ch-names) (every? ch-names win-names)) :challenger-win
      :else :other)))

(defn- query-battles
  "Query all battles with episode info, chef names, ordered by episode and battle number."
  [conx]
  (jdbc/execute! conx
    ["SELECT e.id AS episode_id, e.air_date,
             b.battle_number, b.theme_ingredient,
             (SELECT group_concat(c.name, ', ')
              FROM iron_chefs_battles icb
              JOIN chefs c ON c.id = icb.iron_chef_id
              WHERE icb.battle_id = b.id) AS iron_chefs,
             (SELECT group_concat(c.name, ', ')
              FROM challengers_battles cb
              JOIN chefs c ON c.id = cb.challenger_id
              WHERE cb.battle_id = b.id) AS challengers,
             (SELECT group_concat(c.name, ', ')
              FROM winners_battles wb
              JOIN chefs c ON c.id = wb.winner_id
              WHERE wb.battle_id = b.id) AS winners
      FROM episodes e
      JOIN battles b ON b.episode_id = e.id
      ORDER BY e.id, b.battle_number"]))

(defn- query-movie-files
  "Query all movie files, returning a map of episode_id to file records."
  [conx]
  (->> (jdbc/execute! conx ["SELECT path, episode_id, audio FROM movie_files ORDER BY episode_id, path"])
       (group-by :movie_files/episode_id)))

(defn- render-file-links [files]
  (when (seq files)
    (s/join " "
      (map (fn [f]
             (let [path (:movie_files/path f)
                   audio (:movie_files/audio f)]
               (str "<a href=\"" (escape-html path) "\">"
                    (escape-html path) " (" (escape-html audio) ")</a>")))
           files))))

(defn- result-class
  "Return the CSS class for a battle result."
  [result]
  (case result
    :iron-chef-win "iron-chef-win"
    :challenger-win "challenger-win"
    "other-result"))

(defn- render-battle-row
  "Render a <tr> for a single battle. When first-battle? is true, emits the
   episode-level cells (ep #, air date, video) with rowspan if needed."
  [battle num-battles files first-battle?]
  (let [result (classify-result (:iron_chefs battle) (:challengers battle) (:winners battle))
        winner-class (result-class result)]
    (str "  <tr>\n"
         (when first-battle?
           (let [rs (when (> num-battles 1) (str " rowspan=\"" num-battles "\""))]
             (str "    <td" rs ">" (:episodes/episode_id battle) "</td>\n"
                  "    <td" rs ">" (escape-html (:episodes/air_date battle)) "</td>\n")))
         "    <td>" (escape-html (:battles/theme_ingredient battle)) "</td>\n"
         "    <td>" (escape-html (or (:iron_chefs battle) "")) "</td>\n"
         "    <td>" (escape-html (or (:challengers battle) "")) "</td>\n"
         "    <td class=\"" winner-class "\">" (escape-html (or (:winners battle) "")) "</td>\n"
         (when first-battle?
           (let [rs (when (> num-battles 1) (str " rowspan=\"" num-battles "\""))]
             (str "    <td" rs ">" (or (render-file-links files) "") "</td>\n")))
         "  </tr>\n")))

(defn- render-episode [battles files]
  (let [n (count battles)]
    (str (render-battle-row (first battles) n files true)
         (apply str (map #(render-battle-row % n nil false) (rest battles))))))

(def ^:private table-header
  (str "  <tr>\n"
       "    <th>Ep #</th>\n"
       "    <th>Air Date</th>\n"
       "    <th>Theme Ingredient</th>\n"
       "    <th>Iron Chef</th>\n"
       "    <th>Challenger</th>\n"
       "    <th>Winner</th>\n"
       "    <th>Video</th>\n"
       "  </tr>\n"))

(defn- render-season [label ep-groups files-by-ep]
  (str "<h2>" (escape-html label) "</h2>\n"
       "<table>\n"
       table-header
       (apply str
         (map (fn [ep-battles]
                (let [ep-id (:episodes/episode_id (first ep-battles))
                      files (get files-by-ep ep-id [])]
                  (render-episode ep-battles files)))
              ep-groups))
       "</table>\n"))

(defn generate-html
  "Generate a complete HTML index page from the database, grouped by season."
  [conx]
  (let [all-battles (query-battles conx)
        files-by-ep (query-movie-files conx)
        episodes-by-id (partition-by :episodes/episode_id all-battles)
        ep-id-map (into {} (map (fn [bs] [(:episodes/episode_id (first bs)) bs]) episodes-by-id))]
    (str
      "<!DOCTYPE html>\n"
      "<html lang=\"en\">\n"
      "<head>\n"
      "  <meta charset=\"UTF-8\">\n"
      "  <title>Iron Chef Episode Index</title>\n"
      "  <style>\n"
      "    body { font-family: sans-serif; margin: 2em; }\n"
      "    table { border-collapse: collapse; width: 100%; margin-bottom: 2em; }\n"
      "    th, td { border: 1px solid #ccc; padding: 0.4em 0.6em; text-align: left; vertical-align: top; }\n"
      "    th { background: #333; color: #fff; }\n"
      "    tr:nth-child(even) { background: #f6f6f6; }\n"
      "    a { color: #06c; }\n"
      "    .iron-chef-win { background: #ffd700; }\n"
      "    .challenger-win { background: #90ee90; }\n"
      "    .other-result { background: #d3d3d3; }\n"
      "  </style>\n"
      "</head>\n"
      "<body>\n"
      "<h1>Iron Chef Episode Index</h1>\n"
      (apply str
        (map (fn [[label first-ep last-ep]]
               (let [ep-groups (keep #(get ep-id-map %) (range first-ep (inc last-ep)))]
                 (when (seq ep-groups)
                   (render-season label ep-groups files-by-ep))))
             seasons))
      "</body>\n"
      "</html>\n")))

(defn write-index!
  "Generate the HTML index and write it to the given path."
  [conx output-path]
  (spit output-path (generate-html conx))
  (println "Wrote index to" output-path))
