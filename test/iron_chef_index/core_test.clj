(ns iron-chef-index.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [next.jdbc :as jdbc]
            [iron-chef-index.core :refer :all]))

(def ds (jdbc/get-datasource {:dbtype "sqlite" :dbname "unit-tests.sqlite"}))

(deftest chef-test
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (testing "start out with 0 chefs"
      (is (empty? (get-all-chefs conx))))
    (testing "chef creation"
      (create-chef! conx "Yutaka Ishinabe"    "French")
      (create-chef! conx "Rokusaburo Michiba" "Japanese")
      (is (= 2 (count (get-all-chefs conx)))))
    (testing "chef creation with nationality"
      (create-chef! conx "Paolo Indragoli" "Italian" "Italy")
      (is (= "Italy" (:chefs/nationality (last (get-all-chefs conx))))))
    (testing "chef retrieval works correctly"
      (is (= "Italy" (:chefs/nationality (get-chef-by-name conx "Paolo Indragoli")))))
    (testing "chefs that already exist are not recreated"
      (is (= (:chefs/id (get-chef-by-name conx "Yutaka Ishinabe"))
             (get-or-create-chef-id! conx "Yutaka Ishinabe" "French"))))
    (testing "chefs are created when absent"
      (is (= (get-or-create-chef-id! conx "Joel Gluth" "French")
             (:chefs/id (get-chef-by-name conx "Joel Gluth")))))

    (testing "name fixup works"
      (is (= (get-or-create-chef-id! conx "Kōji Kobayashi" "French")
             (:chefs/id (get-chef-by-name conx "Koji Kobayashi")))))))


(deftest element-text-test

  (testing "Element text is extracted for plain text elements"
    (is (= "5" (element-text {:type :element, :attrs {:align "center"}, :tag :td, :content ["5\n"]}))))

  (testing "Element text is extracted by ignoring superscripts"
    (is (= "6"
           (element-text {:type :element,
                          :attrs {:align "center"},
                          :tag :td,
                          :content
                          ["6"
                           {:type :element,
                            :attrs
                            {:class "reference plainlinks nourlexpansion", :id "ref_2"},
                            :tag :sup,
                            :content
                            [{:type :element,
                              :attrs
                              {:class "external autonumber",
                               :href
                               "https://en.wikipedia.org/wiki/List_of_Iron_Chef_episodes#endnote_2"},
                              :tag :a,
                              :content ["[2]"]}]}
                           "\n"]}))))
  
  (testing "Text surrounded by anchors is returned correctly"
    (is (= "Chen Kenichi"
           (element-text
            {:type :element,
             :attrs {:bgcolor "lightblue"},
             :tag :td,
             :content
             [{:type :element,
               :attrs
               {:href "https://en.wikipedia.org/wiki/Chen_Kenichi",
                :title "Chen Kenichi"},
               :tag :a,
               :content ["Chen Kenichi"]}
              "\n"]}))))
  (testing "Text in braces is ignored"
    (is (= "Eizō Ōyama"
           (element-text
            {:type :element,
             :attrs {:bgcolor "silver"},
             :tag :td,
             :content ["Eizō Ōyama (大山栄蔵)\n"]})))))


;; (modify-map-values (nth table-maps 2) element-text)
(comment
  {"Episode #" "3",
   "Original airdate" "October 31, 1993",
   "Iron Chef" "Chen Kenichi",
   "Challenger" "Paolo Indragoli",
   "Challenger Specialty" "Italian",
   "Theme Ingredient" "Globefish",
   "Winner" "Chen Kenichi"})

(deftest modify-map-values-test
  (testing "empty map is returned unchanged"
    (is (= {} (modify-map-values {} inc))))
  (testing "map with one value is returned correctly"
    (is (= {:a 2} (modify-map-values {:a 1} inc)))))

(deftest zipmulti-test
  (testing "empty key and value seqs return an empty map"
    (is (= {} (zipmulti [] []))))
  (testing "a single key and value are mapped together"
    (is (= {:a [1]} (zipmulti [:a] [1]))))
  (testing "sundry values are ignored"
    (is (= {:a [1]} (zipmulti [:a] [1 2]))))
  (testing "repeated keys put their values in the same vector"
    (is (= {:a [1 3] :b [2]} (zipmulti [:a :b :a] [1 2 3])))))

(deftest html-table-test

  (let [html-doc (parse-html-file)]
    (testing "HTML tables are correctly found in the wiki page"
      (is (= 16 (count (get-tables html-doc)))))
    (testing "Table headers are parsed for labeling"
      (is (= '("Episode #"
               "Original airdate"
               "Iron Chef"
               "Challenger"
               "Challenger Specialty"
               "Theme Ingredient"
               "Winner")
             (get-headers
              (first (get-tables html-doc))))))
    (testing "tables are converted into lists of ordered maps with the headers as keys and HTML elements as values"
      (let [table-maps (table-to-maps (first (get-tables html-doc)))]
        (is (= 10 (count table-maps)))
        (is (= "5" (element-text (first (get (nth table-maps 4) "Episode #")))))
        (comment (is (= "Rokusaburo Michiba"
                        (element-text (get (nth table-maps 5) "Iron Chef")))))))))

(deftest challenger-nationality-test
  (let [html-doc (parse-html-file)
        table-maps (table-to-maps (first (get-tables html-doc)))
        paolo-row (nth table-maps 2)
        toshi-row (nth table-maps 4)
        jacques-row (nth table-maps 9)]
    (testing "Unspecified nationalities are nil"
      (is (nil? (challenger-nationality toshi-row))))
    (testing "Italian nationality is correctly deduced in season 1"
      (is (= "Italy" (challenger-nationality paolo-row))))
    (testing "French nationality is correctly deduced in season 1"
      (is (= "France" (challenger-nationality jacques-row))))))

;; this test written as penance because it actually wasn't working, load-bearing typo, and I hadn't noticed
(deftest episodes-test
  (let [html-doc (parse-html-file)
        table-maps (table-to-maps (second (get-tables html-doc)))
        joel-and-masashi-row (nth table-maps 27)]

    (testing "French nationality is correctly deduced fof the challenger pair in season 1"
      (is (= "France" (challenger-nationality joel-and-masashi-row))))

    (jdbc/with-transaction [conx ds {:rollback-only true}]

      (process-row-map! conx joel-and-masashi-row)
      
      (testing "Iron chef retrieval"
        (is (= 2 (count (get-all-iron-chefs conx))) "Iron chefs should be retrieved"))
      (testing "Challenger retrieval"
        (is (= 2 (count (get-all-challengers conx))) "Challengers should be retrieved")))
    ))

(deftest spanning-rows-test
  (let [html-doc (parse-html-file)
        table (nth (get-tables html-doc) 2)
        headers (get-headers table)
        rows (get-rows table)
        combined-row (apply (partial spanned-rows-to-map headers)
                            (map get-column-fields (rest rows)))]
    
    (is (= ["60"] (get combined-row "Episode #")))
    (is (= 4 (count (get combined-row "Challenger")))
        "All 4 challengers (2 per row) should get collected")))

(deftest annoying-one-off-table-format-test
  (testing "Episodes with multiple challenger columns are processed correctly"
    (let [html-doc (parse-html-file)
          table-maps (table-to-maps (first (get-tables html-doc)))
          table (nth (get-tables html-doc) 2)]
          
      (comment (pprint table))
      
      (jdbc/with-transaction [conx ds {:rollback-only true}]
            
        ;; two identical Challenger <th/> elements, multiple row-spanning columns meaning you can't simply process one row at a time
        (process-stupid-table! conx table)
        
        (is (= 0 (count (get-all-iron-chefs conx))) "there are no iron chefs in this episode, only challengers")
        (is (= 4 (count (get-all-challengers conx))) "4 challengers should have been created")
        ;; TODO
        (comment "Dont' know if I care about there being two bouts in the same episode. TODO maybe.")))))

(deftest execute-test
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (testing "After execution, the correct number of episodes is created"
      (is (= 291 ;; all episodes through 1999
             (count (get-all-episodes conx)))))
    (testing "Specific episodes from TODO file are parsed"
      ;; Test that episodes mentioned in TODO file are properly handled
      (let [episodes (get-all-episodes conx)]
        (is (some #(= (:episodes/id %) 149) episodes) "Episode 149 should be parsed")
        (is (some #(= (:episodes/id %) 160) episodes) "Episode 160 should be parsed")
        (is (some #(= (:episodes/id %) 190) episodes) "Episode 190 should be parsed")
        (is (some #(= (:episodes/id %) 239) episodes) "Episode 239 should be parsed")
        ;; Also test that the specific chefs from special episodes are created
        (is (some #(= (:chefs/name %) "Bernard Leprince") (get-all-chefs conx)) "Bernard Leprince should be parsed")
        (is (some #(= (:chefs/name %) "Lin Kunbi") (get-all-chefs conx)) "Lin Kunbi should be parsed")
        (is (some #(= (:chefs/name %) "Pierre Gagnaire") (get-all-chefs conx)) "Pierre Gagnaire should be parsed")
        (is (some #(= (:chefs/name %) "Gianfranco Vissani") (get-all-chefs conx)) "Gianfranco Vissani should be parsed")
        (is (some #(= (:chefs/name %) "Hsu Cheng") (get-all-chefs conx)) "Hsu Cheng should be parsed")
        (is (some #(= (:chefs/name %) "Leung Waikei") (get-all-chefs conx)) "Leung Waikei should be parsed")
        (is (some #(= (:chefs/name %) "Chow Chung") (get-all-chefs conx)) "Chow Chung should be parsed")
        (is (some #(= (:chefs/name %) "Toshirō Kandagawa") (get-all-chefs conx)) "Toshirō Kandagawa should be parsed")))
    (comment
      (testing "After execution, the correct number of chefs are created "
        (is (= 63 (count (get-all-chefs conx)))))

      (testing "The right number of iron chefs should be allocated during the series"
        (is (= 4 (count (get-all-iron-chefs conx)))))
      (testing "The right number of challengers should be allocated during the series"
        (is (= 59 (count (get-all-challengers conx))))))))

;; Helper function to get episode by ID
(defn get-episode-by-id [conx id]
  (jdbc/execute-one! conx ["select * from episodes where id = ?" id]))

;; Helper to check if an episode's air date falls in a given year
(defn episode-in-year? [episode year]
  (when-let [air-date (:episodes/air_date episode)]
    (.contains air-date (str year))))

(deftest all-episodes-through-1999-test
  "Verify all episodes from 1 through 291 are parsed and inserted into the database"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [episodes (get-all-episodes conx)
          episode-ids (set (map :episodes/id episodes))]

      (testing "No gaps in episode numbering from 1 to 291"
        (doseq [ep-num (range 1 292)]
          (is (contains? episode-ids ep-num)
              (str "Episode " ep-num " should exist"))))

      (testing "No episodes beyond 291 exist"
        (is (empty? (filter #(> (:episodes/id %) 291) episodes))
            "No episodes above 291 should exist")))))

(deftest episodes-by-year-test
  "Verify episode counts by year through 1999"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [episodes (get-all-episodes conx)]

      (testing "1993 episodes (1-10)"
        (let [eps-1993 (filter #(episode-in-year? % "1993") episodes)]
          (is (>= (count eps-1993) 10)
              "At least 10 episodes should be from 1993")))

      (testing "1994 episodes"
        (let [eps-1994 (filter #(episode-in-year? % "1994") episodes)]
          (is (> (count eps-1994) 0)
              "There should be episodes from 1994")))

      (testing "1995 episodes include special episodes 61, 73, 99, 101, 102, 110, 111"
        (doseq [ep-id [61 73 99 101 102 110 111]]
          (is (get-episode-by-id conx ep-id)
              (str "Episode " ep-id " (1995 special) should exist"))))

      (testing "1996 episodes include special episodes 124, 149, 160"
        (doseq [ep-id [124 149 160]]
          (is (get-episode-by-id conx ep-id)
              (str "Episode " ep-id " (1996 special) should exist"))))

      (testing "1997 episodes include special episodes 163, 164, 190, 193, 194, 198"
        (doseq [ep-id [163 164 190 193 194 198]]
          (is (get-episode-by-id conx ep-id)
              (str "Episode " ep-id " (1997 special) should exist"))))

      (testing "1998 episodes (209-256)"
        (doseq [ep-id (range 209 257)]
          (is (get-episode-by-id conx ep-id)
              (str "Episode " ep-id " (1998) should exist"))))

      (testing "1999 episodes (257-291)"
        (doseq [ep-id (range 257 292)]
          (is (get-episode-by-id conx ep-id)
              (str "Episode " ep-id " (1999) should exist")))))))

(deftest episode-air-dates-test
  "Verify key episodes have correct air dates"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "First episode air date"
      (let [ep1 (get-episode-by-id conx 1)]
        (is (= "October 10, 1993" (:episodes/air_date ep1))
            "Episode 1 should air on October 10, 1993")))

    (testing "Episode 61 (1995 New Year special)"
      (let [ep61 (get-episode-by-id conx 61)]
        (is (= "January 2, 1995" (:episodes/air_date ep61)))))

    (testing "Episode 160 (1996 New Year's Eve special)"
      (let [ep160 (get-episode-by-id conx 160)]
        (is (= "December 31, 1996" (:episodes/air_date ep160)))))

    (testing "Episode 198 (1997 3-battle special)"
      (let [ep198 (get-episode-by-id conx 198)]
        (is (= "September 12, 1997" (:episodes/air_date ep198)))))))

(deftest episode-battles-test
  "Verify episodes have battles created"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "Regular episodes have at least one battle"
      ;; Check a sampling of regular episodes
      (doseq [ep-id [1 10 50 100 150 200 250 290]]
        (let [battles (jdbc/execute! conx ["select * from battles where episode_id = ?" ep-id])]
          (is (>= (count battles) 1)
              (str "Episode " ep-id " should have at least one battle")))))

    (testing "Episode 198 has 3 battles"
      (let [battles (jdbc/execute! conx ["select * from battles where episode_id = 198 order by battle_number"])]
        (is (= 3 (count battles))
            "Episode 198 should have 3 battles")
        (is (= ["Beef" "Lobster" "Foie gras"]
               (map :battles/theme_ingredient battles))
            "Episode 198 battles should have correct theme ingredients")))))
