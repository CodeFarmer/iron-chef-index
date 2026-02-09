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
      (is (= 295 ;; episodes 1-291 (through 1999) + 4 special episodes (292-295) in 2000-2002
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

      (testing "2000-2002 special episodes exist (292-295)"
        (doseq [ep-num (range 292 296)]
          (is (contains? episode-ids ep-num)
              (str "Special episode " ep-num " should exist"))))

      (testing "No episodes beyond 295 exist"
        (is (empty? (filter #(> (:episodes/id %) 295) episodes))
            "No episodes above 295 should exist")))))

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
        (is (= "October 10, 1997" (:episodes/air_date ep198)))))))

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

(deftest multi-battle-episodes-test
  "Verify episodes with multiple battles have correct battle counts and ingredients"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "Episode 61 (1995 New Year Special) has 2 battles"
      (let [battles (jdbc/execute! conx ["select * from battles where episode_id = 61 order by battle_number"])]
        (is (= 2 (count battles))
            "Episode 61 should have 2 battles")
        (is (= ["Abalone" "Yellowtail"]
               (map :battles/theme_ingredient battles))
            "Episode 61 battles should have correct theme ingredients")))

    (testing "Episode 73 (Hong Kong Special) has 2 battles"
      (let [battles (jdbc/execute! conx ["select * from battles where episode_id = 73 order by battle_number"])]
        (is (= 2 (count battles))
            "Episode 73 should have 2 battles")
        (is (= ["Pork" "Spiny lobster"]
               (map :battles/theme_ingredient battles))
            "Episode 73 battles should have correct theme ingredients")))

    (testing "Episode 99 (World Cup Special) has 3 battles"
      (let [battles (jdbc/execute! conx ["select * from battles where episode_id = 99 order by battle_number"])]
        (is (= 3 (count battles))
            "Episode 99 should have 3 battles")
        (is (= ["Tuna" "Squid" "Duck"]
               (map :battles/theme_ingredient battles))
            "Episode 99 battles should have correct theme ingredients")))

    (testing "Episode 124 (France vs Japan Special) has 2 battles"
      (let [battles (jdbc/execute! conx ["select * from battles where episode_id = 124 order by battle_number"])]
        (is (= 2 (count battles))
            "Episode 124 should have 2 battles")
        (is (= ["Salmon" "Lobster"]
               (map :battles/theme_ingredient battles))
            "Episode 124 battles should have correct theme ingredients")))

    (testing "Episode 149 (China vs Japan Special) has 2 battles"
      (let [battles (jdbc/execute! conx ["select * from battles where episode_id = 149 order by battle_number"])]
        (is (= 2 (count battles))
            "Episode 149 should have 2 battles")
        (is (= ["Chicken" "Shark fin"]
               (map :battles/theme_ingredient battles))
            "Episode 149 battles should have correct theme ingredients")))

    (testing "Episode 198 (1997 3-battle special) has 3 battles"
      (let [battles (jdbc/execute! conx ["select * from battles where episode_id = 198 order by battle_number"])]
        (is (= 3 (count battles))
            "Episode 198 should have 3 battles")
        (is (= ["Beef" "Lobster" "Foie gras"]
               (map :battles/theme_ingredient battles))
            "Episode 198 battles should have correct theme ingredients")))))

;; Helper to get all battles for an episode
(defn get-battles-for-episode [conx episode-id]
  (jdbc/execute! conx ["select * from battles where episode_id = ? order by battle_number" episode-id]))

;; Helper to get iron chefs for a battle
(defn get-iron-chefs-for-battle [conx battle-id]
  (jdbc/execute! conx ["select c.* from chefs c
                        join iron_chefs_battles icb on c.id = icb.iron_chef_id
                        where icb.battle_id = ?" battle-id]))

;; Helper to get challengers for a battle
(defn get-challengers-for-battle [conx battle-id]
  (jdbc/execute! conx ["select c.* from chefs c
                        join challengers_battles cb on c.id = cb.challenger_id
                        where cb.battle_id = ?" battle-id]))

;; Helper to get winners for a battle
(defn get-winners-for-battle [conx battle-id]
  (jdbc/execute! conx ["select c.* from chefs c
                        join winners_battles wb on c.id = wb.winner_id
                        where wb.battle_id = ?" battle-id]))

(deftest episode-99-participants-test
  "Verify Episode 99 (1995 World Cup Special) has correct participants per Wikipedia:
   Battle 1: Gagnaire vs Vissani (Tuna) - Vissani wins (challenger vs challenger semifinal)
   Battle 2: Michiba vs Hsu Cheng (Squid) - Michiba wins (semifinal)
   Battle 3: Michiba vs Vissani (Duck) - Michiba wins (final)"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [battles (get-battles-for-episode conx 99)
          battle1 (first battles)
          battle2 (second battles)
          battle3 (nth battles 2)]

      (testing "Episode 99 Battle 1: Gagnaire vs Vissani (Tuna)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle1))
              challengers (get-challengers-for-battle conx (:battles/id battle1))
              winners (get-winners-for-battle conx (:battles/id battle1))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Tuna" (:battles/theme_ingredient battle1))
              "Battle 1 theme should be Tuna")
          (is (empty? iron-chefs)
              "Battle 1 should have no Iron Chef (challenger vs challenger)")
          (is (= #{"Pierre Gagnaire" "Gianfranco Vissani"} challenger-names)
              "Battle 1 challengers should be Gagnaire and Vissani")
          (is (= #{"Gianfranco Vissani"} winner-names)
              "Battle 1 winner should be Vissani")))

      (testing "Episode 99 Battle 2: Michiba vs Hsu Cheng (Squid)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle2))
              challengers (get-challengers-for-battle conx (:battles/id battle2))
              winners (get-winners-for-battle conx (:battles/id battle2))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Squid" (:battles/theme_ingredient battle2))
              "Battle 2 theme should be Squid")
          (is (= #{"Rokusaburo Michiba"} iron-chef-names)
              "Battle 2 Iron Chef should be Michiba")
          (is (= #{"Hsu Cheng"} challenger-names)
              "Battle 2 challenger should be Hsu Cheng")
          (is (= #{"Rokusaburo Michiba"} winner-names)
              "Battle 2 winner should be Michiba")))

      (testing "Episode 99 Battle 3: Michiba vs Vissani (Duck)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle3))
              challengers (get-challengers-for-battle conx (:battles/id battle3))
              winners (get-winners-for-battle conx (:battles/id battle3))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Duck" (:battles/theme_ingredient battle3))
              "Battle 3 theme should be Duck")
          (is (= #{"Rokusaburo Michiba"} iron-chef-names)
              "Battle 3 Iron Chef should be Michiba")
          (is (= #{"Gianfranco Vissani"} challenger-names)
              "Battle 3 challenger should be Vissani")
          (is (= #{"Rokusaburo Michiba"} winner-names)
              "Battle 3 winner should be Michiba"))))))

(deftest episode-61-participants-test
  "Verify Episode 61 (1995 New Year Special - Mr. Iron Chef) has correct participants per Wikipedia:
   Battle 1: Kandagawa vs Shimizu (Abalone) - Kandagawa wins (challenger vs challenger, preliminaries)
   Battle 2: Michiba vs Kandagawa (Yellowtail) - Michiba wins (finals)"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [battles (get-battles-for-episode conx 61)
          battle1 (first battles)
          battle2 (second battles)]

      (testing "Episode 61 Battle 1: Kandagawa vs Shimizu (Abalone)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle1))
              challengers (get-challengers-for-battle conx (:battles/id battle1))
              winners (get-winners-for-battle conx (:battles/id battle1))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Abalone" (:battles/theme_ingredient battle1))
              "Battle 1 theme should be Abalone")
          (is (empty? iron-chefs)
              "Battle 1 should have no Iron Chef (challenger vs challenger)")
          (is (= #{"Toshirō Kandagawa" "Tadaaki Shimizu"} challenger-names)
              "Battle 1 challengers should be Kandagawa and Shimizu")
          (is (= #{"Toshirō Kandagawa"} winner-names)
              "Battle 1 winner should be Kandagawa")))

      (testing "Episode 61 Battle 2: Michiba vs Kandagawa (Yellowtail)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle2))
              challengers (get-challengers-for-battle conx (:battles/id battle2))
              winners (get-winners-for-battle conx (:battles/id battle2))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Yellowtail" (:battles/theme_ingredient battle2))
              "Battle 2 theme should be Yellowtail")
          (is (= #{"Rokusaburo Michiba"} iron-chef-names)
              "Battle 2 Iron Chef should be Michiba")
          (is (= #{"Toshirō Kandagawa"} challenger-names)
              "Battle 2 challenger should be Kandagawa")
          (is (= #{"Rokusaburo Michiba"} winner-names)
              "Battle 2 winner should be Michiba"))))))

(deftest episode-73-participants-test
  "Verify Episode 73 (Hong Kong Special) has correct participants per Wikipedia:
   Battle 1: Chen vs Leung Waikei (Pork) - Chen wins
   Battle 2: Michiba vs Chow Chung (Spiny lobster) - Michiba wins"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [battles (get-battles-for-episode conx 73)
          battle1 (first battles)
          battle2 (second battles)]

      (testing "Episode 73 Battle 1: Chen vs Leung Waikei (Pork)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle1))
              challengers (get-challengers-for-battle conx (:battles/id battle1))
              winners (get-winners-for-battle conx (:battles/id battle1))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Pork" (:battles/theme_ingredient battle1))
              "Battle 1 theme should be Pork")
          (is (= #{"Chen Kenichi"} iron-chef-names)
              "Battle 1 Iron Chef should be Chen")
          (is (= #{"Leung Waikei"} challenger-names)
              "Battle 1 challenger should be Leung Waikei")
          (is (= #{"Chen Kenichi"} winner-names)
              "Battle 1 winner should be Chen")))

      (testing "Episode 73 Battle 2: Michiba vs Chow Chung (Spiny lobster)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle2))
              challengers (get-challengers-for-battle conx (:battles/id battle2))
              winners (get-winners-for-battle conx (:battles/id battle2))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Spiny lobster" (:battles/theme_ingredient battle2))
              "Battle 2 theme should be Spiny lobster")
          (is (= #{"Rokusaburo Michiba"} iron-chef-names)
              "Battle 2 Iron Chef should be Michiba")
          (is (= #{"Chow Chung"} challenger-names)
              "Battle 2 challenger should be Chow Chung")
          (is (= #{"Rokusaburo Michiba"} winner-names)
              "Battle 2 winner should be Michiba"))))))

(deftest episode-124-participants-test
  "Verify Episode 124 (France Special at Château de Brissac) has correct participants per Wikipedia:
   Battle 1: Nakamura vs Bernard Leprince (Salmon) - Leprince wins
   Battle 2: Sakai vs Pierre Gagnaire (Lobster) - Gagnaire wins"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [battles (get-battles-for-episode conx 124)
          battle1 (first battles)
          battle2 (second battles)]

      (testing "Episode 124 Battle 1: Nakamura vs Leprince (Salmon)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle1))
              challengers (get-challengers-for-battle conx (:battles/id battle1))
              winners (get-winners-for-battle conx (:battles/id battle1))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Salmon" (:battles/theme_ingredient battle1))
              "Battle 1 theme should be Salmon")
          (is (= #{"Komei Nakamura"} iron-chef-names)
              "Battle 1 Iron Chef should be Nakamura")
          (is (= #{"Bernard Leprince"} challenger-names)
              "Battle 1 challenger should be Leprince")
          (is (= #{"Bernard Leprince"} winner-names)
              "Battle 1 winner should be Leprince")))

      (testing "Episode 124 Battle 2: Sakai vs Gagnaire (Lobster)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle2))
              challengers (get-challengers-for-battle conx (:battles/id battle2))
              winners (get-winners-for-battle conx (:battles/id battle2))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Lobster" (:battles/theme_ingredient battle2))
              "Battle 2 theme should be Lobster")
          (is (= #{"Hiroyuki Sakai"} iron-chef-names)
              "Battle 2 Iron Chef should be Sakai")
          (is (= #{"Pierre Gagnaire"} challenger-names)
              "Battle 2 challenger should be Gagnaire")
          (is (= #{"Pierre Gagnaire"} winner-names)
              "Battle 2 winner should be Gagnaire"))))))

(deftest episode-149-participants-test
  "Verify Episode 149 (China vs Japan Special) has correct participants per Wikipedia:
   Battle 1: Chen vs Sun Liping, Su Dexing, Zhuang Weijia (Chicken) - Chen AND Sun Liping win (tie)
   Battle 2: Chen vs Sun Liping (Shark fin) - Chen wins (tiebreaker)"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [battles (get-battles-for-episode conx 149)
          battle1 (first battles)
          battle2 (second battles)]

      (testing "Episode 149 Battle 1: Chen vs Team China (Chicken) - Tie"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle1))
              challengers (get-challengers-for-battle conx (:battles/id battle1))
              winners (get-winners-for-battle conx (:battles/id battle1))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Chicken" (:battles/theme_ingredient battle1))
              "Battle 1 theme should be Chicken")
          (is (= #{"Chen Kenichi"} iron-chef-names)
              "Battle 1 Iron Chef should be Chen")
          (is (= #{"Sun Liping" "Su Dexing" "Zhuang Weijia"} challenger-names)
              "Battle 1 challengers should be Sun Liping, Su Dexing, and Zhuang Weijia")
          (is (= #{"Chen Kenichi" "Sun Liping"} winner-names)
              "Battle 1 winners should be Chen and Sun Liping (tie)")))

      (testing "Episode 149 Battle 2: Chen vs Sun Liping (Shark fin) - Chen wins"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle2))
              challengers (get-challengers-for-battle conx (:battles/id battle2))
              winners (get-winners-for-battle conx (:battles/id battle2))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Shark fin" (:battles/theme_ingredient battle2))
              "Battle 2 theme should be Shark fin")
          (is (= #{"Chen Kenichi"} iron-chef-names)
              "Battle 2 Iron Chef should be Chen")
          (is (= #{"Sun Liping"} challenger-names)
              "Battle 2 challenger should be Sun Liping")
          (is (= #{"Chen Kenichi"} winner-names)
              "Battle 2 winner should be Chen only (not a tie)"))))))

(deftest episode-198-participants-test
  "Verify Episode 198 (1997 Iron Chef World Cup) has correct participants per Wikipedia:
   Air date: October 10, 1997
   Battle 1: Nakamura vs Liu Xikun (Beef) - Nakamura wins
   Battle 2: Passard vs Patrick Clark (Lobster) - Passard wins (challenger vs challenger)
   Battle 3: Nakamura vs Passard (Foie gras) - Draw (no winner)"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "Episode 198 air date"
      (let [episode (get-episode-by-id conx 198)]
        (is (= "October 10, 1997" (:episodes/air_date episode))
            "Episode 198 should air on October 10, 1997")))

    (testing "Patrick Clark chef exists with correct name"
      (let [chef (get-chef-by-name conx "Patrick Clark")]
        (is (some? chef)
            "Patrick Clark should exist (not Don Clark)")))

    (let [battles (get-battles-for-episode conx 198)
          battle1 (first battles)
          battle2 (second battles)
          battle3 (nth battles 2)]

      (testing "Episode 198 Battle 1: Nakamura vs Liu Xikun (Beef)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle1))
              challengers (get-challengers-for-battle conx (:battles/id battle1))
              winners (get-winners-for-battle conx (:battles/id battle1))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Beef" (:battles/theme_ingredient battle1))
              "Battle 1 theme should be Beef")
          (is (= #{"Komei Nakamura"} iron-chef-names)
              "Battle 1 Iron Chef should be Nakamura")
          (is (= #{"Liu Xikun"} challenger-names)
              "Battle 1 challenger should be Liu Xikun")
          (is (= #{"Komei Nakamura"} winner-names)
              "Battle 1 winner should be Nakamura")))

      (testing "Episode 198 Battle 2: Passard vs Patrick Clark (Lobster)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle2))
              challengers (get-challengers-for-battle conx (:battles/id battle2))
              winners (get-winners-for-battle conx (:battles/id battle2))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Lobster" (:battles/theme_ingredient battle2))
              "Battle 2 theme should be Lobster")
          (is (empty? iron-chefs)
              "Battle 2 should have no Iron Chef (challenger vs challenger)")
          (is (= #{"Alain Passard" "Patrick Clark"} challenger-names)
              "Battle 2 challengers should be Passard and Patrick Clark")
          (is (= #{"Alain Passard"} winner-names)
              "Battle 2 winner should be Passard")))

      (testing "Episode 198 Battle 3: Nakamura vs Passard (Foie gras) - Draw"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle3))
              challengers (get-challengers-for-battle conx (:battles/id battle3))
              winners (get-winners-for-battle conx (:battles/id battle3))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))]
          (is (= "Foie gras" (:battles/theme_ingredient battle3))
              "Battle 3 theme should be Foie gras")
          (is (= #{"Komei Nakamura"} iron-chef-names)
              "Battle 3 Iron Chef should be Nakamura")
          (is (= #{"Alain Passard"} challenger-names)
              "Battle 3 challenger should be Passard")
          (is (empty? winners)
              "Battle 3 should have no winner (draw)"))))))

(deftest episode-295-participants-test
  "Verify Episode 295 (Japan Cup 2002) has correct participants per Wikipedia:
   Battle 1: Chen vs Yūichirō Ebisu (King crab) - Chen wins
   Battle 2: Nonaga vs Tanabe (Pacific bluefin tuna) - Nonaga wins (challenger vs challenger)
   Battle 3: Chen vs Nonaga (Ingii chicken) - Nonaga wins"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "Takeshi Tanabe chef has correct specialty"
      (let [chef (get-chef-by-name conx "Takeshi Tanabe")]
        (is (some? chef)
            "Takeshi Tanabe should exist")
        (is (= "French" (:chefs/cuisine chef))
            "Takeshi Tanabe's cuisine should be French")))

    (let [battles (get-battles-for-episode conx 295)
          battle1 (first battles)
          battle2 (second battles)
          battle3 (nth battles 2)]

      (testing "Episode 295 Battle 1: Chen vs Ebisu (King crab)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle1))
              challengers (get-challengers-for-battle conx (:battles/id battle1))
              winners (get-winners-for-battle conx (:battles/id battle1))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "King crab" (:battles/theme_ingredient battle1))
              "Battle 1 theme should be King crab")
          (is (= #{"Chen Kenichi"} iron-chef-names)
              "Battle 1 Iron Chef should be Chen")
          (is (= #{"Yūichirō Ebisu"} challenger-names)
              "Battle 1 challenger should be Ebisu")
          (is (= #{"Chen Kenichi"} winner-names)
              "Battle 1 winner should be Chen")))

      (testing "Episode 295 Battle 2: Nonaga vs Tanabe (Pacific bluefin tuna)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle2))
              challengers (get-challengers-for-battle conx (:battles/id battle2))
              winners (get-winners-for-battle conx (:battles/id battle2))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Pacific bluefin tuna" (:battles/theme_ingredient battle2))
              "Battle 2 theme should be Pacific bluefin tuna")
          (is (empty? iron-chefs)
              "Battle 2 should have no Iron Chef (challenger vs challenger)")
          (is (= #{"Kimio Nonaga" "Takeshi Tanabe"} challenger-names)
              "Battle 2 challengers should be Nonaga and Tanabe")
          (is (= #{"Kimio Nonaga"} winner-names)
              "Battle 2 winner should be Nonaga")))

      (testing "Episode 295 Battle 3: Chen vs Nonaga (Ingii chicken)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle3))
              challengers (get-challengers-for-battle conx (:battles/id battle3))
              winners (get-winners-for-battle conx (:battles/id battle3))
              iron-chef-names (set (map :chefs/name iron-chefs))
              challenger-names (set (map :chefs/name challengers))
              winner-names (set (map :chefs/name winners))]
          (is (= "Ingii chicken" (:battles/theme_ingredient battle3))
              "Battle 3 theme should be Ingii chicken")
          (is (= #{"Chen Kenichi"} iron-chef-names)
              "Battle 3 Iron Chef should be Chen")
          (is (= #{"Kimio Nonaga"} challenger-names)
              "Battle 3 challenger should be Nonaga")
          (is (= #{"Kimio Nonaga"} winner-names)
              "Battle 3 winner should be Nonaga"))))))

(deftest episode-239-participants-test
  "Verify Episode 239 (2,000th Plate Special) has correct team battle participants per Wikipedia:
   Team 'All French': Hiroyuki Sakai, Yutaka Ishinabe, Etsuo Jō
   Team 'All China': Chen Kenichi, Shōzō Miyamoto, Yūji Wakiya
   Theme: Spare rib, snapping turtle, and banana
   Winner: Team 'All French'"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "Episode 239 exists with correct air date"
      (let [episode (get-episode-by-id conx 239)]
        (is (some? episode) "Episode 239 should exist")
        (is (= "August 28, 1998" (:episodes/air_date episode))
            "Episode 239 should air on August 28, 1998")))

    (testing "Episode 239 team battle chefs exist"
      (let [all-chefs (get-all-chefs conx)
            chef-names (set (map :chefs/name all-chefs))]
        ;; Team All French
        (is (contains? chef-names "Hiroyuki Sakai") "Sakai should exist")
        (is (contains? chef-names "Yutaka Ishinabe") "Ishinabe should exist")
        (is (contains? chef-names "Etsuo Jō") "Etsuo Jō should exist")
        ;; Team All China
        (is (contains? chef-names "Chen Kenichi") "Chen should exist")
        (is (contains? chef-names "Shōzō Miyamoto") "Shōzō Miyamoto should exist")
        (is (contains? chef-names "Yūji Wakiya") "Yūji Wakiya should exist")))

    (let [battles (get-battles-for-episode conx 239)
          battle (first battles)]

      (testing "Episode 239 has one battle"
        (is (= 1 (count battles)) "Episode 239 should have 1 battle"))

      (testing "Episode 239 battle theme"
        (is (= "Spare rib, snapping turtle, and banana" (:battles/theme_ingredient battle))
            "Theme should be 'Spare rib, snapping turtle, and banana'"))

      (testing "Episode 239 battle participants (team vs team, all challengers)"
        (let [iron-chefs (get-iron-chefs-for-battle conx (:battles/id battle))
              challengers (get-challengers-for-battle conx (:battles/id battle))
              challenger-names (set (map :chefs/name challengers))]
          (is (empty? iron-chefs)
              "Should have no Iron Chefs (team vs team battle)")
          (is (= #{"Hiroyuki Sakai" "Yutaka Ishinabe" "Etsuo Jō"
                   "Chen Kenichi" "Shōzō Miyamoto" "Yūji Wakiya"}
                 challenger-names)
              "All 6 chefs should be challengers")))

      (testing "Episode 239 winners (Team All French)"
        (let [winners (get-winners-for-battle conx (:battles/id battle))
              winner-names (set (map :chefs/name winners))]
          (is (= #{"Hiroyuki Sakai" "Yutaka Ishinabe" "Etsuo Jō"} winner-names)
              "Winners should be Team All French (Sakai, Ishinabe, Jō)"))))))

(deftest special-episodes-2000-2002-test
  "Verify special episodes from 2000-2002 are parsed and included"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (let [all-chefs (get-all-chefs conx)
          chef-names (set (map :chefs/name all-chefs))]

      ;; === 2000 Special Episode Chefs ===
      (testing "Millennium Cup (January 5, 2000) chefs are created"
        (is (contains? chef-names "Zhao Renliang")
            "Zhao Renliang (Millennium Cup challenger) should exist")
        (is (contains? chef-names "Dominique Bouchet")
            "Dominique Bouchet (Millennium Cup challenger) should exist"))

      (testing "New York Special (March 28, 2000) chefs are created"
        (is (contains? chef-names "Masaharu Morimoto")
            "Masaharu Morimoto (Iron Chef) should exist")
        (is (contains? chef-names "Bobby Flay")
            "Bobby Flay (New York Special challenger) should exist"))

      ;; === 2001 Special Episode Chefs ===
      (testing "21st Century Battles (January 2, 2001) - Kandagawa already exists from episode 61"
        (is (contains? chef-names "Toshirō Kandagawa")
            "Toshirō Kandagawa should exist"))

      ;; === 2002 Special Episode Chefs ===
      (testing "Japan Cup (January 2, 2002) chefs are created"
        (is (contains? chef-names "Yūichirō Ebisu")
            "Yūichirō Ebisu (Japan Cup challenger) should exist")
        (is (contains? chef-names "Kimio Nonaga")
            "Kimio Nonaga (Japan Cup Iron Chef) should exist")
        (is (contains? chef-names "Takeshi Tanabe")
            "Takeshi Tanabe (Japan Cup challenger) should exist")))))

(deftest special-episodes-2000-2002-air-dates-test
  "Verify special episodes from 2000-2002 have correct air dates"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "Millennium Cup (episode 292) has correct air date"
      (let [ep (get-episode-by-id conx 292)]
        (is (some? ep) "Millennium Cup episode should exist")
        (is (= "January 5, 2000" (:episodes/air_date ep)))))

    (testing "New York Special (episode 293) has correct air date"
      (let [ep (get-episode-by-id conx 293)]
        (is (some? ep) "New York Special episode should exist")
        (is (= "March 28, 2000" (:episodes/air_date ep)))))

    (testing "21st Century Battles (episode 294) has correct air date"
      (let [ep (get-episode-by-id conx 294)]
        (is (some? ep) "21st Century Battles episode should exist")
        (is (= "January 2, 2001" (:episodes/air_date ep)))))

    (testing "Japan Cup (episode 295) has correct air date"
      (let [ep (get-episode-by-id conx 295)]
        (is (some? ep) "Japan Cup episode should exist")
        (is (= "January 2, 2002" (:episodes/air_date ep)))))))

(deftest special-episodes-2000-2002-battles-test
  "Verify special episodes from 2000-2002 have correct battles"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    (testing "Millennium Cup (episode 292) has 2 battles"
      (let [battles (get-battles-for-episode conx 292)]
        (is (= 2 (count battles))
            "Millennium Cup should have 2 battles")
        (is (= #{"Abalone" "Kobe beef"}
               (set (map :battles/theme_ingredient battles)))
            "Millennium Cup battles should have correct theme ingredients")))

    (testing "New York Special (episode 293) has 1 battle"
      (let [battles (get-battles-for-episode conx 293)]
        (is (= 1 (count battles))
            "New York Special should have 1 battle")
        (is (= "Rock crab" (:battles/theme_ingredient (first battles)))
            "New York Special should have Rock crab theme")))

    (testing "21st Century Battles (episode 294) has 2 battles"
      (let [battles (get-battles-for-episode conx 294)]
        (is (= 2 (count battles))
            "21st Century Battles should have 2 battles")
        (is (= #{"Red snapper" "Spiny lobster"}
               (set (map :battles/theme_ingredient battles)))
            "21st Century Battles should have correct theme ingredients")))

    (testing "Japan Cup (episode 295) has 3 battles"
      (let [battles (get-battles-for-episode conx 295)]
        (is (= 3 (count battles))
            "Japan Cup should have 3 battles")
        (is (= #{"King crab" "Pacific bluefin tuna" "Ingii chicken"}
               (set (map :battles/theme_ingredient battles)))
            "Japan Cup battles should have correct theme ingredients")))))

;; === Video File Parsing Tests ===

(deftest parse-video-filename-test
  "Verify video filename parsing handles standard and special cases"

  (testing "Standard dubbed files"
    (is (= [{:path "IC101.avi" :episode-id 1 :audio "dubbed"}]
           (parse-video-filename "IC101.avi"))
        "IC101.avi -> season 1, ep 1 = episode 1, dubbed")
    (is (= [{:path "IC210.avi" :episode-id 20 :audio "dubbed"}]
           (parse-video-filename "IC210.avi"))
        "IC210.avi -> season 2, ep 10 = episode 20, dubbed")
    (is (= [{:path "IC350.avi" :episode-id 110 :audio "dubbed"}]
           (parse-video-filename "IC350.avi"))
        "IC350.avi -> season 3, ep 50 = episode 110, dubbed"))

  (testing "OA (Original Audio) suffix = original"
    (is (= [{:path "IC310OA.avi" :episode-id 70 :audio "original"}]
           (parse-video-filename "IC310OA.avi"))
        "IC310OA.avi -> episode 70, original"))

  (testing "s (subtitled) suffix = subtitled"
    (is (= [{:path "IC310s.avi" :episode-id 70 :audio "subtitled"}]
           (parse-video-filename "IC310s.avi"))
        "IC310s.avi -> episode 70, subtitled"))

  (testing "OT suffix = original"
    (is (= [{:path "IC310OT.avi" :episode-id 70 :audio "original"}]
           (parse-video-filename "IC310OT.avi"))
        "IC310OT.avi -> episode 70, original"))

  (testing "Part suffixes keep audio type"
    (is (= [{:path "IC310-Pt1.avi" :episode-id 70 :audio "dubbed"}]
           (parse-video-filename "IC310-Pt1.avi"))
        "IC310-Pt1.avi -> episode 70, dubbed (part suffix only)")
    (is (= [{:path "IC310a.avi" :episode-id 70 :audio "dubbed"}]
           (parse-video-filename "IC310a.avi"))
        "IC310a.avi -> episode 70, dubbed (a/b part suffix)")
    (is (= [{:path "IC310OA-Pt1.avi" :episode-id 70 :audio "original"}]
           (parse-video-filename "IC310OA-Pt1.avi"))
        "IC310OA-Pt1.avi -> episode 70, original (OA + part)"))

  (testing "Season offsets are applied correctly"
    (is (= [{:path "IC11.avi" :episode-id 1 :audio "dubbed"}]
           (parse-video-filename "IC11.avi"))
        "Season 1, ep 1 = episode 1 (base 0)")
    (is (= [{:path "IC21.avi" :episode-id 11 :audio "dubbed"}]
           (parse-video-filename "IC21.avi"))
        "Season 2, ep 1 = episode 11 (base 10)")
    (is (= [{:path "IC31.avi" :episode-id 61 :audio "dubbed"}]
           (parse-video-filename "IC31.avi"))
        "Season 3, ep 1 = episode 61 (base 60)")
    (is (= [{:path "IC41.avi" :episode-id 111 :audio "dubbed"}]
           (parse-video-filename "IC41.avi"))
        "Season 4, ep 1 = episode 111 (base 110)")
    (is (= [{:path "IC51.avi" :episode-id 163 :audio "dubbed"}]
           (parse-video-filename "IC51.avi"))
        "Season 5, ep 1 = episode 163 (base 162)")
    (is (= [{:path "IC61.avi" :episode-id 211 :audio "dubbed"}]
           (parse-video-filename "IC61.avi"))
        "Season 6, ep 1 = episode 211 (base 210)"))

  (testing "Special case: IC415-416 -> two episodes"
    (is (= [{:path "IC415-416.avi" :episode-id 125 :audio "dubbed"}
            {:path "IC415-416.avi" :episode-id 126 :audio "dubbed"}]
           (parse-video-filename "IC415-416.avi"))
        "IC415-416.avi -> episodes 125 and 126 (France Special)"))

  (testing "Special case: IC452OA-NYE -> episode 162"
    (is (= [{:path "IC452OA-NYE1.zip" :episode-id 162 :audio "original"}]
           (parse-video-filename "IC452OA-NYE1.zip"))
        "IC452OA-NYE1.zip -> episode 162, original")
    (is (= [{:path "IC452OA-NYE2.zip" :episode-id 162 :audio "original"}]
           (parse-video-filename "IC452OA-NYE2.zip"))
        "IC452OA-NYE2.zip -> episode 162, original"))

  (testing "Special case: IC315OA85 -> episode 75"
    (is (= [{:path "IC315OA85-Pt1.avi" :episode-id 75 :audio "original"}]
           (parse-video-filename "IC315OA85-Pt1.avi"))
        "IC315OA85-Pt1.avi -> episode 75, original")
    (is (= [{:path "IC315OA85-Pt2.avi" :episode-id 75 :audio "original"}]
           (parse-video-filename "IC315OA85-Pt2.avi"))
        "IC315OA85-Pt2.avi -> episode 75, original"))

  (testing "Special case: 97ICWC -> episode 198"
    (is (= [{:path "97ICWC1OA.avi" :episode-id 198 :audio "original"}]
           (parse-video-filename "97ICWC1OA.avi"))
        "97ICWC1OA.avi -> episode 198, original (OA)")
    (is (= [{:path "97ICWCpt2.avi" :episode-id 198 :audio "dubbed"}]
           (parse-video-filename "97ICWCpt2.avi"))
        "97ICWCpt2.avi -> episode 198, dubbed (no OA)"))

  (testing "Non-video files return nil"
    (is (nil? (parse-video-filename "notes.txt"))
        ".txt files should be skipped")
    (is (nil? (parse-video-filename "index.sqlite.part"))
        ".part files should be skipped")
    (is (nil? (parse-video-filename "metadata.xml"))
        ".xml files should be skipped"))

  (testing "Beijing files return nil"
    (is (nil? (parse-video-filename "BeijingFinal.zip"))
        "BeijingFinal.zip should be skipped")
    (is (nil? (parse-video-filename "BeijingPre.zip"))
        "BeijingPre.zip should be skipped"))

  (testing "Invalid season returns nil"
    (is (nil? (parse-video-filename "IC710.avi"))
        "Season 7 doesn't exist"))

  (testing "mp4 and zip extensions work"
    (is (= [{:path "IC310.mp4" :episode-id 70 :audio "dubbed"}]
           (parse-video-filename "IC310.mp4"))
        "mp4 files are parsed")
    (is (= [{:path "IC310.zip" :episode-id 70 :audio "dubbed"}]
           (parse-video-filename "IC310.zip"))
        "zip files are parsed"))

  (testing "Verified mapping: IC452 = episode 162 (NYE special)"
    (is (= [{:path "IC452.avi" :episode-id 162 :audio "dubbed"}]
           (parse-video-filename "IC452.avi"))
        "IC452 should map to episode 162 (season 4 base 110 + 52)")))

(deftest scan-videos-test
  "Verify scan-videos! populates movie_files from a directory of video files"
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)

    ;; Create a temp directory with some fake video files
    (let [tmp-dir (java.io.File/createTempFile "ic-videos" "")
          _ (.delete tmp-dir)
          _ (.mkdirs tmp-dir)]
      (try
        ;; Create test files
        (doseq [name ["IC101.avi"         ;; ep 1
                       "IC210.avi"         ;; ep 20
                       "IC310OA.avi"       ;; ep 70, original audio
                       "IC710.avi"         ;; invalid season, should skip
                       "notes.txt"         ;; non-video, should skip
                       "BeijingFinal.zip"  ;; should skip
                       ]]
          (.createNewFile (java.io.File. tmp-dir name)))

        (scan-videos! conx (.getAbsolutePath tmp-dir))

        (let [rows (jdbc/execute! conx ["select * from movie_files order by episode_id"])]
          (testing "Only valid video files are inserted"
            (is (= 3 (count rows))
                "Should have 3 movie file records"))

          (testing "Episode IDs are correct"
            (is (= [1 20 70] (map :movie_files/episode_id rows))))

          (testing "Audio types are correct"
            (is (= ["dubbed" "dubbed" "original"] (map :movie_files/audio rows))
                "First two should be dubbed, third should be original"))

          (testing "Paths are filenames"
            (is (= ["IC101.avi" "IC210.avi" "IC310OA.avi"]
                   (map :movie_files/path rows)))))

        (finally
          ;; Clean up temp files
          (doseq [f (.listFiles tmp-dir)] (.delete f))
          (.delete tmp-dir))))))
