(ns iron-chef-index.core-test
  (:require [clojure.test :refer :all]
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
        (is (= "5" (element-text (get (nth table-maps 4) "Episode #"))))
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
        (is (= 2 (count (get-all-challengers conx))) "Challengers should be retrieved")))))

(deftest execute-test
  (jdbc/with-transaction [conx ds {:rollback-only true}]
    (execute! conx)
    (testing "After execution, the correct number of episodes is created"
      (is (= 59 (count (get-all-episodes conx)))))
    (testing "After execution, the correct number of chefs are created "
      (is (= 63 (count (get-all-chefs conx)))))

    (testing "The right number of iron chefs should be allocated during the series"
      (is (= 4 (count (get-all-iron-chefs conx)))))
    (testing "The right number of challengers should be allocated during the series"
      (is (= 59 (count (get-all-challengers conx)))))))
