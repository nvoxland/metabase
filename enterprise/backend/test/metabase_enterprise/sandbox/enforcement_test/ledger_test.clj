(ns metabase-enterprise.sandbox.enforcement-test.ledger-test
  "The ledger's own rules, against a temporary ledger file: what makes a run fail, and what write mode records."
  (:require
   [clojure.test :refer :all]
   [metabase-enterprise.sandbox.enforcement-test.ledger :as ledger]
   [metabase.driver :as driver]))

(set! *warn-on-reflection* true)

(defn- run-against-ledger!
  "Run `thunk` against a ledger file holding `entries` for test `:t` on H2, in write mode when `write?`. Returns the
  failed assertions' messages and the file's contents afterwards."
  [entries write? thunk]
  (let [f        (java.io.File/createTempFile "ledger" ".edn")
        failures (atom [])]
    (try
      (spit f (pr-str {:t {:h2 entries}}))
      (with-redefs [ledger/path        (.getPath f)
                    ledger/write-mode? (constantly write?)]
        (binding [driver/*driver*     :h2
                  clojure.test/report (fn [m] (when (#{:fail :error} (:type m)) (swap! failures conj (:message m))))]
          (thunk)))
      {:failures @failures :file (slurp f)}
      (finally
        (.delete f)))))

(def ^:private known
  [{:state {:policy :p :mode :production :probe :a} :outcome :raw-ref :ticket "SEC-1"}])

(deftest ^:synchronized known-findings-pass-test
  (is (= [] (:failures (run-against-ledger! known false #(ledger/with-ledger :t
                                                           (ledger/check! :t {:policy :p :mode :production :probe :a} :raw-ref true)
                                                           (ledger/check! :t {:policy :p :mode :production :probe :b} :consistent false)))))))

(deftest ^:synchronized new-failure-fails-test
  (is (= 1 (count (:failures (run-against-ledger! known false #(ledger/with-ledger :t
                                                                 (ledger/check! :t {:policy :p :mode :production :probe :a} :raw-ref true)
                                                                 (ledger/check! :t {:policy :p :mode :production :probe :b} :extra-rows true))))))))

(deftest ^:synchronized changed-or-fixed-finding-fails-test
  (testing "another failing outcome"
    (is (= 1 (count (:failures (run-against-ledger! known false #(ledger/with-ledger :t
                                                                   (ledger/check! :t {:policy :p :mode :production :probe :a} :canary-leak true))))))))
  (testing "fixed: the state now passes"
    (is (= 1 (count (:failures (run-against-ledger! known false #(ledger/with-ledger :t
                                                                   (ledger/check! :t {:policy :p :mode :production :probe :a} :consistent false)))))))))

(deftest ^:synchronized stale-entry-fails-only-within-the-scope-that-ran-test
  (testing "the policy and mode ran, but the listed state did not"
    (is (= 1 (count (:failures (run-against-ledger! known false #(ledger/with-ledger :t
                                                                   (ledger/check! :t {:policy :p :mode :production :probe :b} :consistent false))))))))
  (testing "another policy ran: the entry is not judged"
    (is (= [] (:failures (run-against-ledger! known false #(ledger/with-ledger :t
                                                             (ledger/check! :t {:policy :q :mode :production :probe :a} :consistent false))))))))

(deftest ^:synchronized write-mode-records-failing-states-with-tickets-test
  (let [{:keys [failures file]} (run-against-ledger! known true #(ledger/with-ledger :t
                                                                   (ledger/check! :t {:policy :p :mode :production :probe :a} :consistent false)
                                                                   (ledger/check! :t {:policy :p :mode :production :probe :c} :raw-ref true)))]
    (is (= [] failures))
    (testing "the fixed state is gone, the new one is recorded with the ticket classify gives it"
      (is (not (re-find #":probe :a" file)))
      (is (re-find #":probe :c" file))
      (is (re-find #"SEC-1336" file)))))

(deftest ^:parallel classify-test
  (testing "a hidden column's name in an error is part of the column sandbox's accidental failure"
    (is (= "SEC-1338" (ledger/classify :hand-written {:policy :card-hide-price :mode :production :probe :nested-shops}
                                       :hidden-name-in-error)))))
