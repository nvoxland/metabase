(ns dev.engine-bench
  "A side-by-side benchmark of the two execution paths for the same SQL: the Postgres JDBC driver, and the native query
  engine (plan, unparse, execute on the engine's own connection, Arrow back). Builds a generated e-commerce database
  in the local Postgres, registers and syncs it in Metabase, then runs a set of analytical queries through both
  drivers and compares the results and timings.

  The queries are written in the intersection of the engine's dialect and Postgres, so the exact same text runs both
  ways. A saved native question always runs on the driver (its SQL is the warehouse's own dialect); this calls the two
  drivers directly, which is what a card in the engine dialect would do once that exists.

    (require 'dev.engine-bench)
    (dev.engine-bench/create-dataset!)      ; a few minutes; idempotent per table
    (def db (dev.engine-bench/register!))   ; the Metabase Database, synced
    (dev.engine-bench/run-benchmark! db)    ; prints the comparison table, returns the measurements

  Run the REPL against a release build of the engine for meaningful numbers
  (`MB_ENGINE_NATIVE_LIBRARY_PATH=engine/target/release/libmetabase_engine.dylib`), with the application initialized
  (`(require 'metabase.core.init 'metabase-enterprise.core.init)` in a bare REPL, or sync can't read every setting).

  Both paths get the same row cap, from how the query runs (`:as :card`, the default 2,000 rows, or `:as :download`).
  Reading the numbers: the JDBC path passes the cap to `Statement.setMaxRows`, which Postgres receives as a
  row-limited portal and plans without parallel workers; the engine stops the statement after the cap instead, which
  leaves the plan alone. As a card, the JDBC path also buffers the whole (capped) result before the first row."
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.engine.core :as engine]
   [metabase.query-processor.core :as qp.constraints]
   [metabase.query-processor.store :as qp.store]
   [metabase.sync.core :as sync]
   [toucan2.core :as t2])
  (:import
   (java.time OffsetDateTime)))

(set! *warn-on-reflection* true)

(def db-name
  "The generated database's name, in Postgres and in Metabase."
  "engine_bench")

(def ^:private server
  {:host "localhost" :port 5432 :user "metabase" :password "password"})

(defn- spec [dbname]
  (merge {:dbtype "postgresql" :dbname dbname} server))

;;; ------------------------------------------------------- data --------------------------------------------------------

(def ^:private tables
  "Each table: its DDL and the statement that fills it. Sizes are chosen so the big queries take hundreds of
  milliseconds on the warehouse, where per-query overhead and row transfer both show."
  [["regions"
    "CREATE TABLE regions (id int PRIMARY KEY, name text NOT NULL, country text NOT NULL)"
    "INSERT INTO regions
     SELECT g, 'Region ' || g, (ARRAY['US','CA','GB','DE','FR','BR','JP','IN','AU','MX'])[1 + g % 10]
     FROM generate_series(1, 40) g"]
   ["customers"
    "CREATE TABLE customers (id int PRIMARY KEY, name text NOT NULL, email text NOT NULL, region_id int NOT NULL,
                             segment text NOT NULL, signup_at timestamptz NOT NULL, birth_year int)"
    "INSERT INTO customers
     SELECT g,
            'Customer ' || g,
            'customer' || g || '@example.' || (ARRAY['com','org','net','io'])[1 + g % 4],
            1 + (g * 7) % 40,
            (ARRAY['consumer','consumer','consumer','smb','smb','enterprise'])[1 + g % 6],
            timestamptz '2021-01-01 00:00:00+00' + (random() * 1000) * interval '1 day',
            CASE WHEN g % 11 = 0 THEN NULL ELSE 1950 + (g % 55) END
     FROM generate_series(1, 200000) g"]
   ["products"
    "CREATE TABLE products (id int PRIMARY KEY, name text NOT NULL, category text NOT NULL, brand text NOT NULL,
                            price numeric(10,2) NOT NULL, cost numeric(10,2) NOT NULL)"
    "INSERT INTO products
     SELECT g,
            (ARRAY['Basic','Pro','Ultra','Mini','Max','Eco'])[1 + g % 6] || ' ' ||
              (ARRAY['Widget','Gadget','Gizmo','Doohickey','Sprocket','Thingamajig'])[1 + (g / 6) % 6] || ' ' || g,
            (ARRAY['Home','Garden','Toys','Books','Electronics','Sports','Beauty','Grocery','Auto','Office',
                   'Pets','Music'])[1 + g % 12],
            'Brand ' || (1 + (g * 31) % 200),
            round((5 + random() * 495)::numeric, 2),
            0
     FROM generate_series(1, 10000) g;
     UPDATE products SET cost = round(price * (0.35 + (id % 30) / 100.0), 2)"]
   ["orders"
    "CREATE TABLE orders (id bigint PRIMARY KEY, customer_id int NOT NULL, status text NOT NULL, channel text NOT NULL,
                          created_at timestamptz NOT NULL, shipped_at timestamptz, discount numeric(5,2) NOT NULL)"
    ;; skewed toward a minority of heavy customers, and growing over time (more orders recently)
    "INSERT INTO orders
     SELECT g,
            1 + floor(200000 * power(random(), 2))::int,
            (ARRAY['complete','complete','complete','complete','complete','complete','returned','cancelled',
                   'pending'])[1 + floor(random() * 9)::int],
            (ARRAY['web','web','mobile','mobile','store','partner'])[1 + floor(random() * 6)::int],
            timestamptz '2022-01-01 00:00:00+00' + (sqrt(random()) * 1000) * interval '1 day',
            NULL,
            CASE WHEN random() < 0.2 THEN round((random() * 30)::numeric, 2) ELSE 0 END
     FROM generate_series(1, 2000000) g;
     UPDATE orders SET shipped_at = created_at + (1 + id % 7) * interval '1 day' WHERE status IN ('complete','returned')"]
   ["order_items"
    "CREATE TABLE order_items (id bigserial PRIMARY KEY, order_id bigint NOT NULL, product_id int NOT NULL,
                               quantity int NOT NULL, unit_price numeric(10,2) NOT NULL)"
    "INSERT INTO order_items (order_id, product_id, quantity, unit_price)
     SELECT o.id, p.id, 1 + (o.id + i) % 4, p.price
     FROM orders o
     CROSS JOIN LATERAL generate_series(1, 1 + (o.id % 5)::int) i
     JOIN products p ON p.id = 1 + ((o.id * 7919 + i * 104729) % 10000)"]
   ["page_views"
    "CREATE TABLE page_views (id bigint PRIMARY KEY, customer_id int, session_id text NOT NULL, url text NOT NULL,
                              referrer text, device text NOT NULL, viewed_at timestamptz NOT NULL, duration_ms int NOT NULL)"
    "INSERT INTO page_views
     SELECT g,
            CASE WHEN random() < 0.6 THEN 1 + floor(200000 * random())::int END,
            md5((g / 7)::text),
            (ARRAY['/','/search','/product','/product','/product','/cart','/checkout','/account','/help',
                   '/category/home','/category/toys','/category/books'])[1 + floor(random() * 12)::int],
            (ARRAY[NULL,NULL,'google.com','bing.com','facebook.com','newsletter','partner.example'])[1 + floor(random() * 7)::int],
            (ARRAY['desktop','desktop','mobile','mobile','mobile','tablet'])[1 + floor(random() * 6)::int],
            timestamptz '2024-01-01 00:00:00+00' + random() * interval '270 days',
            floor(500 + random() * 120000)::int
     FROM generate_series(1, 3000000) g"]])

(def ^:private indexes
  ["CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id)"
   "CREATE INDEX IF NOT EXISTS orders_created_idx ON orders (created_at)"
   "CREATE INDEX IF NOT EXISTS order_items_order_idx ON order_items (order_id)"
   "CREATE INDEX IF NOT EXISTS order_items_product_idx ON order_items (product_id)"
   "CREATE INDEX IF NOT EXISTS page_views_viewed_idx ON page_views (viewed_at)"])

(defn- table-exists? [db table]
  (seq (jdbc/query db ["SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?" table])))

(defn create-dataset!
  "Create the `engine_bench` database and fill it (skipping tables that already exist), then add indexes and
  statistics. Returns the row count of every table."
  []
  (let [admin (spec "metabase_1")]
    (when-not (seq (jdbc/query admin ["SELECT 1 FROM pg_database WHERE datname = ?" db-name]))
      (jdbc/execute! admin [(str "CREATE DATABASE " db-name)] {:transaction? false})))
  (let [db (spec db-name)]
    (doseq [[table ddl fill] tables
            :when (not (table-exists? db table))]
      (let [start (System/nanoTime)]
        (jdbc/execute! db [ddl])
        (doseq [statement (str/split fill #";\s*")]
          (jdbc/execute! db [statement] {:transaction? false}))
        (printf "%-12s %6.1f s%n" table (/ (- (System/nanoTime) start) 1e9))
        (flush)))
    (doseq [statement indexes]
      (jdbc/execute! db [statement] {:transaction? false}))
    (jdbc/execute! db ["ANALYZE"] {:transaction? false})
    (into (sorted-map)
          (for [[table] tables]
            [table (:count (first (jdbc/query db [(str "SELECT count(*) AS count FROM " table)])))]))))

;;; ------------------------------------------------ Metabase database ------------------------------------------------

(defn register!
  "The Metabase Database for `engine_bench`, created and synced (schema only) if it doesn't exist yet."
  []
  (or (t2/select-one :model/Database :engine :postgres :name db-name)
      (let [db (first (t2/insert-returning-instances! :model/Database
                                                      {:name    db-name
                                                       :engine  :postgres
                                                       :details (assoc server :dbname db-name :ssl false)}))]
        (sync/sync-database! db {:scan :schema})
        (t2/select-one :model/Database :id (:id db)))))

;;; ----------------------------------------------------- queries -----------------------------------------------------

(def queries
  "Analytical queries in the intersection of the engine's dialect and Postgres: `[name description sql]`."
  [["point-lookup"
    "one customer's orders with item counts (latency floor)"
    "SELECT o.id, o.status, o.created_at, sum(oi.quantity) AS items
     FROM orders o JOIN order_items oi ON oi.order_id = o.id
     WHERE o.customer_id = 4242
     GROUP BY o.id, o.status, o.created_at
     ORDER BY o.created_at DESC, o.id"]
   ["small-table-filter"
    "string filter on a small table"
    "SELECT brand, count(*) AS products, avg(price) AS avg_price
     FROM products
     WHERE lower(name) LIKE '%pro%'
     GROUP BY brand
     ORDER BY products DESC, brand
     LIMIT 20"]
   ["revenue-by-month-category"
    "3-way join, monthly revenue per category"
    "SELECT date_trunc('month', o.created_at) AS month, p.category,
            sum(oi.quantity * oi.unit_price) AS revenue, count(DISTINCT o.id) AS orders
     FROM orders o
     JOIN order_items oi ON oi.order_id = o.id
     JOIN products p ON p.id = oi.product_id
     WHERE o.status = 'complete'
     GROUP BY date_trunc('month', o.created_at), p.category
     ORDER BY month, p.category"]
   ["top-customers-per-region"
    "aggregate, then rank within region"
    "SELECT region_id, customer_id, spend, r
     FROM (SELECT s.region_id, s.customer_id, s.spend,
                  rank() OVER (PARTITION BY s.region_id ORDER BY s.spend DESC, s.customer_id) AS r
           FROM (SELECT c.region_id, c.id AS customer_id, sum(oi.quantity * oi.unit_price) AS spend
                 FROM customers c
                 JOIN orders o ON o.customer_id = c.id
                 JOIN order_items oi ON oi.order_id = o.id
                 GROUP BY c.region_id, c.id) s) ranked
     WHERE r <= 5
     ORDER BY region_id, r"]
   ["rolling-7-day"
    "daily orders with a 7-day moving average"
    "SELECT day, orders,
            avg(orders) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg
     FROM (SELECT date_trunc('day', created_at) AS day, count(*) AS orders
           FROM orders GROUP BY date_trunc('day', created_at)) d
     ORDER BY day"]
   ["median-by-channel"
    "ordered-set aggregate over 2M rows"
    "SELECT channel, count(*) AS orders,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY discount) AS median_discount,
            avg(discount) AS avg_discount
     FROM orders
     GROUP BY channel
     ORDER BY channel"]
   ["cohort-retention"
    "signup cohort x order month, distinct customers"
    "SELECT date_trunc('month', c.signup_at) AS cohort, date_trunc('month', o.created_at) AS month,
            count(DISTINCT c.id) AS active_customers
     FROM customers c JOIN orders o ON o.customer_id = c.id
     WHERE c.signup_at >= timestamptz '2022-01-01 00:00:00+00'
     GROUP BY date_trunc('month', c.signup_at), date_trunc('month', o.created_at)
     ORDER BY cohort, month"]
   ["sessions-by-device"
    "3M-row scan with count distinct"
    "SELECT device, count(*) AS views, count(DISTINCT session_id) AS sessions, avg(duration_ms) AS avg_ms
     FROM page_views
     WHERE viewed_at >= timestamptz '2024-03-01 00:00:00+00'
     GROUP BY device
     ORDER BY device"]
   ["extract-50k"
    "50k-row detail extract (row transfer)"
    "SELECT o.id, o.customer_id, o.status, o.channel, o.created_at, o.shipped_at, o.discount
     FROM orders o
     WHERE o.id <= 50000
     ORDER BY o.id"]
   ["extract-500k"
    "500k-row detail extract (row transfer)"
    "SELECT o.id, o.customer_id, o.status, o.channel, o.created_at, o.shipped_at, o.discount
     FROM orders o
     WHERE o.id <= 500000
     ORDER BY o.id"]])

;;; ----------------------------------------------------- running -----------------------------------------------------

(defn- normalize
  "A value in a form both paths agree on: numbers as doubles rounded to 6 places (the engine returns sums of integers as
  longs and averages as doubles where JDBC returns BigDecimals), instants as instants."
  [v]
  (cond
    (number? v)                  (/ (Math/round (* (double v) 1e6)) 1e6)
    (instance? OffsetDateTime v) (.toInstant ^OffsetDateTime v)
    :else                        v))

(def ^:private run-as
  "How Metabase runs the query, which decides its row cap on both paths (and, on the JDBC path, whether results
  stream): as a saved question (the default 2,000-row cap) or as a CSV download (1,048,575)."
  {:card     {:constraints (qp.constraints/default-query-constraints) :info {:context :question}}
   :download {:info {:context :csv-download}}})

(defn- execute
  "Run `sql` through `driver` against `db` the way `as` says, and reduce every row. Returns
  `{:ms :first-row-ms :rows}`, with the rows only when `keep-rows?`."
  [driver db sql as keep-rows?]
  (qp.store/with-metadata-provider (:id db)
    (binding [driver/*driver* driver]
      (let [start   (System/nanoTime)
            ms      #(/ (- (System/nanoTime) start) 1e6)
            first-t (volatile! nil)
            result  (driver/execute-reducible-query
                     driver
                     (merge {:database (:id db) :type :native :native {:query sql :params []}}
                            (run-as as))
                     {}
                     (fn [_metadata rows]
                       (reduce (fn [acc row]
                                 (when-not @first-t (vreset! first-t (ms)))
                                 (if keep-rows? (conj! acc (mapv normalize row)) (conj! acc nil)))
                               (transient [])
                               rows)))]
        {:ms           (ms)
         :first-row-ms (or @first-t (ms))
         :rows         (persistent! result)}))))

(defn- median [xs]
  (let [xs (vec (sort xs))]
    (nth xs (quot (count xs) 2))))

(defn- measure
  "Time `sql` on both paths: one warm-up each, then `runs` alternating runs. Checks the two answers match."
  [db sql as runs]
  (let [jdbc-rows   (:rows (execute :postgres db sql as true))
        engine-rows (:rows (execute :datafusion db sql as true))
        timings     (for [_ (range runs)
                          path [:postgres :datafusion]]
                      [path (execute path db sql as false)])
        by-path     (group-by first timings)
        summarize   (fn [path]
                      (let [ts (map second (by-path path))]
                        {:median-ms       (median (map :ms ts))
                         :min-ms          (apply min (map :ms ts))
                         :first-row-ms    (median (map :first-row-ms ts))}))]
    {:rows   (count jdbc-rows)
     :match? (= jdbc-rows engine-rows)
     :jdbc   (summarize :postgres)
     :engine (summarize :datafusion)}))

(defn plan-ms
  "How long the engine takes to plan and unparse `sql` (no execution), median of `runs`."
  [db sql runs]
  (engine/with-session [session {:database_id (:id db)}]
    (engine/plan-and-unparse session sql)
    (median (for [_ (range runs)]
              (let [start (System/nanoTime)]
                (engine/plan-and-unparse session sql)
                (/ (- (System/nanoTime) start) 1e6))))))

(defn run-benchmark!
  "Run every query through both paths, print a comparison, and return the measurements."
  ([db] (run-benchmark! db {}))
  ([db {:keys [runs only as] :or {runs 5 as :card}}]
   (let [results (vec (for [[query-name description sql] queries
                            :when (or (nil? only) (contains? (set only) query-name))]
                        (do (printf "running %s...%n" query-name) (flush)
                            (merge {:name query-name :description description}
                                   (measure db sql as runs)
                                   {:plan-ms (plan-ms db sql runs)}))))]
     (printf "%nrun as: %s%n" (name as))
     (printf "%-26s %8s %6s  %9s %9s %7s  %9s %9s  %7s%n"
             "query" "rows" "same?" "jdbc ms" "engine ms" "ratio" "jdbc 1st" "eng 1st" "plan ms")
     (doseq [{:keys [name rows match? jdbc engine plan-ms]} results]
       (printf "%-26s %8d %6s  %9.1f %9.1f %6.2fx  %9.1f %9.1f  %7.2f%n"
               name rows (if match? "yes" "NO")
               (:median-ms jdbc) (:median-ms engine) (/ (:median-ms engine) (:median-ms jdbc))
               (:first-row-ms jdbc) (:first-row-ms engine) plan-ms))
     (flush)
     results)))
