(ns handler
  (:require
   [clojure.data.csv :as csv]
   [clojure.string :as str]
   [langohr.basic :as lb]
   [langohr.channel :as lch]
   [langohr.consumers :as lc]
   [langohr.core :as rmq]
   [langohr.queue :as lq])
  (:import
   [java.io StringReader StringWriter]))

(def queue-in "weather.chunks.in")

(defn safe-double [s]
  (when (and s (not (str/blank? s)))
    (parse-double (str/trim s))))

(defn parse-open-meteo-row [[time temp hum rain snow snow-depth
                             wind10 wind100 dir10 dir100
                             press-msl surf-press soil-temp soil-moist]]
  (let [t (safe-double temp)
        h (safe-double hum)
        p (safe-double press-msl)]
    (when (and (seq time) t h p (< -90.0 t 70.0))
      {:time       (str/trim time)
       :temp       t
       :hum        h
       :rain       (or (safe-double rain) 0.0)
       :snow       (or (safe-double snow) 0.0)
       :snow-depth (or (safe-double snow-depth) 0.0)
       :wind10     (or (safe-double wind10) 0.0)
       :wind100    (or (safe-double wind100) 0.0)
       :dir10      (or (safe-double dir10) 0.0)
       :dir100     (or (safe-double dir100) 0.0)
       :press-msl  p
       :surf-press (or (safe-double surf-press) p)
       :soil-temp  (or (safe-double soil-temp) t)
       :soil-moist (or (safe-double soil-moist) 0.0)})))

(defn enrich-features [rows]
  (map (fn [[r1 r2 r3]]
         (let [temp-ma3   (/ (+ (:temp r1) (:temp r2) (:temp r3)) 3.0)
               press-diff (- (:press-msl r3) (:press-msl r2))]
           (assoc r3
                  :temp-ma3   (String/format java.util.Locale/US "%.2f" (into-array Object [temp-ma3]))
                  :press-diff (String/format java.util.Locale/US "%.2f" (into-array Object [press-diff])))))
       (partition 3 1 rows)))

(def output-keys
  [:time :temp :hum :rain :snow :snow-depth
   :wind10 :wind100 :dir10 :dir100
   :press-msl :surf-press :soil-temp :soil-moist
   :temp-ma3 :press-diff])

(defn process-csv-string [csv-str]
  (with-open [reader (StringReader. csv-str)]
    (let [raw-rows   (csv/read-csv reader)
          clean-rows (->> raw-rows
                          (keep parse-open-meteo-row)
                          (enrich-features))
          out-writer (StringWriter.)]
      (csv/write-csv out-writer (map (apply juxt output-keys) clean-rows))
      (str out-writer))))

(defn connect-with-retry [settings]
  (loop [attempt 1]
    (let [conn (try
                 (rmq/connect settings)
                 (catch Exception e
                   (println (str "Попытка #" attempt ": RabbitMQ еще не готов (" (.getMessage e) "), жду 3 сек..."))
                   nil))]
      (if conn
        conn
        (do
          (Thread/sleep 3000)
          (recur (inc attempt)))))))

(defn message-handler [worker-id ch {:keys [delivery-tag reply-to correlation-id]} ^bytes payload]
  (let [raw-csv (String. payload "UTF-8")]
    (println (str "[" worker-id "] Получен чанк #" correlation-id ", размер: " (count raw-csv) " байт"))
    (try
      (let [processed-csv (process-csv-string raw-csv)]
        ;; Отправляем результат обратно в персональную очередь Java-оркестратора
        (when reply-to
          (lb/publish ch "" reply-to (.getBytes processed-csv "UTF-8")
                      {:correlation-id correlation-id
                       :content-type "text/csv"}))
        ;; Подтверждаем RabbitMQ, что сообщение успешно обработано
        (lb/ack ch delivery-tag)
        (println (str "[" worker-id "] Чанк #" correlation-id " готов!")))
      (catch Exception e
        (println (str "[" worker-id "] Ошибка обработки чанка #" correlation-id ": " (.getMessage e)))
        ;; Отклоняем сообщение без возврата в очередь (чтобы не зациклить битый чанк)
        (lb/nack ch delivery-tag false false)))))

(defn -main []
  (let [worker-id (or (System/getenv "HOSTNAME") "local-worker")
        rabbit-settings {:host     (or (System/getenv "RABBIT_HOST") "localhost")
                         :port     (parse-long (or (System/getenv "RABBIT_PORT") "5672"))
                         :username (or (System/getenv "RABBIT_USER") "guest")
                         :password (or (System/getenv "RABBIT_PASS") "guest")}
        conn (connect-with-retry rabbit-settings)
        ch   (lch/open conn)]
    ;; Объявляем очередь входящих чанков
    (lq/declare ch queue-in {:durable true :auto-delete false})
    ;; Воркер берет строго 1 чанк за раз, пока не обработает текущий
    (lb/qos ch 1)
    ;; Запускаем неблокирующего консьюмера с ручным подтверждением (ack)
    (lc/subscribe ch queue-in (partial message-handler worker-id) {:auto-ack false})
    (println (str "[" worker-id "] Подключен к RabbitMQ. Жду задачи в " queue-in "..."))
    ;; Бесконечно удерживаем главный поток JVM, пока консьюмер работает в фоне
    @(promise)))
