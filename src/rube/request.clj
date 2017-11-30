(ns rube.request
  (:require
   [clojure.data.json :as json]
   [taoensso.timbre :as timbre]
   [clojure.string :as str]
   [byte-streams :as bs]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [aleph.http :as http]
   [clojure.core.async :as a :refer [<! >! <!! alts!! go go-loop timeout chan close!]]))

;; pretty much copied from clj-kubernetes-api
(defn- parameterize-path [path params]
  (when-not path (throw (ex-info "Path is required" {})))
  (reduce-kv (fn [s k v]
               (str/replace s (re-pattern (str "\\{" (str k) "\\}")) v))
             path
             (or params {})))

;; pretty much copied from clj-kubernetes-api
(defn- url [{:keys [server]} path params]
  (str server (parameterize-path path params)))

;; pretty much copied from clj-kubernetes-api
(defn- content-type [method]
  (case method
    :patch "application/merge-patch+json"
    "application/json"))

(defn- tail-watch-request
  "Handle the WATCH protocol: an HTTP request body is kept open and lines of JSON are streamed to us.
  Parse the JSON as it arrives and put it on `return-ch`. Exit if `kill-ch` closes."
  [req return-ch kill-ch]
  (let [raw-ch (chan)]
    (-> req
        (d/chain
         :body
         #(s/filter identity %)
         #(s/map bs/to-string %))
        deref
        (s/connect raw-ch))
    (a/thread
      (loop [buf ""]
        (let [[chunk p] (a/alts!! [raw-ch kill-ch])]
          (when-not (= p kill-ch)
            (if (nil? chunk)
              (close! return-ch)
              (if-let [i (str/index-of chunk "\n")]
                (do
                  (a/put! return-ch
                          (-> (subs (str buf chunk) 0 (+ (count buf) i 1))
                              (json/read-str)))
                  (recur (subs chunk (inc i))))
                (recur (str buf chunk))))))))))

(def default-connection-pool
  (memoize
   (fn [watch? ks]
     (http/connection-pool
      {:connections-per-host 128
       :insecure? false
       :connection-options {:ssl-context ks
                            :raw-stream? watch?}}))))

(defn request [{:keys [ks kube-token namespace] :as ctx} {:keys [method path params query body kill-ch pool] :as req-opt}]
  (let [params     (merge {"namespace" namespace} params)
        watch?     (:watch query)
        return-ch  (chan)
        req (http/request
             {:query-params query
              :body (json/write-str body)
              :headers (cond-> {"Content-Type" (content-type method)}
                         kube-token (assoc "Authorization" (str "Bearer " kube-token)))
              :pool (or pool (default-connection-pool watch? ks))
              :method method
              :url (url ctx path params)

              :pool-timeout       16000
              :connection-timeout 16000
              :request-timeout    16000})]

    (if watch?

      ;; continuously put parsed objects on the return channel as they arrive
      (tail-watch-request req return-ch kill-ch)

      ;; put the parsed body on the return channel
      (-> req
          (d/chain (fn [z]
                     (update z :body #(-> % bs/to-string (json/read-str)))))
          (d/catch (fn [e]
                     (if-let [error-response (ex-data e)]
                       (-> error-response (update :body (comp #(json/read-str %) bs/to-string)))
                       e)))
          (s/connect return-ch)))

    return-ch))

(defn watch-request [ctx resource-version request-params]
  (request ctx (update request-params :query assoc
                       :watch true
                       :resourceVersion resource-version)))
