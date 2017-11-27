(ns rube.api.swagger
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as timbre]
            [aleph.http :as http]
            [byte-streams :as bs]
            [manifold.deferred :as md]
            [com.stuartsierra.component :as component]
            [clojure.data.json :as json])
  (:import [java.security.cert X509Certificate]))

(defn gen-resource-map [{:keys [server kube-token ks]}]
  "Queries Kubernetes for watchable resources, returning a map of resource name
  to URI path suitable for get operations."
  (timbre/info "Fetching Kubernetes API layout from swagger endpoint")
  (when ks (timbre/info "Using Kubernetes-provided certificate store"))
  (when kube-token (timbre/info "Using Kubernetes-provided access token"))
  (-> (md/chain (http/get (str server "/swagger.json")
                          {:accept "application/json"
                           :ssl-context ks
                           :pool (http/connection-pool {:insecure? true
                                                        :connection-options {:ssl-context ks}})
                           :headers (cond-> {"Content-Type" "application/json"}
                                      kube-token (assoc "Authorization" (str "Bearer " kube-token)))})
                (fn [resp]
                  (let [body (bs/to-string (:body resp))
                        paths (get (json/read-str body) "paths")]
                    (into {}
                          (comp
                           (filter (fn [[k v]]
                                     (and (re-matches #".*watch.*" k)
                                          (re-matches #".*namespace.*" k)
                                          (not (re-matches #".*\{name\}.*" k)))))
                           (map (fn [[k v]]
                                  [(last (str/split k #"/"))
                                   (str/replace k #"/watch" "")])))
                          paths))))
      (md/catch Exception
          (fn [e]
            (throw (ex-info "Could not connect to Kubernetes API server swagger server"
                            {:status 404
                             :msg (.getMessage e)}))))))

(defn path-pattern
  "Return a resource path URI for a resource, pre-templated with `{namespace}`"
  [resource-map resource]
  (if-let [resource-path (get-in resource-map [resource])]
    resource-path
    (throw (ex-info (format "Resource path %s not found." resource) {}))))

(defn path-pattern-one
  "Return a resource path URI for a specific resource, pre-templated with `{namespace}` and `{name}`"
  [resource-map resource]
  (str (path-pattern resource-map resource) "/{name}"))
