(ns clj-soap.client
  (:require [clojure.tools.logging :as log]
            [clojure.data.xml :as xml]
            [clojure.data.xml.tree :as xmlt]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.data.xml.jvm.pprint :as xmljprn]
            [clojure.data.xml.jvm.parse :as xmljprs])
  (:import [org.apache.axis2.client ServiceClient
            Options]
           [org.apache.axis2.addressing EndpointReference]
           [org.apache.axis2.transport.http HTTPConstants
            HTTPAuthenticator]
           [org.apache.axis2.transport.http.impl.httpclient3
            HttpTransportPropertiesImpl$Authenticator]
           [org.apache.axiom.om OMAbstractFactory]
           [org.apache.axis2.description OutOnlyAxisOperation
            AxisService]
           [javax.xml.namespace QName]
           [javax.xml.transform.stream StreamResult]
           [java.net URL
            Authenticator
            PasswordAuthentication]
           [java.io StringWriter Writer]))


(defn axis-indent-xml [om-element ^Writer writer]
  (.transform (xmljprn/indenting-transformer)
              (.getSAXSource om-element true)
              (StreamResult. writer)))

(defn axis-indent-xml-str [om-element]
  (let [sw (StringWriter.)]
    (axis-indent-xml om-element sw)
    (str sw)))

(defn axis-event-seq
  [sreader opts]
  (let [props* (merge {:include-node? #{:element :characters}
                       :coalescing true
                       :supporting-external-entities false
                       :location-info true}
                      opts)]
    (xmljprs/pull-seq sreader props* nil)))

(defn axis-parse [om-container & {:as opts}]
  (let [sreader (.getXMLStreamReader om-container)]
    (xmlt/event-tree (axis-event-seq sreader opts))))

(defn axis-op-name
  [axis-op]
  (.getLocalPart (.getName axis-op)))

(defn axis-op-namespace
  [axis-op]
  (.getNamespaceURI (.getName axis-op)))

(defn axis-complex-element? [element]
  (instance? org.apache.ws.commons.schema.XmlSchemaComplexType
             (.getSchemaType element)))

(defn axis-simple-element? [element]
  (instance? org.apache.ws.commons.schema.XmlSchemaSimpleType
             (.getSchemaType element)))

(defn axis-complex-element-types [schema-element]
  (->> schema-element
      .getSchemaType .getParticle .getItems
      (map (fn [elem]
             {:name (.getName elem)
              :type (some-> elem .getSchemaType .getName keyword)
              :elem elem}))))

(defn axis-op-args
  [axis-op]
  (some->> (.getMessages axis-op)
           iterator-seq
           (filter #(= "out" (.getDirection %)))
           first
           .getSchemaElement
           axis-complex-element-types))

(defn axis-op-qname
  [op argtype]
  (QName. (axis-op-namespace op) (:name argtype)))

(defn axis-service-operations
  [axis-service]
  (iterator-seq (.getOperations axis-service)))

(defn axis-service-operation
  [axis-client key]
  (let [axis-service (.getAxisService axis-client)
        op-name (name key)]
    (->> (axis-service-operations axis-service)
         (filter #(= op-name (axis-op-name %)))
         first)))

(defmulti ^{:doc "Build OMElement nodes from data for composing requests"}
  build-om-element (fn [argtype _ _ _] (:type argtype)))

(defmethod ^{:doc ""}
  build-om-element :default [argtype factory parent-qname argval]
  (let [simple? (axis-simple-element? (:elem argtype))
        new-qname (if simple?
                    (QName. (.getNamespaceURI parent-qname)
                            (:name argtype)
                            (.getPrefix parent-qname))
                    (QName. (.getNamespaceURI (.getQName (:elem argtype)))
                            (:name argtype)))
        new-element (.createOMElement factory new-qname)]
    (if simple?
      (if (not (nil? argval)) (.setText new-element (str argval))
          (let [xsi-ns (.createOMNamespace factory
                                           "http://www.w3.org/2001/XMLSchema-instance"
                                           "xsi")
                nil-attr (.createOMAttribute factory "nil" xsi-ns "true")]
            (.addAttribute new-element nil-attr)))

      ;; Complex type, from now on we assume that argval is a seq
      (let [component-types (axis-complex-element-types (:elem argtype))]
        (cond
          ;; Same size: we match all components and types
          (= (count component-types) (count argval))
          (doseq [[cval ctype] (map list argval component-types)]
            (.addChild new-element (build-om-element ctype factory new-qname cval)))

          ;; Different size, if there is only a component it must be an array
          (= (count component-types) 1)
          (let [component-types-list (repeat (count argval) (first component-types))]
            (doseq [[cval ctype] (map list argval component-types-list)]
              (.addChild new-element (build-om-element ctype factory new-qname cval))))

          :otherwise
          (throw (ex-info "Types and arguments mismatch. They should be the same or types count should be 1"
                          {:types component-types
                           :argval argval})))
        ))
    new-element))

(defn make-request
  [op options & args]
  (let [factory (OMAbstractFactory/getOMFactory)
        op-qname (QName. (axis-op-namespace op) (axis-op-name op))
        xsi-ns (.createOMNamespace factory
                                   "http://www.w3.org/2001/XMLSchema-instance"
                                   "xsi")
        request (doto (.createOMElement factory op-qname)
                  (.declareNamespace xsi-ns))
        op-args (axis-op-args op)]
    (doseq [[argval argtype] (map list args op-args)]
      (.addChild request (build-om-element argtype factory op-qname argval)))
    request))

(defn client-call
  [client op options & args]
  (log/trace "Invoking SOAP Operation:" (.getName op))
  (let [request (apply make-request op options args)]
    (log/trace " * Request:\n" (axis-indent-xml-str request))
    (locking client
      (if (isa? (class op) OutOnlyAxisOperation)
        (.sendRobust client (.getName op) request)
        (let [axis-response (.sendReceive client (.getName op) request)]
          (log/trace " * Response:\n" (axis-indent-xml-str axis-response))
          (axis-parse axis-response))))))

(defn client-proxy
  [client options]
  (->> (for [op (axis-service-operations (.getAxisService client))]
         [(keyword (axis-op-name op))
          (fn soap-call [& args] (apply client-call client op options args))])
       (into {})))

(defn make-client
  [url & [{:keys [auth throw-faults timeout chunked? wsdl-auth]
           :or {throw-faults true
                chunked? false}}]]
  (let [options (doto (Options.)
                  (.setTo (EndpointReference. url))
                  (.setProperty HTTPConstants/CHUNKED (str chunked?))
                  (.setExceptionToBeThrownOnSOAPFault throw-faults))]

    ; if WSDL is password-protected, must enable access for URLConnection/connect
    ; which is used internally by Axis2
    (when wsdl-auth
      (let [url-authenticator (proxy [Authenticator] []
                                (getPasswordAuthentication []
                                  (PasswordAuthentication. (:username wsdl-auth)
                                                           (char-array (:password wsdl-auth)))))]
        (Authenticator/setDefault url-authenticator)))

    ; support authentication when making SOAP requests
    (when auth
      (let [req-authenticator (doto (HttpTransportPropertiesImpl$Authenticator.)
                                (.setUsername (:username auth))
                                (.setPassword (:password auth)))]
        (.setProperty options HTTPConstants/AUTHENTICATE req-authenticator)))

    ; enable connection timeouts
    (when timeout
      (.setTimeOutInMilliSeconds options timeout)
      (.setProperty options HTTPConstants/SO_TIMEOUT timeout)
      (.setProperty options HTTPConstants/CONNECTION_TIMEOUT timeout))

    ; ensure all created operation clients also have the same set of options
    (doto (ServiceClient. nil (AxisService/createClientSideAxisService
                                (URL. url) nil nil options))
      (.setOverrideOptions options))))

(defn client-fn
  "Creates SOAP client proxy function which must be invoked with keywordised
  version of the SOAP function and any additional arguments
  e.g. (client :GetData \"test1\" \"test2\").
  A map of options are required for generating the function.
  Either :base-client must be supplied (created with make-client) or the :wsdl
  URL string with :options data."
  [{:keys [wsdl options base-client]}]
  (let [; either base client must be supplied or URL with optional data
        client (or base-client (make-client wsdl options))
        px (client-proxy client options)]
    (fn [opname & args]
      (when-let [operation (px opname)]
        (apply operation args)))))
