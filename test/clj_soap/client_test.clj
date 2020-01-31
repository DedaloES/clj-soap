(ns clj-soap.client-test
  (:require [clj-soap.client :as client]
            [clojure.test :refer [deftest testing is]]))

(deftest client-fn []
  (testing "Build a client function from a local WSDL resource"
    (is (client/client-fn {:wsdl "file:test/resources/hello.wsdl" :options {}}))))
