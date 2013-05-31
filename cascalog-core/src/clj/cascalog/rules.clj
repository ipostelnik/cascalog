(ns cascalog.rules
  (:use [cascalog.debug :only (debug-print)]
        [clojure.set :only (intersection union difference subset?)]
        [clojure.walk :only (postwalk)]
        [jackknife.core :only (throw-illegal throw-runtime)])
  (:require [jackknife.seq :as s]
            [cascalog.vars :as v]
            [cascalog.util :as u]
            [cascalog.options :as opts]
            [cascalog.graph :as g]
            [cascalog.predicate :as p]
            [hadoop-util.core :as hadoop]
            [cascalog.fluent.conf :as conf]
            [cascalog.fluent.tap :as tap]
            [cascalog.fluent.workflow :as w])
  (:import [cascading.tap Tap]
           [cascading.tuple Fields Tuple TupleEntry]
           [cascading.flow Flow FlowConnector]
           [cascading.pipe Pipe]
           [cascading.flow.hadoop HadoopFlowProcess]
           [cascading.pipe.joiner CascalogJoiner CascalogJoiner$JoinType]
           [cascalog.aggregator CombinerSpec]
           [cascalog ClojureCombiner ClojureCombinedAggregator Util
            ClojureParallelAgg]
           [org.apache.hadoop.mapred JobConf]
           [jcascalog Predicate Subquery PredicateMacro ClojureOp
            PredicateMacroTemplate]
           [java.util ArrayList]))

;; infields for a join are the names of the join fields
(p/defpredicate join :infields)
(p/defpredicate group
  :assembly
  :infields
  :totaloutfields)

(defn- find-generator-join-set-vars [node]
  (let [predicate     (g/get-value node)
        inbound-nodes (g/get-inbound-nodes node)
        pred-type     (:type predicate)]
    (condp = pred-type
      :join       nil
      :group      nil
      :generator (if-let [v (:join-set-var predicate)] [v])
      (if (= 1 (count inbound-nodes))
        (recur (first inbound-nodes))
        (throw-runtime "Planner exception: Unexpected number of "
                       "inbound nodes to non-generator predicate.")))))

(defstruct tailstruct
  :ground?
  :operations
  :drift-map
  :available-fields
  :node)

(defn- connect-op [tail op]
  (let [new-node (g/connect-value (:node tail)
                                  op)
        new-outfields (concat (:available-fields tail)
                              (:outfields op))]
    (struct tailstruct
            (:ground? tail)
            (:operations tail)
            (:drift-map tail)
            new-outfields new-node)))

(defn- add-op [tail op]
  (debug-print "Adding op to tail " op tail)
  (let [tail (connect-op tail op)
        new-ops (s/remove-first (partial = op) (:operations tail))]
    (merge tail {:operations new-ops})))

(defn- op-allowed? [tail op]
  (let [ground? (:ground? tail)
        available-fields (:available-fields tail)
        join-set-vars (find-generator-join-set-vars (:node tail))
        infields-set (set (:infields op))]
    (and (or (:allow-on-genfilter? op)
             (empty? join-set-vars))
         (subset? infields-set (set available-fields))
         (or ground? (every? v/ground-var? infields-set)))))

(defn- fixed-point-operations
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
  (if-let [op (s/find-first (partial op-allowed? tail)
                            (:operations tail))]
    (recur (add-op tail op))
    tail))

;; TODO: refactor and simplify drift algorithm
(defn- add-drift-op
  [tail equality-sets rename-map new-drift-map]
  (let [eq-assemblies (map w/equal equality-sets)
        outfields (vec (keys rename-map))
        rename-in (vec (vals rename-map))
        rename-assembly (if (seq rename-in)
                          (w/identity rename-in :fn> outfields :> Fields/SWAP)
                          identity)
        assembly   (apply w/compose-straight-assemblies
                          (concat eq-assemblies [rename-assembly]))
        infields (vec (apply concat rename-in equality-sets))
        tail (connect-op tail (p/predicate p/operation
                                           assembly
                                           infields
                                           outfields
                                           false))
        newout (difference (set (:available-fields tail))
                           (set rename-in))]
    (merge tail {:drift-map new-drift-map
                 :available-fields newout} )))

(defn- determine-drift
  [drift-map available-fields]
  (let [available-set (set available-fields)
        rename-map (reduce (fn [m f]
                             (let [drift (drift-map f)]
                               (if (and drift (not (available-set drift)))
                                 (assoc m drift f)
                                 m)))
                           {} available-fields)
        eqmap (select-keys (u/reverse-map (select-keys drift-map available-fields))
                           available-fields)
        equality-sets (map (fn [[k v]] (conj v k)) eqmap)
        new-drift-map (->> equality-sets
                           (apply concat (vals rename-map))
                           (apply dissoc drift-map))]
    [new-drift-map equality-sets rename-map]))

(defn- add-ops-fixed-point
  "Adds operations to tail until can't anymore. Returns new tail"
  [tail]
  (let [{:keys [drift-map available-fields] :as tail} (fixed-point-operations tail)
        [new-drift-map equality-sets rename-map]
        (determine-drift drift-map available-fields)]
    (if (and (empty? equality-sets)
             (empty? rename-map))
      tail
      (recur (add-drift-op tail equality-sets rename-map new-drift-map)))))

(defn- tail-fields-intersection [& tails]
  (->> tails
       (map #(set (:available-fields %)))
       (apply intersection)))

(defn- joinable? [joinfields tail]
  (let [join-set (set joinfields)
        tailfields (set (:available-fields tail))]
    (and (subset? join-set tailfields)
         (or (:ground? tail)
             (every? v/unground-var? (difference tailfields join-set))))))

(defn- find-join-fields [tail1 tail2]
  (let [join-set (tail-fields-intersection tail1 tail2)]
    (when (every? (partial joinable? join-set) [tail1 tail2])
      join-set)))

(defn- select-join
  "Splits tails into [join-fields {join set} {rest of tails}] This is
   unoptimal. It's better to rewrite this as a search problem to find
   optimal joins"
  [tails]
  (let [max-join (->> (u/all-pairs tails)
                      (map (fn [[t1 t2]]
                             (or (find-join-fields t1 t2) [])))
                      (sort-by count)
                      (last))]
    (if (empty? max-join)
      (throw-illegal "Unable to join predicates together")
      (cons (vec max-join)
            (s/separate (partial joinable? max-join) tails)))))

(defn- intersect-drift-maps
  [drift-maps]
  (let [tokeep (->> drift-maps
                    (map #(set (seq %)))
                    (apply intersection))]
    (u/pairs->map (seq tokeep))))

(defn- select-selector [seq1 selector]
  (mapcat (fn [o b] (if b [o])) seq1 selector))

(defn- merge-tails
  [graph tails]
  (let [tails (map add-ops-fixed-point tails)]
    (if (= 1 (count tails))
      ;; if still unground, allow operations to be applied
      (add-ops-fixed-point (merge (first tails) {:ground? true}))
      (let [[join-fields join-set rest-tails] (select-join tails)
            join-node             (g/create-node graph (p/predicate join join-fields))
            join-set-vars    (map find-generator-join-set-vars (map :node join-set))
            available-fields (vec (set (apply concat
                                              (cons (apply concat join-set-vars)
                                                    (select-selector
                                                     (map :available-fields join-set)
                                                     (map not join-set-vars))))))
            new-ops (vec (apply intersection (map #(set (:operations %)) join-set)))
            new-drift-map    (intersect-drift-maps (map :drift-map join-set))]
        (debug-print "Selected join" join-fields join-set)
        (debug-print "Join-set-vars" join-set-vars)
        (debug-print "Available fields" available-fields)
        (dorun (map #(g/create-edge (:node %) join-node) join-set))
        (recur graph (cons (struct tailstruct
                                   (s/some? :ground? join-set)
                                   new-ops
                                   new-drift-map
                                   available-fields
                                   join-node)
                           rest-tails))))))

;; ## Aggregation Operations
;;
;; The following operations deal with Cascalog's aggregations. I think
;; we can replace all of this by delegating out to the GroupBy
;; implementation in operations.

(letfn [
        ;; Returns the union of all grouping fields and all outputs
        ;; for every aggregation field. These are the only fields
        ;; available after the aggregation.
        (agg-available-fields
          [grouping-fields aggs]
          (->> aggs
               (map #(set (:outfields %)))
               (apply union)
               (union (set grouping-fields))
               (vec)))

        ;; These are the operations that go into the
        ;; aggregators.
        (agg-infields [sort-fields aggs]
          (->> aggs
               (map #(set (:infields %)))
               (apply union (set sort-fields))
               (vec)))

        ;; Returns [new-grouping-fields inserter-assembly]. If no
        ;; grouping fields are supplied, ths function groups on 1,
        ;; which forces a global grouping.
        (normalize-grouping
          [grouping-fields]
          (if (seq grouping-fields)
            [grouping-fields identity]
            (let [newvar (v/gen-nullable-var)]
              [[newvar] (w/insert newvar 1)])))

        (mk-combined-aggregator [pagg argfields outfields]
          (w/raw-every (w/fields argfields)
                       (ClojureCombinedAggregator. (w/fields outfields) pagg)
                       Fields/ALL))

        (mk-agg-arg-fields [fields]
          (when (seq fields)
            (w/fields fields)))

        (mk-parallel-aggregator [grouping-fields aggs]
          (let [argfields  (map #(mk-agg-arg-fields (:infields %)) aggs)
                tempfields (map #(v/gen-nullable-vars (count (:outfields %))) aggs)
                specs      (map :parallel-agg aggs)
                combiner (ClojureCombiner. (w/fields grouping-fields)
                                           argfields
                                           (w/fields (apply concat tempfields))
                                           specs)]
            [[(w/raw-each Fields/ALL combiner Fields/RESULTS)]
             (map mk-combined-aggregator specs tempfields (map :outfields aggs))]))

        (mk-parallel-buffer-agg [grouping-fields agg]
          [[((:parallel-agg agg) grouping-fields)]
           [(:serial-agg-assembly agg)]])

        ;; returns [pregroup vec, postgroup vec]
        (build-agg-assemblies
          [grouping-fields aggs]
          (cond (and (= 1 (count aggs))
                     (:parallel-agg (first aggs))
                     (:buffer? (first aggs)))
                (mk-parallel-buffer-agg grouping-fields
                                        (first aggs))

                (every? :parallel-agg aggs)
                (mk-parallel-aggregator grouping-fields aggs)

                :else [[identity] (map :serial-agg-assembly aggs)]))

        ;; Create a groupby operation, respecting grouping and sorting
        ;; options.

        (mk-group-by
          [grouping-fields options]
          (let [{s :sort rev :reverse} options]
            (if (seq s)
              (w/group-by grouping-fields s rev)
              (w/group-by grouping-fields))))]

  ;; Note that the final group that pops out of this thing is keeping
  ;; track of the input fields to each of the aggregators as well as
  ;; the total number of available output fields. These include the
  ;; fields generated by the aggregations, as well as the fields used
  ;; for grouping.
  (defn- build-agg-tail [options prev-tail grouping-fields aggs]
    (debug-print "Adding aggregators to tail" options prev-tail grouping-fields aggs)
    (when (and (empty? aggs) (:sort options))
      (throw-illegal "Cannot specify a sort when there are no aggregators"))
    (if (and (not (:distinct options)) (empty? aggs))
      prev-tail
      (let [aggs (or (not-empty aggs) [p/distinct-aggregator])
            [grouping-fields inserter] (normalize-grouping grouping-fields)
            [prep-aggs postgroup-aggs] (build-agg-assemblies grouping-fields aggs)
            assem (apply w/compose-straight-assemblies
                         (concat [inserter]
                                 (map :pregroup-assembly aggs)
                                 prep-aggs
                                 [(mk-group-by grouping-fields options)]
                                 postgroup-aggs
                                 (map :post-assembly aggs)))
            total-fields (agg-available-fields grouping-fields aggs)
            all-agg-infields  (agg-infields (:sort options) aggs)
            prev-node (:node prev-tail)
            node (g/create-node (g/get-graph prev-node)
                                (p/predicate group assem
                                             all-agg-infields
                                             total-fields))]
        (g/create-edge prev-node node)
        (struct tailstruct
                (:ground? prev-tail)
                (:operations prev-tail)
                (:drift-map prev-tail)
                total-fields
                node)))))

(defn projection-fields [needed-vars allfields]
  (let [needed-set (set needed-vars)
        all-set    (set allfields)
        inter      (intersection needed-set all-set)]
    (cond
     ;; maintain ordering when =, this is for output of generators to
     ;; match declared ordering
     (= inter needed-set) needed-vars

     ;; this happens during global aggregation, need to keep one field
     ;; in
     (empty? inter) [(first allfields)]
     :else (vec inter))))

(defmulti node->generator (fn [pred & _] (:type pred)))

(defmethod node->generator :generator [pred prevgens]
  (when (not-empty prevgens)
    (throw (RuntimeException. "Planner exception: Generator has inbound nodes")))
  pred)

(defn join-fields-selector [num-fields]
  (cascalog.fluent.def/mapfn [& args]
    (let [joins (partition num-fields args)]
      (or (s/find-first (partial s/some? (complement nil?)) joins)
          (repeat num-fields nil)))))

(w/defmapop truthy? [arg]
  (if arg true false))

(defn- replace-join-fields [join-fields join-renames fields]
  (let [replace-map (zipmap join-fields join-renames)]
    (reduce (fn [ret f]
              (let [newf (replace-map f)
                    newf (if newf newf f)]
                (conj ret newf)))
            [] fields)))

(defn- generate-join-fields [numfields numpipes]
  (take numpipes (repeatedly (partial v/gen-nullable-vars numfields))))

(defn- new-pipe-name [prevgens]
  (.getName (:pipe (first prevgens))))

(defn- gen-join-type [gen]
  (cond (:join-set-var gen) :outerone
        (:ground? gen)      :inner
        :else               :outer))

;; split into inner-gens outer-gens join-set-gens
(defmethod node->generator :join [pred prevgens]
  (debug-print "Creating join" pred)
  (debug-print "Joining" prevgens)
  (let [join-fields (:infields pred)
        num-join-fields (count join-fields)
        sourcemap   (apply merge (map :sourcemap prevgens))
        trapmap     (apply merge (map :trapmap prevgens))
        {inner-gens :inner
         outer-gens :outer
         outerone-gens :outerone} (group-by gen-join-type prevgens)
        join-set-fields (map :join-set-var outerone-gens)
        prevgens    (concat inner-gens outer-gens outerone-gens) ; put them in order
        infields    (map :outfields prevgens)
        inpipes     (map (fn [p f] (w/assemble p (w/select f) (w/pipe-rename (u/uuid))))
                         (map :pipe prevgens)
                         infields)      ; is this necessary?
        join-renames (generate-join-fields num-join-fields (count prevgens))
        rename-fields (flatten (map (partial replace-join-fields join-fields) join-renames infields))
        keep-fields (vec (set (apply concat
                                     (cons join-set-fields
                                           (map :outfields
                                                (concat inner-gens outer-gens))))))
        cascalogjoin (concat (repeat (count inner-gens) CascalogJoiner$JoinType/INNER)
                             (repeat (count outer-gens) CascalogJoiner$JoinType/OUTER)
                             (repeat (count outerone-gens) CascalogJoiner$JoinType/EXISTS))
        joined      (apply w/assemble inpipes
                           (concat
                            [(w/co-group (repeat (count inpipes) join-fields)
                                         rename-fields (CascalogJoiner. cascalogjoin))]
                            (mapcat
                             (fn [gen joinfields]
                               (if-let [join-set-var (:join-set-var gen)]
                                 [(truthy? (take 1 joinfields) :fn> [join-set-var] :> Fields/ALL)]))
                             prevgens join-renames)
                            [(join-fields-selector [num-join-fields] (flatten join-renames) :fn> join-fields :> Fields/SWAP)
                             (w/select keep-fields)
                             ;; maintain the pipe name (important for setting traps on subqueries)
                             (w/pipe-rename (new-pipe-name prevgens))]))]
    (p/predicate p/generator nil true sourcemap joined keep-fields trapmap)))

(defmethod node->generator :operation [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: operation has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (concat (:outfields pred) (:outfields prevpred))
                     :pipe ((:assembly pred) (:pipe prevpred))})))

;; Look at the previous generators -- we should only have a single
;; incoming generator in this case, since we're pushing into a single
;; grouping. The output fields that are remaining need to follow this
;; damned logic.
;;
;; Basically, Nathan is implementing this ad-hoc field algebra...
(defmethod node->generator :group [pred prevgens]
  (when-not (= 1 (count prevgens))
    (throw (RuntimeException. "Planner exception: group has multiple inbound generators")))
  (let [prevpred (first prevgens)]
    (merge prevpred {:outfields (:totaloutfields pred)
                     :pipe ((:assembly pred) (:pipe prevpred))})))

;; forceproject necessary b/c *must* reorder last set of fields coming
;; out to match declared ordering

;; So, it looks like this function builds up a generator of tuples,
;; which in Nathan's world is a pipe, some output fields and the
;; remaining information for the FlowDef.
;;
;; Other than the Fields algebra, I think I've got it.

(comment
  "This is what it comes down to. We want to convert nodes to
  generators that can be compiled down into the FlowDef
  representation. This'll be accomplished by keeping around a graph
  with a bunch of nodes, and resolving the input generators down."

  "Nodes are generator, join, operation, group."
  (defpredicate generator
    :join-set-var
    :ground?
    :sourcemap
    :pipe
    :outfields
    :trapmap))

;; This projection assembly checks every output field against the
;; existing "allfields" deal. If the fields match up and no project is
;; being forced, then the function goes ahead and passes identity back
;; out.
;;
;; The final compilation step (the subquery step) requires a forced
;; projection, since a subquery declares its output variables.

(letfn [(mk-projection-assembly
          [forceproject projection-fields allfields]
          (if (and (not forceproject)
                   (= (set projection-fields)
                      (set allfields)))
            identity
            (w/select projection-fields)))]

  (defn build-generator [forceproject needed-vars node]
    (let [
          ;; Get the current operation at this node.
          pred           (g/get-value node)

          ;; The input had some required variables -- I've got to get
          ;; my required variables and concatenate them onto the set
          ;; of required variables.
          my-needed      (vec (set (concat (:infields pred) needed-vars)))

          ;; Now, go ahead and build up generators for every input
          ;; node in the graph. This will recursively walk backward
          ;; along the graph until it finds the root generators, I
          ;; think.
          prev-gens      (doall (map (partial build-generator false my-needed)
                                     (g/get-inbound-nodes node)))

          ;;
          newgen         (node->generator pred prev-gens)
          project-fields (projection-fields needed-vars (:outfields newgen)) ]
      (debug-print "build gen:" my-needed project-fields pred)
      (if (and forceproject (not= project-fields needed-vars))
        (throw-runtime (format "Only able to build to %s but need %s. Missing %s"
                               project-fields
                               needed-vars
                               (vec (clojure.set/difference (set needed-vars)
                                                            (set project-fields)))))
        (merge newgen
               {:pipe ((mk-projection-assembly forceproject
                                               project-fields
                                               (:outfields newgen))
                       (:pipe newgen))
                :outfields project-fields})))))


;; This is my hook into the var unique thing. Figure out what this
;; does.
(defn- mk-var-uniquer-reducer [out?]
  (fn [[preds vmap] {:keys [op invars outvars]}]
    (let [[updatevars vmap] (v/uniquify-vars (if out? outvars invars)
                                             vmap
                                             :force-unique? out?)
          [invars outvars] (if out?
                             [invars updatevars]
                             [updatevars outvars])]
      [(conj preds {:op op
                    :invars invars
                    :outvars outvars})
       vmap])))

(defn mk-drift-map [vmap]
  (let [update-fn (fn [m [_ vals]]
                    (let [target (first vals)]
                      (prn m target vals)
                      (reduce #(assoc %1 %2 target) m (rest vals))))]
    (reduce update-fn {} (seq vmap))))

;; TODO: Take a look at this thing when the actual job is getting
;; compiled and figure out what this mk-var-uniquer-reducer is
;; actually doing.

(defn- uniquify-query-vars
  "TODO: this won't handle drift for generator filter outvars should
  fix this by rewriting and simplifying how drift implementation
  works."
  [out-vars raw-predicates]
  (let [[raw-predicates vmap] (reduce (mk-var-uniquer-reducer true) [[] {}] raw-predicates)
        [raw-predicates vmap] (reduce (mk-var-uniquer-reducer false) [[] vmap] raw-predicates)
        [out-vars vmap]       (v/uniquify-vars out-vars vmap)
        drift-map             (mk-drift-map vmap)]
    [out-vars raw-predicates drift-map]))

;; This thing handles the logic that the output of an operation, if
;; marked as a constant, should be a filter.
;;
;; This gets called twice, since we might have some new output
;; variables after converting generator as set logic.
(defn split-outvar-constants
  [{:keys [op invars outvars] :as m}]
  (let [[new-outvars newpreds] (reduce
                                (fn [[outvars preds] v]
                                  (if (v/cascalog-var? v)
                                    [(conj outvars v) preds]
                                    (let [newvar (v/gen-nullable-var)]
                                      [(conj outvars newvar)
                                       (conj preds {:op (p/predicate
                                                         p/outconstant-equal)
                                                    :invars [v newvar]
                                                    :outvars []})])))
                                [[] []]
                                outvars)]
    (-> (assoc m :outvars new-outvars)
        (cons newpreds))))

;; Handles the output for generator as set.
(defn- rewrite-predicate [{:keys [op invars outvars] :as predicate}]
  (if-not (and (p/generator? op) (seq invars))
    predicate
    (if (= 1 (count outvars))
      {:op (p/predicate p/generator-filter op (first outvars))
       :invars []
       :outvars invars}
      (throw-illegal (str "Generator filter can only have one outvar -> "
                          outvars)))))

(defn- parse-predicate [[op vars]]
  (let [{invars :<< outvars :>>}
        (p/parse-variables vars (p/predicate-default-var op))]
    {:op op
     :invars invars
     :outvars outvars}))

;; ## Gen-As-Set Validation

(defn- unzip-generators
  "Returns a vector containing two sequences; the subset of the
  supplied sequence of parsed-predicates identified as generators, and
  the rest."
  [parsed-preds]
  (s/separate (comp p/generator? :op) parsed-preds))

(defn gen-as-set?
  "Returns true if the supplied parsed predicate is a generator meant
  to be used as a set, false otherwise."
  [parsed-pred]
  (and (p/generator? (:op parsed-pred))
       (not-empty    (:invars parsed-pred))))

(defn- gen-as-set-ungrounding-vars
  "Returns a sequence of ungrounding vars present in the
  generators-as-sets contained within the supplied sequence of parsed
  predicates (maps with keys :op, :invars, :outvars)."
  [parsed-preds]
  (mapcat (comp (partial filter v/unground-var?)
                #(mapcat % [:invars :outvars]))
          (filter gen-as-set? parsed-preds)))

(defn- parse-ungrounding-outvars
  "For the supplied sequence of parsed cascalog predicates of the form
  `[op hof-args invars outvars]`, returns a vector of two
  entries: a sequence of all output ungrounding vars that appear
  within generator predicates, and a sequence of all ungrounding vars
  that appear within non-generator predicates."
  [parsed-preds]
  (map (comp (partial mapcat #(filter v/unground-var? %))
             (partial map :outvars))
       (unzip-generators parsed-preds)))

(defn- pred-clean!
  "Performs various validations on the supplied set of parsed
  predicates. If all validations pass, returns the sequence
  unchanged."
  [parsed-preds]
  (let [gen-as-set-vars (gen-as-set-ungrounding-vars parsed-preds)
        [gen-outvars pred-outvars] (parse-ungrounding-outvars parsed-preds)
        extra-vars  (vec (difference (set pred-outvars)
                                     (set gen-outvars)))
        dups (s/duplicates gen-outvars)]
    (cond
     (not-empty gen-as-set-vars)
     (throw-illegal (str "Can't have unground vars in generators-as-sets."
                         (vec gen-as-set-vars)
                         " violate(s) the rules.\n\n" (pr-str parsed-preds)))

     (not-empty extra-vars)
     (throw-illegal (str "Ungrounding vars must originate within a generator. "
                         extra-vars
                         " violate(s) the rules."))

     (not-empty dups)
     (throw-illegal (str "Each ungrounding var can only appear once per query."
                         "The following are duplicated: "
                         dups))
     :else parsed-preds)))

;; ## Query Building

(defn- split-predicates
  "returns [generators operations aggregators]."
  [predicates]
  (let [{ops :operation aggs :aggregator gens :generator}
        (merge {:operation [] :aggregator [] :generator []}
               (group-by :type predicates))]
    (when (and (> (count aggs) 1)
               (some :buffer? aggs))
      (throw-illegal "Cannot use both aggregators and buffers in same grouping"))
    [gens ops aggs]))

(defn- build-query
  "This is the big mother. This is where the query gets built and
  where I'll be spending some good time."
  [out-vars raw-predicates]
  (debug-print "outvars:" out-vars)
  (debug-print "raw predicates:" raw-predicates)
  (let [
        ;; Parse the options out of the predicates,
        [raw-opts raw-predicates] (s/separate (comp keyword? first) raw-predicates)

        ;; and assemble the option map.
        option-m (opts/generate-option-map raw-opts)

        ;; rewrite all predicates and assemble the true set of output
        ;; variables.
        [out-vars raw-predicates drift-map] (->> raw-predicates
                                                 (map parse-predicate)
                                                 (pred-clean!)
                                                 (mapcat split-outvar-constants)
                                                 (map rewrite-predicate)
                                                 (mapcat split-outvar-constants)
                                                 (uniquify-query-vars out-vars))

        ;; Split up generators, operations and aggregators.
        [gens ops aggs] (->> raw-predicates
                             (map (partial apply p/build-predicate option-m))
                             (split-predicates))

        rule-graph (g/mk-graph)
        joined (->> gens
                    (map (fn [{:keys [ground? outfields] :as g}]
                           (struct tailstruct
                                   ground?
                                   ops
                                   drift-map
                                   outfields
                                   (g/create-node rule-graph g))))
                    (merge-tails rule-graph))
        ;; Group by the pre-aggregation variables that are in the
        ;; result vector.
        grouping-fields (seq (intersection (set (:available-fields joined))
                                           (set out-vars)))
        agg-tail (build-agg-tail option-m joined grouping-fields aggs)
        {:keys [operations node]} (add-ops-fixed-point agg-tail)]
    (if (not-empty operations)
      (throw-runtime "Could not apply all operations: " (pr-str operations))
      (build-generator true out-vars node))))

(defn- new-var-name! [replacements v]
  (let [new-name  (if (contains? @replacements v)
                    (@replacements v)
                    (v/uniquify-var v))]
    (swap! replacements assoc v new-name)
    new-name))

(defn- pred-macro-updater [[replacements ret] [op vars]]
  (let [vars (vec vars) ; in case it's a java data structure
        newvars (postwalk #(if (v/cascalog-var? %)
                             (new-var-name! replacements %)
                             %)
                          vars)]
    [replacements (conj ret [op newvars])]))

(defn- build-predicate-macro-fn
  [invars-decl outvars-decl raw-predicates]
  (when (seq (intersection (set invars-decl)
                           (set outvars-decl)))
    (throw-runtime
     "Cannot declare the same var as an input and output to predicate macro: "
     invars-decl
     " "
     outvars-decl))
  (fn [invars outvars]
    (let [outvars (if (and (empty? outvars)
                           (sequential? outvars-decl)
                           (= 1 (count outvars-decl)))
                    [true]
                    outvars)
          ;; kind of a hack, simulate using pred macros like filters
          replacements (atom (u/mk-destructured-seq-map invars-decl
                                                        invars
                                                        outvars-decl
                                                        outvars))]
      (second (reduce pred-macro-updater
                      [replacements []]
                      raw-predicates)))))

(defn- build-predicate-macro [invars outvars raw-predicates]
  (->> (build-predicate-macro-fn invars outvars raw-predicates)
       (p/predicate p/predicate-macro)))

(defn- to-jcascalog-fields [fields]
  (jcascalog.Fields. (or fields [])))

;; ## Predicate Macro Expansion
;;
;; This section deals with predicate macro expansion.

(defn- expand-predicate-macro
  [p vars]
  (let [{invars :<< outvars :>>} (p/parse-variables vars :<)]
    (cond (var? p) [[(var-get p) vars]]
          (instance? Subquery p) [[(.getCompiledSubquery p) vars]]
          (instance? ClojureOp p) [(.toRawCascalogPredicate p vars)]
          (instance? PredicateMacro p) (-> p (.getPredicates
                                              (to-jcascalog-fields invars)
                                              (to-jcascalog-fields outvars)))
          (instance? PredicateMacroTemplate p) [[(.getCompiledPredMacro p) vars]]
          :else ((:pred-fn p) invars outvars))))

(defn- expand-predicate-macros
  "Returns a sequence of predicates with all internal predicate macros
  expanded."
  [raw-predicates]
  (mapcat (fn [[p vars :as raw-pred]]
            (if (p/predicate-macro? p)
              (expand-predicate-macros (expand-predicate-macro p vars))
              [raw-pred]))
          raw-predicates))

;; ## Query Compilation
;;
;; This is the entry point from "construct".
;;
;; Before compilation, all predicates are normalized down to clojrue
;; predicates. I'm not sure I agree with this, as we lose type
;; information that would be valuable on the Java side.
;;
;; Query compilation steps are as follows:
;;
;; 1. Normalize all predicates
;; 2. Expand predicate macros
;; 3. Desugar all of the argument selectors (remember positional!)

(defn normalize
  "Returns a predicate of the form [op [infields, link,
  outfields]]. For example,

   [* [\"?x\" \"?x\" :> \"?y\"]]"
  [pred]
  (if (instance? Predicate pred)
    (.toRawCascalogPredicate pred)
    pred))

(defn query-signature?
  "Accepts the normalized return vector of a Cascalog form and returns
  true if the return vector is from a subquery, false otherwise. (A
  predicate macro would trigger false, for example.)"
  [vars]
  (not (some v/cascalog-keyword? vars)))

;; This is the main entry point from api.clj. What's the significance
;; of a query vs a predicate macro? The main thing in the current API
;; is that a query triggers an actual grouping, and compiles down to
;; another generator with a pipe, etc, while a predicate macro is just
;; a collection of predicates that expands out within another
;; subquery.

(defn build-rule
  "This is the entry point into the rules of the system. output
  variables and raw predicates come in, predicate macros are expanded,
  and the query (or another predicate macro) is compiled."
  [out-vars raw-predicates]
  (let [predicates (->> raw-predicates
                        (map normalize)
                        expand-predicate-macros)]
    (if (query-signature? out-vars)
      (build-query out-vars predicates)
      (let [parsed (p/parse-variables out-vars :<)]
        (build-predicate-macro (parsed :<<)
                               (parsed :>>)
                               predicates)))))

(defn mk-raw-predicate
  "Receives a cascalog predicate of the form [op <any other vars>] and
  sanitizes the reserved cascalog variables by converting symbols into
  strings."
  [[op-sym & vars]]
  [op-sym (v/sanitize vars)])

(defn enforce-gen-schema
  "Accepts a cascalog generator; if `g` is well-formed, acts as
`identity`. If the supplied generator is a vector, list, or a straight
cascading tap, returns a new generator with field-names."
  [g]
  (cond (= (:type g) :cascalog-tap)
        (enforce-gen-schema (:source g))

        (or (instance? Tap g)
            (vector? g)
            (list? g))
        (let [pluck (if (instance? Tap g) tap/pluck-tuple, first)
              size  (count (pluck g))
              vars  (v/gen-nullable-vars size)]
          (if (zero? size)
            (throw-illegal
             "Data structure is empty -- memory sources must contain tuples.")
            (->> [[g :>> vars] [:distinct false]]
                 (map mk-raw-predicate)
                 (build-rule vars))))
        :else g))

(defn connect-to-sink
  [gen sink]
  ((w/pipe-rename (u/uuid)) (:pipe gen)))

(defn normalize-gen [gen]
  (if (instance? Subquery gen)
    (.getCompiledSubquery gen)
    gen))

(defn combine* [gens distinct?]
  ;; it would be nice if cascalog supported Fields/UNKNOWN as output of generator
  (let [gens (map (comp enforce-gen-schema normalize-gen) gens)
        outfields (:outfields (first gens))
        pipes (map :pipe gens)
        pipes (for [p pipes]
                (w/assemble p (w/identity Fields/ALL
                                          :fn>
                                          outfields
                                          :>
                                          Fields/RESULTS)))
        outpipe (if-not distinct?
                  (w/assemble pipes (w/group-by Fields/ALL))
                  (w/assemble pipes (w/group-by Fields/ALL) (w/first)))]
    (p/predicate p/generator
                 nil
                 true
                 (apply merge (map :sourcemap gens))
                 outpipe
                 outfields
                 (apply merge (map :trapmap gens)))))

(defn generator-selector [gen & _]
  (cond (instance? Tap gen) :tap
        (instance? Subquery gen) :java-subquery
        :else (:type gen)))

(defn normalize-sink-connection [sink subquery]
  (cond (fn? sink)  (sink subquery)
        (map? sink) (normalize-sink-connection (:sink sink) subquery)
        :else       [sink subquery]))
