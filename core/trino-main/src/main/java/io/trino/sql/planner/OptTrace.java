/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.trino.Session;
import io.trino.cost.CostProvider;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.optimizations.ActualProperties;
import io.trino.sql.planner.optimizations.PreferredProperties;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.Statement;

import javax.annotation.concurrent.Immutable;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OptTrace
{
    File file;
    FileWriter fileWriter;
    BufferedWriter bufferedWriter;
    int indent;
    int incrIndent;
    long uid;
    Lookup lookUp;
    Memo memo;
    Stack<Long> uidStack;
    BasicQueryInfo queryInfo;
    boolean outputQueryInfo;
    HashMap<String, Integer> traceIdMap;
    HashMap<String, Integer> joinIdMap;
    Integer minusOne;
    HashMap<PlanNode, Pair<String, String>> joinStringMap;
    int currentTraceId;
    int currentJoinId;
    ArrayList<JoinConstraintNode> planConstraints;
    ArrayList<JoinNode> enumeratedJoins;
    HashMap<Integer, PruneReason> prunedJoinIds;
    ArrayList<Integer> validUnderConstraintsJoinIds;
    boolean traceIdsAssigned;
    HashMap<String, PreferredProperties> planNodeIdToPreferredPropertiesMap;
    HashMap<String, ActualProperties> planNodeIdToActualPropertiesMap;
    HashMap<PlanNode, String> planNodeToLookUpStringMap;
    BiMap<String, String> locationToTableOrAliasNameMap;
    ArrayList<String> duplicateTableOrAliasNames;
    HashMap<PlanNode, JoinConstraintNode> planNodeToJoinConstraintMap;

    Metadata metadata;
    Session session;
    TypeProvider types;
    SqlParser parser;
    CostProvider costProvider;
    StatsProvider statsProvider;

    public OptTrace(String dirPath, Metadata metadataParam, Session sessionParam, TypeProvider typesParam, SqlParser parserParam, Lookup lookUpParam, Memo memoParam,
            CostProvider costProviderParam,
            StatsProvider statsProviderParam)
    {
        requireNonNull(dirPath, "dirPath is null");

        indent = 0;
        incrIndent = 2;
        metadata = metadataParam;
        session = sessionParam;
        types = typesParam;
        parser = parserParam;
        lookUp = lookUpParam;
        memo = memoParam;
        uid = 0;
        uidStack = new Stack<Long>();
        queryInfo = null;
        outputQueryInfo = true;
        traceIdMap = new HashMap<String, Integer>();
        joinIdMap = new HashMap<String, Integer>();
        minusOne = Integer.valueOf(-1);
        joinStringMap = new HashMap<PlanNode, Pair<String, String>>();
        currentTraceId = 0;
        currentJoinId = 0;
        planConstraints = null;
        enumeratedJoins = new ArrayList<JoinNode>();
        prunedJoinIds = new HashMap<Integer, PruneReason>();
        validUnderConstraintsJoinIds = new ArrayList<Integer>();
        traceIdsAssigned = false;
        planNodeIdToPreferredPropertiesMap = new HashMap<String, PreferredProperties>();
        planNodeIdToActualPropertiesMap = new HashMap<String, ActualProperties>();
        planNodeToLookUpStringMap = new HashMap<PlanNode, String>();
        planNodeToJoinConstraintMap = new HashMap<PlanNode, JoinConstraintNode>();
        locationToTableOrAliasNameMap = HashBiMap.create();
        duplicateTableOrAliasNames = new ArrayList<String>();
        costProvider = costProviderParam;
        statsProvider = statsProviderParam;

        Path path = Paths.get(dirPath);

        if (Files.exists(path)) {
            if (!dirPath.endsWith("/")) {
                dirPath = dirPath + "/";
            }

            String tryFileName = dirPath + "optTrace_0.txt";
            path = Paths.get(tryFileName);

            for (int i = 1; Files.exists(path); ++i) {
                tryFileName = dirPath + "optTrace_" + i + ".txt";
                path = Paths.get(tryFileName);
            }

            file = new File(tryFileName);

            try {
                fileWriter = new FileWriter(file, true);
                bufferedWriter = new BufferedWriter(fileWriter);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Optimizer trace directory does not exist : %s", dirPath));
        }
    }

    public static void begin(Optional<OptTrace> optTraceParam, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.begin(msgString, args));
    }

    public static void addPrunedJoinId(Optional<OptTrace> optTraceParam, Integer joinId, PruneReason reason)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.addPrunedJoinId(joinId, reason));
    }

    public static void addPlanNodeToPreferredPropertiesMapping(Optional<OptTrace> optTraceParam, PlanNode planNode, PreferredProperties preferredProperties)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.addPlanNodeToPreferredPropertiesMapping(planNode, preferredProperties));
    }

    public static void tracePlanNodeStatsEstimate(Optional<OptTrace> optTraceParam, PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        if (optTraceParam.isPresent()) {
            optTraceParam.get().tracePlanNodeStatsEstimate(planNode, indentCnt, msgString, args);
        }
    }

    public static void traceSymbols(Optional<OptTrace> optTraceParam, List<Symbol> varList, int indentCnt,
            String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceSymbols(varList, indentCnt, msgString, args));
    }

    public static void tracePreferredProperties(Optional<OptTrace> optTraceParam, PreferredProperties preferredProperties, PlanNode planNode, int indentCnt,
            String msgString, Object... args)
    {
        if (preferredProperties != null) {
            optTraceParam.ifPresent(optTrace -> optTrace.tracePreferredProperties(preferredProperties, indentCnt, msgString, args));
        }
    }

    public static PruneReason isPruned(Optional<OptTrace> optTraceParam, Integer traceId)
    {
        PruneReason pruned;
        if (optTraceParam.isPresent()) {
            OptTrace optTrace = optTraceParam.get();
            pruned = optTrace.isPruned(traceId);
        }
        else {
            pruned = null;
        }

        return pruned;
    }

    public static void queryInfo(Optional<OptTrace> optTraceParam)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.queryInfo());
    }

    public static void addEnumeratedJoin(Optional<OptTrace> optTraceParam, PlanNode planNode)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.addEnumeratedJoin(planNode));
    }

    public static void traceJoinIdMap(Optional<OptTrace> optTraceParam, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceJoinIdMap(indentCnt, msgString, args));
    }

    public static void trace(Optional<OptTrace> optTraceParam, PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.tracePlanNode(planNode, indentCnt, msgString, args));
    }

    public static void traceEnumeratedJoins(Optional<OptTrace> optTraceParam, PlanNode root, CostProvider costProvider, StatsProvider statsProvider,
            int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceEnumeratedJoins(root, costProvider, statsProvider, indentCnt, msgString, args));
    }

    public static void traceJoinConstraint(Optional<OptTrace> optTraceParam, PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceJoinConstraint(planNode, indentCnt, msgString, args));
    }

    public static void trace(Optional<OptTrace> optTraceParam, PlanCostEstimate planCostEstimate, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.tracePlanCostEstimate(planCostEstimate, indentCnt, msgString, args));
    }

    public static void msg(Optional<OptTrace> optTraceParam, String msgString, boolean eol, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.msg(msgString, eol, args));
    }

    public static void end(Optional<OptTrace> optTraceParam, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.end(msgString, args));
    }

    public static void incrIndent(Optional<OptTrace> optTraceParam, int indentCnt)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.incrIndent(indentCnt));
    }

    public static void decrIndent(Optional<OptTrace> optTraceParam, int indentCnt)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.decrIndent(indentCnt));
    }

    public static void trace(Optional<OptTrace> optTraceParam, Set<Integer> partition, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.tracePartition(partition, msgString, args));
    }

    public static void trace(Optional<OptTrace> optTraceParam, Set<PlanNode> sources, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.traceJoinSources(sources, indentCnt, msgString, args));
    }

    private static PlanNode findRootJoin(PlanNode planNode, Lookup lookUp)
    {
        requireNonNull(planNode, "plan node is null");

        PlanNode rootJoinNode = null;
        if (planNode instanceof JoinNode || planNode instanceof SemiJoinNode) {
            rootJoinNode = planNode;
        }
        else if (planNode instanceof GroupReference) {
            GroupReference groupReference = (GroupReference) planNode;
            Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
            Optional<PlanNode> groupPlanNode = planNodes.findFirst();
            if (groupPlanNode.isPresent()) {
                PlanNode tmpNode = groupPlanNode.get();
                rootJoinNode = findRootJoin(tmpNode, lookUp);
            }
        }
        else {
            for (PlanNode source : planNode.getSources()) {
                PlanNode sourceRootJoinNode = findRootJoin(source, lookUp);
                if (sourceRootJoinNode != null) {
                    if (rootJoinNode != null) {
                        rootJoinNode = null;
                        break;
                    }
                    else {
                        rootJoinNode = sourceRootJoinNode;
                    }
                }
            }
        }

        return rootJoinNode;
    }

    public static JoinConstraintNode joinConstraintNode(Optional<OptTrace> optTraceParam, JoinNode joinNode, boolean ignoreDistributionType)
    {
        JoinConstraintNode joinConstraintNode = null;
        if (optTraceParam.isPresent()) {
            joinConstraintNode = optTraceParam.get().joinConstraintNode(joinNode, null, ignoreDistributionType);
        }

        return joinConstraintNode;
    }

    public static JoinConstraintNode joinConstraintNode(Optional<OptTrace> optTraceParam, SemiJoinNode semiJoinNode, boolean ignoreDistributionType)
    {
        JoinConstraintNode joinConstraintNode = null;
        if (optTraceParam.isPresent()) {
            joinConstraintNode = optTraceParam.get().joinConstraintNode(semiJoinNode, null, ignoreDistributionType);
        }

        return joinConstraintNode;
    }

    public static void assignTraceIds(Optional<OptTrace> optTraceParam, PlanNode node, Statement statement)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.assignTraceIds(node, statement));
    }

    public static void addJoinConstraintNode(Optional<OptTrace> optTraceParam, PlanNode node)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.addJoinConstraintNode(node));
    }

    public static boolean constraintsPresent(Optional<OptTrace> optTraceParam)
    {
        boolean constraintsPresent = false;
        if (optTraceParam.isPresent()) {
            constraintsPresent = (optTraceParam.get().planConstraints != null && optTraceParam.get().planConstraints.size() > 0);
        }

        return constraintsPresent;
    }

    public static boolean joinConstraintsPresent(Optional<OptTrace> optTraceParam)
    {
        boolean joinConstraintsPresent = false;
        if (optTraceParam.isPresent()) {
            if (optTraceParam.get().planConstraints != null && optTraceParam.get().planConstraints.size() > 0) {
                for (JoinConstraintNode constraint : optTraceParam.get().planConstraints) {
                    if (constraint.constraintType() != null && constraint.constraintType() == JoinConstraintNode.ConstraintType.JOIN) {
                        joinConstraintsPresent = true;
                        break;
                    }
                }
            }
        }

        return joinConstraintsPresent;
    }

    public static boolean satisfiesAnyJoinConstraint(Optional<OptTrace> optTraceParam, PlanNode planNode, boolean... ignoreDistributionType)
    {
        boolean satifiesAny = false;
        if (optTraceParam.isPresent()) {
            satifiesAny = optTraceParam.get().satisfiesAnyJoinConstraint(planNode, ignoreDistributionType);
        }

        return satifiesAny;
    }

    public static PlanNodeStatsEstimate stats(Optional<OptTrace> optTraceParam, PlanNode planNode, PlanNodeStatsEstimate stats)
    {
        PlanNodeStatsEstimate newStats = null;

        if (optTraceParam.isPresent()) {
            newStats = optTraceParam.get().planNodeStats(planNode, stats);
        }

        return newStats;
    }

    public static boolean valid(Optional<OptTrace> optTraceParam, PlanNode planNode)
    {
        boolean valid;

        if (optTraceParam.isPresent()) {
            OptTrace optTrace = optTraceParam.get();

            Integer joinId = optTrace.getJoinId(planNode);

            PruneReason reason = optTrace.isPruned(joinId);
            if (reason != null) {
                valid = false;
            }
            else if (optTrace.isValidUnderConstraints(joinId)) {
                valid = true;
            }
            else if (optTrace.planConstraints != null && optTrace.planConstraints.size() > 0) {
                JoinConstraintNode joinConstraintNode = optTrace.joinConstraintNode(planNode, null, false);
                valid = joinConstraintNode.joinIsValid(optTrace.planConstraints, optTrace);

                if (valid) {
                    optTrace.setIsValidUnderConstraints(joinId);
                }
                else {
                    reason = PruneReason.CONSTRAINT;
                    optTrace.addPrunedJoinId(joinId, reason);
                }
            }
            else {
                valid = true;
            }

            if (reason != null) {
                Pair<String, String> joinStrings = optTrace.getJoinStrings(planNode);
                requireNonNull(joinStrings, "join strings are null");
                requireNonNull(joinStrings.getKey(), "join string is null");
                optTrace.msg("Join not valid (** PRUNED BY %s **) : (%s , join id %d)",
                        true, reason.getString().toUpperCase(), joinStrings.getKey(), joinId.intValue());
            }
        }
        else {
            valid = true;
        }

        return valid;
    }

    private static String nodeLocation(io.trino.sql.tree.Node node)
    {
        String locationString;
        if (node.getLocation().isPresent()) {
            locationString = nodeLocationToString(node.getLocation().get());
        }
        else {
            locationString = null;
        }

        return locationString;
    }

    private static String nodeLocationToString(NodeLocation location)
    {
        String locationString = location.getLineNumber() + ":" + location.getColumnNumber();

        return locationString;
    }

    private static String tableName(TableScanNode tableScanNode, OptTrace optTrace)
    {
        return tableScanNode.getTable().toString();
    }

    public Session session()
    {
        return session;
    }

    void setTypeProvider(TypeProvider typesParam)
    {
        requireNonNull(typesParam, " type provider is null");
        types = typesParam;
    }

    public void addPlanNodeToPreferredPropertiesMapping(PlanNode planNode, PreferredProperties preferredProperties)
    {
        requireNonNull(planNode, "plan node is null");
        requireNonNull(preferredProperties, "preferred properties is null");
        planNodeIdToPreferredPropertiesMap.put(planNode.getId().toString(), preferredProperties);
    }

    public void addPrunedJoinId(Integer joinId, PruneReason reason)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "join id not set");
        }

        boolean add;
        if (reason == PruneReason.CONSTRAINT) {
            add = true;
        }
        else if (prunedJoinIds.get(joinId) == null) {
            add = true;
        }
        else {
            add = false;
        }

        if (add && prunedJoinIds.get(joinId) == null) {
            prunedJoinIds.put(joinId, reason);
        }
    }

    public PruneReason isPruned(Integer joinId)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "trace id not set");
        }

        return prunedJoinIds.get(joinId);
    }

    public boolean isValidUnderConstraints(Integer joinId)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "trace id not set");
        }

        return validUnderConstraintsJoinIds.contains(joinId);
    }

    public void setIsValidUnderConstraints(Integer joinId)
    {
        requireNonNull(joinId, "join id is null");
        if (joinId == -1) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "trace id not set");
        }

        validUnderConstraintsJoinIds.add(joinId);
    }

    public void addEnumeratedJoin(PlanNode planNode)
    {
        requireNonNull(planNode, "join node is null");

        if (planNode instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) planNode;
            if (!enumeratedJoins.contains(planNode)) {
                enumeratedJoins.add(joinNode);
            }
        }
        else if (planNode instanceof GroupReference) {
            GroupReference groupReference = (GroupReference) planNode;
            Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
            Optional<PlanNode> groupPlanNode = planNodes.findFirst();
            if (groupPlanNode.isPresent()) {
                PlanNode tmpNode = groupPlanNode.get();
                if (tmpNode instanceof JoinNode) {
                    if (!enumeratedJoins.contains(tmpNode)) {
                        enumeratedJoins.add((JoinNode) tmpNode);
                    }
                }
            }
        }
    }

    public BasicQueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    public void setQueryInfo(BasicQueryInfo queryInfoParam)
    {
        queryInfo = queryInfoParam;
    }

    public void reinitialize(Lookup lookUpParam, Memo memoParam, CostProvider costProviderParam, StatsProvider statsProviderParam)
    {
        lookUp = lookUpParam;
        memo = memoParam;
        costProvider = costProviderParam;
        statsProvider = statsProviderParam;
    }

    public LinkedHashSet<TableScanNode> getTableScans(PlanNode planNode)
    {
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.FIND_TABLES);
        planNode.accept(new OptTraceVisitor(), optTraceContext);

        return optTraceContext.tableScans;
    }

    public int getTableScanCnt(PlanNode planNode)
    {
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.CNT_TABLES);
        planNode.accept(new OptTraceVisitor(), optTraceContext);

        return optTraceContext.tableCnt;
    }

    public int getJoinCnt(PlanNode planNode)
    {
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.CNT_JOINS);
        planNode.accept(new OptTraceVisitor(), optTraceContext);

        return optTraceContext.joinCnt;
    }

    public Pair<String, String> getJoinStrings(PlanNode planNode)
    {
        Pair<String, String> joinStrings = this.joinStringMap.get(planNode);

        if (joinStrings == null) {
            OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.BUILD_JOIN_STRING);
            planNode.accept(new OptTraceVisitor(), optTraceContext);
            String joinString = new String(optTraceContext.tableCnt + "-way " + optTraceContext.joinString);

            String joinConstraintString = joinConstraintString(planNode, false);

            joinStrings = new Pair<String, String>(joinString, joinConstraintString);
            joinStringMap.put(planNode, joinStrings);
        }

        return joinStrings;
    }

    public Lookup lookUp()
    {
        return lookUp;
    }

    public Memo memo()
    {
        return memo;
    }

    public StatsProvider statsProvider()
    {
        return statsProvider;
    }

    public CostProvider costProvider()
    {
        return costProvider;
    }

    public Long nextUid()
    {
        ++uid;
        return uid;
    }

    public void begin(String msgString, Object... args)
    {
        uidStack.push(Long.valueOf(uid + 1));
        if (msgString != null) {
            String beginString = new String("BEGIN : " + msgString);

            msg(beginString, true, args);
        }

        indent += incrIndent;
    }

    private void buildPlanConstraints(String queryString)
            throws IOException
    {
        planConstraints = null;
        int beginConstraintPos = queryString.indexOf("/*!");
        if (beginConstraintPos != -1) {
            planConstraints = new ArrayList<JoinConstraintNode>();
            int endConstraintPos = queryString.indexOf("*/", beginConstraintPos);
            String constraintString = queryString.substring(beginConstraintPos + 3, endConstraintPos);
            JoinConstraintNode.parse(constraintString, planConstraints);
        }
    }

    public void queryInfo()
    {
        if (outputQueryInfo) {
            this.msg("Query info :", true);
            this.incrIndent(1);
            if (queryInfo != null) {
                this.msg("Query string (id %s) :", true, queryInfo.getQueryId().getId());
                this.msg("  %s", true, queryInfo.getQuery());

                try {
                    buildPlanConstraints(queryInfo.getQuery());
                }
                catch (IOException e) {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid join constraint");
                }
            }
            else {
                this.msg("<null>", true);
            }

            if (planConstraints != null) {
                this.msg("Constraints :", true);
                incrIndent(1);
                int cnt = 0;
                for (JoinConstraintNode joinConstraintNode : planConstraints) {
                    this.msg("%d : %s", true, cnt, joinConstraintNode.joinConstraintString());
                }
                decrIndent(1);
            }

            this.decrIndent(1);
            outputQueryInfo = false;
        }
    }

    public void end(String msgString, Object... args)
    {
        indent -= incrIndent;

        if (indent < 0) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Optimizer trace indent < 0"));
        }

        if (uidStack.empty()) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Optimizer trace UID stack empty"));
        }

        Long beginUid = uidStack.pop();

        if (msgString != null) {
            String endString = new String("END (for UID " + beginUid + ") : " + msgString);

            msg(endString, true, args);
        }
    }

    public void assignTraceIds(PlanNode planNode, Statement statement)
    {
        begin("assignTraceIds");
        if (!traceIdsAssigned) {
            //buildLocationToTableOrAliasNameMap(statement);
            assignTableScanTraceIds(planNode);

            if (planConstraints != null) {
                for (JoinConstraintNode joinConstraintNode : planConstraints) {
                    joinConstraintNode.setJoinIdsFromTableOrAliasName(joinIdMap, locationToTableOrAliasNameMap);
                }
            }

            begin("location-to-table/alias map");
            for (Map.Entry<String, String> entry : locationToTableOrAliasNameMap.entrySet()) {
                msg(entry.getKey() + " <=> " + entry.getValue(), true);
            }
            end("location-to-table/alias map");

            traceIdsAssigned = true;
        }

        end("assignTraceIds");
    }

    private String tableScanLookUpString(TableScanNode tableScanNode)
    {
//        StringBuilder builder = new StringBuilder();
//
//        if (tableScanNode.getSourceLocation().isPresent()) {
//            builder.append(sourceLocationToString(tableScanNode.getSourceLocation().get()));
//        }
//        else {
//            builder.append(tableScanNode.getId().getId());
//        }
//
//        return builder.toString();
        return tableScanNode.getId().toString();
    }

    private String getLookUpString(PlanNode planNode, boolean... ignoreDistributionType)
    {
        String lookUpString;
        boolean useCache;

        if (ignoreDistributionType.length == 0 || !ignoreDistributionType[0]) {
            lookUpString = planNodeToLookUpStringMap.get(planNode);
            useCache = true;
        }
        else {
            lookUpString = null;
            useCache = false;
        }

        if (lookUpString == null) {
            if (planNode instanceof TableScanNode) {
                lookUpString = tableScanLookUpString((TableScanNode) planNode);
            }
            else if (planNode instanceof GroupReference) {
                GroupReference groupReference = (GroupReference) planNode;
                Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
                Optional<PlanNode> groupPlanNode = planNodes.findFirst();
                if (groupPlanNode.isPresent()) {
                    PlanNode tmpNode = groupPlanNode.get();
                    lookUpString = getLookUpString(tmpNode);
                }
                else {
                    lookUpString = new String("Group Reference " + groupReference.getGroupId());
                }
            }
            else if (planNode instanceof JoinNode || planNode instanceof SemiJoinNode) {
                lookUpString = joinConstraintString(planNode, ignoreDistributionType);
            }
            else if (planNode instanceof ProjectNode || planNode instanceof ExchangeNode) {
                if (planNode.getSources() != null && planNode.getSources().size() == 1) {
                    lookUpString = getLookUpString(planNode.getSources().get(0), ignoreDistributionType);
                }
            }

            if (lookUpString == null) {
                String className = planNode.getClass().getSimpleName().toUpperCase();
                if (className.substring(className.length() - 4).equals("NODE")) {
                    className = className.substring(0, className.length() - 4);
                }

                StringBuilder builder = new StringBuilder(className);

                if (planNode.getSources().size() > 1) {
                    builder.append("(");
                }

                int cnt = 0;
                for (PlanNode source : planNode.getSources()) {
                    if (cnt > 0) {
                        builder.append(" ");
                    }

                    builder.append("(");
                    builder.append(getLookUpString(source, ignoreDistributionType));

                    builder.append(")");
                    ++cnt;
                }

                if (planNode.getSources().size() > 1) {
                    builder.append(")");
                }

                lookUpString = builder.toString();
            }
        }

        if (useCache) {
            planNodeToLookUpStringMap.put(planNode, lookUpString);
        }

        return lookUpString;
    }

    public void assignTableScanTraceIds(PlanNode node)
    {
        begin("assignTableScanTraceIds");
        LinkedHashSet<TableScanNode> tableScans = getTableScans(node);

        if (tableScans != null) {
            TableScanNameLocationComparator compare = new TableScanNameLocationComparator(this);

            ArrayList<TableScanNode> tableScansArray = new ArrayList<>(tableScans);
            tableScansArray.sort(compare);

            begin("location-to-id map");

            StringBuilder builder = new StringBuilder();
            for (TableScanNode tableScanNode : tableScansArray) {
                String lookUpString = tableScanLookUpString(tableScanNode);
                int traceId = newTraceId(lookUpString);
                newJoinId(lookUpString);

                builder.append(lookUpString);
                builder.append(" => ");
                builder.append(traceId);

                msg(builder.toString(), true);

                builder.setLength(0);
            }

            end("location-to-id map");
        }

        end("assignTableScanTraceIds");
    }

    public Pair<ArrayList<Integer>, ArrayList<Integer>> traceIds(JoinNode joinNode)
    {
        return null;
    }

    private Integer newTraceId(String lookUpString)
    {
        requireNonNull(lookUpString, "lookup string is null");

        Integer traceId = traceIdMap.get(lookUpString);

        if (traceId == null) {
            traceId = currentTraceId;
            traceIdMap.put(lookUpString, traceId);
            traceIdMap.put(traceId.toString(), traceId);
            ++currentTraceId;
        }

        return traceId;
    }

    private Integer newJoinId(String lookUpString)
    {
        requireNonNull(lookUpString, "lookup string is null");

        Integer joinId = joinIdMap.get(lookUpString);

        if (joinId == null) {
            joinId = Integer.valueOf(currentJoinId);
            joinIdMap.put(lookUpString, joinId);
            ++currentJoinId;
        }

        return joinId;
    }

    public Integer getTraceId(PlanNode planNode)
    {
        Integer traceId = minusOne;
        String lookUpString;

        lookUpString = getLookUpString(planNode);

        traceId = traceIdMap.get(lookUpString);

        if (traceId == null) {
            traceId = Integer.valueOf(currentTraceId);
            traceIdMap.put(lookUpString, traceId);
            ++currentTraceId;
        }

        return traceId;
    }

    public Integer getJoinId(PlanNode planNode, boolean... ignoreDistributionType)
    {
        Integer joinId = minusOne;

        String lookUpString;

        lookUpString = getLookUpString(planNode, ignoreDistributionType);

        requireNonNull(lookUpString, "lookup string is null");

        joinId = joinIdMap.get(lookUpString);

        if (joinId == null) {
            joinId = Integer.valueOf(currentJoinId);
            joinIdMap.put(lookUpString, joinId);
            ++currentJoinId;
        }

        return joinId;
    }

    Integer getJoinId(String lookUpString)
    {
        Integer joinId = joinIdMap.get(lookUpString);
        return joinId;
    }

    public void tracePlanNodeStatsEstimate(PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        incrIndent(indentCnt);
        if (statsProvider != null) {
            try {
                PlanNodeStatsEstimate stats = statsProvider.getStats(planNode);
                if (stats != null) {
                    tracePlanNodeStatsEstimate(stats, 0, msgString);
                }
            }
            catch (RuntimeException e) {
                msg("Stats lookup failed.", true);
            }
        }
        else {
            msg("No stats provider.", true);
        }

        decrIndent(indentCnt);
    }

    public String joinConstraintString(PlanNode planNode, boolean... ignoreDistributionType)
    {
        JoinConstraintNode joinConstraintNode = joinConstraintNode(planNode, null, ignoreDistributionType);
        String joinConstraintString;
        if (joinConstraintNode != null) {
            joinConstraintString = joinConstraintNode.joinConstraintString();
        }
        else {
            joinConstraintString = null;
        }

        return joinConstraintString;
    }

    private JoinConstraintNode joinConstraintNode(PlanNode planNode, PlanNode rootJoinNode, boolean... ignoreDistributionType)
    {
        JoinConstraintNode joinConstraintNode = null;
        boolean useCache;

        if (ignoreDistributionType.length == 0 || !ignoreDistributionType[0]) {
            joinConstraintNode = planNodeToJoinConstraintMap.get(planNode);
            useCache = true;
        }
        else {
            useCache = false;
        }

        if (joinConstraintNode == null) {
            if (planNode instanceof GroupReference) {
                GroupReference groupReference = (GroupReference) planNode;
                Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
                Optional<PlanNode> groupPlanNode = planNodes.findFirst();
                if (groupPlanNode.isPresent()) {
                    PlanNode tmpNode = groupPlanNode.get();
                    joinConstraintNode = joinConstraintNode(tmpNode, rootJoinNode, ignoreDistributionType);
                }
                else {
                    joinConstraintNode = new JoinConstraintNode(getJoinId(groupReference), groupReference, JoinConstraintNode.ConstraintType.JOIN);
                }
            }
            else {
                if (planNode instanceof TableScanNode) {
                    TableScanNode tableScanNode = (TableScanNode) planNode;
                    Integer joinId = getJoinId(planNode);

                    String tableOrAliasName = tableScanNode.getTable().toString();
//
//                    if (tableScanNode.getSourceLocation().isPresent()) {
//                        tableOrAliasName = locationToTableOrAliasNameMap.get(sourceLocationToString(tableScanNode.getSourceLocation().get()));
//                    }
//                    else {
//                        tableOrAliasName = null;
//                    }

                    joinConstraintNode = new JoinConstraintNode(tableScanNode, joinId, tableOrAliasName, JoinConstraintNode.ConstraintType.JOIN);
                }
                else if (planNode instanceof JoinNode) {
                    JoinNode joinNode = (JoinNode) planNode;
                    joinConstraintNode = new JoinConstraintNode(joinNode, ignoreDistributionType);

                    if (rootJoinNode == null) {
                        rootJoinNode = joinNode;
                    }

                    JoinConstraintNode childJoinConstraintNode = joinConstraintNode(joinNode.getLeft(), rootJoinNode, ignoreDistributionType);
                    joinConstraintNode.appendChild(childJoinConstraintNode);
                    childJoinConstraintNode = joinConstraintNode(joinNode.getRight(), rootJoinNode, ignoreDistributionType);
                    joinConstraintNode.appendChild(childJoinConstraintNode);

                    String joinConstraintString = joinConstraintNode.joinConstraintString();
                    Integer joinId = joinIdMap.get(joinConstraintString);

                    if (joinId == null) {
                        joinId = newJoinId(joinConstraintString);
                    }

                    joinConstraintNode.setId(joinId);
                }
                else if (planNode instanceof SemiJoinNode) {
                    SemiJoinNode semiJoinNode = (SemiJoinNode) planNode;
                    joinConstraintNode = new JoinConstraintNode(semiJoinNode, ignoreDistributionType);

                    if (rootJoinNode == null) {
                        rootJoinNode = semiJoinNode;
                    }

                    JoinConstraintNode childJoinConstraintNode = joinConstraintNode(semiJoinNode.getSource(), rootJoinNode, ignoreDistributionType);
                    joinConstraintNode.appendChild(childJoinConstraintNode);
                    childJoinConstraintNode = joinConstraintNode(semiJoinNode.getFilteringSource(), rootJoinNode, ignoreDistributionType);
                    joinConstraintNode.appendChild(childJoinConstraintNode);
                    String joinConstraintString = joinConstraintNode.joinConstraintString();
                    Integer joinId = joinIdMap.get(joinConstraintString);

                    if (joinId == null) {
                        joinId = newJoinId(joinConstraintString);
                    }

                    joinConstraintNode.setId(joinId);
                }
                else if (planNode instanceof ProjectNode || planNode instanceof ExchangeNode) {
                    if (planNode.getSources() != null && planNode.getSources().size() == 1) {
                        joinConstraintNode = joinConstraintNode(planNode.getSources().get(0), rootJoinNode, ignoreDistributionType);
                    }
                }

                if (joinConstraintNode == null) {
                    if (rootJoinNode == null) {
                        if (planNode instanceof JoinNode || planNode instanceof SemiJoinNode) {
                            rootJoinNode = findRootJoin(planNode, lookUp);
                        }

                        if (rootJoinNode != null) {
                            joinConstraintNode = joinConstraintNode(rootJoinNode, null, ignoreDistributionType);
                        }
                        else {
                            joinConstraintNode = new JoinConstraintNode(getJoinId(planNode, ignoreDistributionType), planNode,
                                    JoinConstraintNode.ConstraintType.JOIN);
                        }
                    }
                    else {
                        joinConstraintNode = new JoinConstraintNode(getJoinId(planNode), planNode, JoinConstraintNode.ConstraintType.JOIN);
                    }
                }
            }

            if (useCache) {
                planNodeToJoinConstraintMap.put(planNode, joinConstraintNode);
            }
        }

        return joinConstraintNode;
    }

    private void addJoinConstraintNode(PlanNode node)
    {
        JoinConstraintNode joinConstraintNode = joinConstraintNode(node, null);
        planConstraints.add(joinConstraintNode);
    }

    public boolean satisfiesAnyJoinConstraint(PlanNode planNode, boolean... ignoreDistributionType)
    {
        requireNonNull(planNode, "plan node is null");
        boolean satifiesAny = false;

        if (planConstraints != null && planConstraints.size() > 0 &&
                (planNode instanceof JoinNode || planNode instanceof SemiJoinNode || planNode instanceof GroupReference
                        || planNode instanceof TableScanNode)) {
            JoinConstraintNode joinConstraintNode = joinConstraintNode(planNode, null, ignoreDistributionType);
            requireNonNull(joinConstraintNode, "join constraint is null");

            satifiesAny = joinConstraintNode.satisfiesAnyConstraint(planConstraints, ignoreDistributionType);
        }

        return satifiesAny;
    }

    public Integer getCardinality(PlanNode planNode)
    {
        requireNonNull(planNode, "plan node is null");
        Integer cardinality = null;

        if (planConstraints != null && planConstraints.size() > 0) {
            JoinConstraintNode joinConstraintNode = joinConstraintNode(planNode, null, true);
            requireNonNull(joinConstraintNode, "join constraint is null");

            cardinality = joinConstraintNode.cardinality(planConstraints, this);
        }

        return cardinality;
    }

    public PlanNodeStatsEstimate planNodeStats(PlanNode planNode, PlanNodeStatsEstimate stats)
    {
        PlanNodeStatsEstimate newStats = null;

        if (stats != null) {
            Integer cardinality = getCardinality(planNode);

            if (cardinality != null) {
                double cardinalityAsDouble = cardinality.doubleValue();
                double factor = cardinalityAsDouble / stats.getOutputRowCount();
//                double totalSize = round(factor * stats.getOutputSizeInBytes(planNode.getOutputSymbols(), types), 0);
                PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.buildFrom(stats);
                builder.setOutputRowCount(cardinality.doubleValue());
//                builder.setTotalSize(totalSize);
//                builder.setConfident(true);
                newStats = builder.build();
            }
        }

        return newStats;
    }

    public String tableScansToString(LinkedHashSet<TableScanNode> tableScans)
    {
        requireNonNull(tableScans, "tableScans is null");
        List<String> nameList = new ArrayList<String>();
        for (TableScanNode tableScan : tableScans) {
            String name = tableName(tableScan, this);
            nameList.add(name);
        }

        Collections.sort(nameList);

        StringBuilder builder = new StringBuilder("(");

        if (nameList.size() > 1) {
            builder = new StringBuilder("Join(");
        }
        else {
            builder = new StringBuilder("(");
        }

        boolean first = true;
        for (String name : nameList) {
            if (!first) {
                builder.append(" ");
            }

            builder.append(name);

            first = false;
        }

        builder.append(")");

        return builder.toString();
    }

    public void buildLocationToTableOrAliasNameMap(Node node)
    {
//        if (node != null) {
//            new OptTraceAstVisitor(this).process(node);
//        }
    }

    private void doIndent(int indentCnt)
    {
        try {
            for (int i = 0; i < indentCnt; ++i) {
                bufferedWriter.write(" ");
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String tableName(int groupId)
    {
        String name = null;
        if (memo != null) {
            PlanNode planNode = memo.getNode(groupId);

            if (planNode instanceof TableScanNode) {
                TableScanNode tableScan = (TableScanNode) planNode;
                name = tableName(tableScan, this);
            }
        }

        return name;
    }

    public void tracePartition(Set<Integer> partition, String msgString, Object... args)
    {
        requireNonNull(partition, "partition is null");

        StringBuilder partMsgBuilderIds = new StringBuilder();

        if (msgString != null) {
            partMsgBuilderIds.append(String.format(msgString, args));
        }

        partMsgBuilderIds.append("[");

        boolean first = true;
        boolean foundName = false;
        for (int group : partition) {
            if (!first) {
                partMsgBuilderIds.append(" , ");
            }

            partMsgBuilderIds.append(String.format("%s", group));

            first = false;
        }

        partMsgBuilderIds.append("]");

        msg(partMsgBuilderIds.toString(), true);
    }

    public void traceJoinSources(Set<PlanNode> sources, int indentCnt, String msgString, Object... args)
    {
        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        int cnt = 0;
        for (PlanNode source : sources) {
            int joinId = getJoinId(source);
            String sourceStr = null;

            String nodeStr;

            if (source instanceof TableScanNode || source instanceof JoinNode || source instanceof SemiJoinNode) {
                Pair<String, String> joinStrings = getJoinStrings(source);
                requireNonNull(joinStrings, "join strings are null");

                nodeStr = joinStrings.getKey();
            }
            else {
                LinkedHashSet<TableScanNode> tableScans = getTableScans(source);

                if (tableScans != null && tableScans.size() > 0) {
                    nodeStr = tableScansToString(tableScans);
                }
                else {
                    nodeStr = source.getClass().getSimpleName() + " " + source.getId();
                }
            }

            if (source instanceof GroupReference) {
                GroupReference groupReference = (GroupReference) source;
                Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
                Optional<PlanNode> groupPlanNode = planNodes.findFirst();
                if (groupPlanNode.isPresent()) {
                    source = groupPlanNode.get();
                }
            }

            String sourceSimpleName;
            if (source instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) source;
                sourceSimpleName = joinNode.getType().name();
            }
            else {
                sourceSimpleName = source.getClass().getSimpleName();
            }

            if (joinId != -1) {
                sourceStr = "Source %d : " + sourceSimpleName + " " + nodeStr + format(" (node id %s , join id %d)",
                        source.getId(), joinId);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("join id lookup failed"));
            }

            msg(sourceStr, true, cnt);
            ++cnt;
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePlanCostEstimate(PlanCostEstimate planCostEstimate, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planCostEstimate, "node is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        if (planCostEstimate == PlanCostEstimate.infinite()) {
            msg("<infinite>", true);
        }
        else if (planCostEstimate == PlanCostEstimate.unknown()) {
            msg("<unknown>", true);
        }
        else if (planCostEstimate == PlanCostEstimate.zero()) {
            msg("<zero>", true);
        }
        else {
            msg("Cpu : %.3f , Network : %.3f , Max. mem. : %.3f", true, planCostEstimate.getCpuCost(),
                    planCostEstimate.getNetworkCost(),
                    planCostEstimate.getMaxMemory());
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePlanNodeStatsEstimate(PlanNodeStatsEstimate planNodeStatsEstimate, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planNodeStatsEstimate, "stats node estimate is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        if (planNodeStatsEstimate == PlanNodeStatsEstimate.unknown()) {
            msg("<unknown>", true);
        }
        else {
            msg("Row count : %.0f, confident? NA", true, planNodeStatsEstimate.getOutputRowCount());
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void traceJoinConstraint(PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planNode, "node is null");

        if (msgString != null) {
            incrIndent(indentCnt);

            msg(msgString, true, args);
        }

        incrIndent(1);
        JoinConstraintNode joinConstraint = joinConstraintNode(planNode, null, false);

        if (joinConstraint != null) {
            String joinConstraintString = null;
            Integer joinId = joinConstraint.joinId();
            if (joinId == null) {
                PlanNode source = joinConstraint.getSourceNode();
                requireNonNull(source, "source of join constraint is null");
                joinId = getJoinId(source);
                requireNonNull(joinId, "join id is null");
            }

            joinConstraintString = joinConstraint.joinConstraintString();
            msg(joinConstraintString + " (join id %d)", true, joinId.intValue());
        }
        else {
            msg("<no join>", true);
        }

        decrIndent(1);

        if (msgString != null) {
            decrIndent(indentCnt);
        }
    }

    public void traceEnumeratedJoins(PlanNode root, CostProvider costProvider, StatsProvider statsProvider, int indentCnt,
            String msgString, Object... args)
    {
        requireNonNull(root, "root is null");
        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        PlanNodeTableScanCntComparator compare = new PlanNodeTableScanCntComparator(this);
        LinkedHashSet<TableScanNode> tableScans = getTableScans(root);

        if (tableScans != null) {
            ArrayList<TableScanNode> tableScansArray = new ArrayList<>(tableScans);
            tableScansArray.sort(compare);

            msg("Table scans :", true);
            incrIndent(1);
            int cnt = 0;
            for (TableScanNode tableScanNode : tableScansArray) {
                int joinId = getJoinId(tableScanNode);
                String sourceStr = null;
                Pair<String, String> joinStrings = getJoinStrings(tableScanNode);
                requireNonNull(joinStrings, "join strings are null");

                if (joinId != -1) {
                    sourceStr = "Table scan %d : " + tableScanNode.getClass().getSimpleName() + " " + joinStrings.getKey() + format(" (node id %s , join id %d)",
                            tableScanNode.getId(), joinId);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("join id lookup failed"));
                }

                msg(sourceStr, true, cnt);

                if (costProvider != null) {
                    PlanCostEstimate planCostEstimate = costProvider.getCost(tableScanNode);
                    tracePlanCostEstimate(planCostEstimate, 1, "Estimated cost :");
                }

                if (statsProvider != null) {
                    PlanNodeStatsEstimate planNodeStatsEstimate = statsProvider.getStats(tableScanNode);
                    tracePlanNodeStatsEstimate(planNodeStatsEstimate, 1, "Estimated stats :");
                }

                ++cnt;
            }

            decrIndent(1);
        }
        else {
            msg("No table scans.", true);
        }

        Collections.sort(enumeratedJoins, compare);

        msg("Joins :", true);
        incrIndent(1);
        int cnt = 0;
        for (JoinNode joinNode : enumeratedJoins) {
            Integer joinId = getJoinId(joinNode);
            requireNonNull(joinId, "join id is null");
            Pair<String, String> joinStrings = getJoinStrings(joinNode);
            PruneReason pruneReason = isPruned(joinId);

            if (pruneReason != null) {
                msg("%d : %s , join id %d (** PRUNED BY %s **)", true, cnt,
                        joinStrings.getValue(), joinId.intValue(), pruneReason.getString().toUpperCase());
            }
            else {
                msg("%d : %s , join id %d (** ACCEPTED **)", true, cnt, joinStrings.getValue(), joinId.intValue());
            }

            msg("    %s", true, joinStrings.getKey());

            if (costProvider != null) {
                PlanCostEstimate planCostEstimate = costProvider.getCost(joinNode);
                tracePlanCostEstimate(planCostEstimate, 1, "Estimated cost :");
            }

            if (statsProvider != null) {
                PlanNodeStatsEstimate planNodeStatsEstimate = statsProvider.getStats(joinNode);
                tracePlanNodeStatsEstimate(planNodeStatsEstimate, 1, "Estimated stats :");
            }

            ++cnt;
        }

        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePartitioning(Partitioning partitioning, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(partitioning, "partitioning is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        PartitioningHandle handle = partitioning.getHandle();

        msg("Single node? %s", true, handle.getConnectorHandle().isSingleNode() ? "true" : "false");
        msg("Coordinator only? %s", true, handle.getConnectorHandle().isCoordinatorOnly() ? "true" : "false");

        msg("Arguments :", true);
        incrIndent(1);
        List<Partitioning.ArgumentBinding> arguments = partitioning.getArguments();
        int cnt = 0;
        for (Partitioning.ArgumentBinding arg : arguments) {
            msg("%d : %s", true, cnt, arg.toString());
            ++cnt;
        }
        decrIndent(1);

        Set<Symbol> varRefs = partitioning.getColumns();
        traceSymbols(varRefs.stream().collect(toList()), 1, "Variable references :");

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void traceOrderingScheme(Optional<OrderingScheme> orderingSchemeParam, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(orderingSchemeParam, "ordering scheme is null");
        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        msg("Orderings :", true);

        incrIndent(1);
        if (orderingSchemeParam.isPresent()) {
            OrderingScheme orderingScheme = orderingSchemeParam.get();

            int cnt = 0;
            for (Map.Entry<Symbol, SortOrder> entry : orderingScheme.getOrderings().entrySet()) {
                Symbol s = entry.getKey();
                SortOrder order = entry.getValue();
                msg("%d : %s %s", true, cnt, s.getName(), order.name());
                ++cnt;
            }
        }
        else {
            msg("<not present>", true);
        }
        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePartitioningScheme(PartitioningScheme partitioningScheme, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(partitioningScheme, "partitioning scheme is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        tracePartitioning(partitioningScheme.getPartitioning(), 0, "Partitioning :");

        traceSymbols(partitioningScheme.getOutputLayout(), 0, "Output layout :");

        Optional<Symbol> hashColumn = partitioningScheme.getHashColumn();

        if (hashColumn.isPresent()) {
            msg("Hash column : %s", true, hashColumn.get().getName());
        }
        else {
            msg("Hash column : <not present>", true);
        }

        msg("Replicate nulls and any : %s", true, partitioningScheme.isReplicateNullsAndAny() ? "true" : "false");

        if (partitioningScheme.getBucketToPartition().isPresent()) {
            msg("Bucket to partition : %s", true, partitioningScheme.getBucketToPartition().get().toString());
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void traceSymbols(List<Symbol> varList, int indentCnt, String msgString, Object... args)
    {
        int cnt = 0;

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true);
            incrIndent(1);
        }

        for (Symbol var : varList) {
            {
                msg("%d : %s (%s)", true, cnt, var.getName(), types.get(var).getDisplayName());
            }
            ++cnt;
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePartitioningProperties(PreferredProperties.PartitioningProperties partitioningProperties, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(partitioningProperties, "partitioning properties is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        traceSymbols(partitioningProperties.getPartitioningColumns().stream().collect(toList()), 0,
                "Partitioning columns :");

        msg("Partitioning :", true);
        incrIndent(1);

        if (partitioningProperties.getPartitioning().isPresent()) {
            tracePartitioning(partitioningProperties.getPartitioning().get(), 0, null);
        }
        else {
            msg("<not present>", true);
        }

        msg("Replicate nulls and any : %s", true, partitioningProperties.isNullsAndAnyReplicated() ? "true" : "false");
        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePreferredProperties(PreferredProperties preferredProperties, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(preferredProperties, "plan node is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        msg("Global properties :", true);
        incrIndent(1);
        Optional<PreferredProperties.Global> globalPropertiesOpt = preferredProperties.getGlobalProperties();
        if (globalPropertiesOpt.isPresent()) {
            PreferredProperties.Global globalProperties = globalPropertiesOpt.get();
            msg("Distributed? : %s", true, globalProperties.isDistributed() ? "true" : "false");
            msg("Partitioning properties? :", true);
            incrIndent(1);
            if (globalProperties.getPartitioningProperties().isPresent()) {
                tracePartitioningProperties(globalProperties.getPartitioningProperties().get(), 0, null);
            }
            else {
                msg("<not present>", true);
            }
            decrIndent(1);
        }
        else {
            msg("<not present>", true);
        }

        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePreferredPropertiesForPlanNode(PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planNode, "plan node is null");

        PreferredProperties preferredProperties = planNodeIdToPreferredPropertiesMap.get(planNode.getId().toString());

        if (preferredProperties != null) {
            tracePreferredProperties(preferredProperties, indentCnt, msgString, args);
        }
    }

    public void traceActualProperties(ActualProperties actualProperties, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(actualProperties, "plan node is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        Optional<Partitioning> nodePartitioning = actualProperties.getNodePartitioning();

        msg("Node partitioning :", true);
        incrIndent(1);

        if (nodePartitioning.isPresent()) {
            tracePartitioning(nodePartitioning.get(), 0, null);
        }
        else {
            msg("<not present>", true);
        }

        msg("Single node? : %s", true, actualProperties.isSingleNode() ? "true" : "false");
        msg("Coordinator only? : %s", true, actualProperties.isCoordinatorOnly() ? "true" : "false");
        msg("Replicate nulls and any : %s", true, actualProperties.isNullsAndAnyReplicated() ? "true" : "false");

        decrIndent(1);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void traceActualPropertiesForPlanNode(PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planNode, "plan node is null");

        ActualProperties actualProperties = planNodeIdToActualPropertiesMap.get(planNode.getId().toString());

        if (actualProperties != null) {
            traceActualProperties(actualProperties, indentCnt, msgString, args);
        }
    }

    public void traceJoinIdMap(int indentCnt, String msgString, Object... args)
    {
        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        ArrayList<Map.Entry<String, Integer>> joinIdList = new ArrayList<Map.Entry<String, Integer>>(joinIdMap.entrySet());

        Collections.sort(joinIdList, Comparator.comparingInt(Map.Entry::getValue));

        int cnt = 0;
        for (Map.Entry<String, Integer> entry : joinIdList) {
            String lookUpString = entry.getKey();
            Integer joinId = entry.getValue();
            msg("%d : join id %d <= %s", true, cnt, joinId, lookUpString);
            ++cnt;
        }

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void tracePlanNode(PlanNode node, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(node, "node is null");

        incrIndent(indentCnt);
        if (msgString != null) {
            msg(msgString, true, args);
            incrIndent(1);
        }

        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.PRINT);
        node.accept(new OptTraceVisitor(), optTraceContext);

        if (msgString != null) {
            decrIndent(1);
        }

        decrIndent(indentCnt);
    }

    public void incrIndent(int indentCnt)
    {
        indent += incrIndent * indentCnt;
    }

    public void decrIndent(int indentCnt)
    {
        indent -= incrIndent * indentCnt;
    }

    public void msg(String msgString, boolean eol, Object... args)
    {
        try {
            doIndent(indent);

            if (args != null) {
                bufferedWriter.write(String.format(msgString, args));
            }
            else {
                bufferedWriter.write(msgString);
            }

            bufferedWriter.write(" (UID " + nextUid() + ")");

            if (eol) {
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void finalize_old()
    {
        if (this.bufferedWriter != null) {
            try {
                bufferedWriter.flush();
                bufferedWriter.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public enum VisitType
    {
        NONE, PRINT, FIND_TABLES, BUILD_JOIN_STRING, CNT_TABLES, CNT_JOINS
    }

    public enum PruneReason
    {
        COST("cost"),
        CONSTRAINT("constraint");

        private String string;

        PruneReason(String stringRep)
        {
            this.string = stringRep;
        }

        public String getString()
        {
            return string;
        }
    }

    @Immutable
    public static class Pair<K, V>
    {
        private final K key;
        private final V value;

        @JsonCreator
        public Pair(@JsonProperty("key") K key, @JsonProperty("value") V value)
        {
            this.key = requireNonNull(key, "key is null");
            this.value = requireNonNull(value, "value is null");
        }

        @JsonProperty
        public K getKey()
        {
            return key;
        }

        @JsonProperty
        public V getValue()
        {
            return value;
        }
    }

    /**
     * This AST visitor was added in Presto tracer to aid resolving table names from TableScanNodes
     * By looking up Node source location references in this prebuilt Map to see what was the original table name
     * This does not work in Trino, because SourceLocation is not plumbed the same way
     */
//    private static class OptTraceAstVisitor
//            extends DefaultExpressionTraversalVisitor<Node>
//    {
//        private OptTrace optTrace;
//
//        OptTraceAstVisitor(OptTrace optTraceParam)
//        {
//            requireNonNull(optTraceParam, "null opt trace");
//            optTrace = optTraceParam;
//        }
//
//        protected Node visitRelation(Relation node, Void context)
//        {
//            if (node instanceof Table) {
//                Table table = (Table) node;
//                String tableName = table.getName().toString();
//                if (optTrace.locationToTableOrAliasNameMap.inverse().get(tableName) != null) {
//                    optTrace.locationToTableOrAliasNameMap.inverse().remove(tableName);
//                    optTrace.duplicateTableOrAliasNames.add(tableName);
//                }
//                else if (!optTrace.duplicateTableOrAliasNames.contains(tableName) && node.getLocation().isPresent()) {
//                    optTrace.locationToTableOrAliasNameMap.put(nodeLocation(node), tableName);
//                }
//            }
//
//            return visitNode(node, context);
//        }
//
//        @Override
//        protected Node visitSubqueryExpression(SubqueryExpression node, Void context)
//        {
//            return process(node.getQuery(), context);
//        }
//
//        @Override
//        protected Node visitAliasedRelation(AliasedRelation node, Void context)
//        {
//            requireNonNull(node.getAlias(), "alias is null");
//
//            if (node.getRelation() instanceof Table) {
//                String aliasName = node.getAlias().getValue();
//                if (optTrace.locationToTableOrAliasNameMap.inverse().get(aliasName) != null) {
//                    optTrace.locationToTableOrAliasNameMap.inverse().remove(aliasName);
//                    optTrace.duplicateTableOrAliasNames.add(aliasName);
//                }
//                else if (!optTrace.duplicateTableOrAliasNames.contains(aliasName) && node.getLocation().isPresent()) {
//                    optTrace.locationToTableOrAliasNameMap.put(nodeLocation(node), aliasName);
//                }
//
//                return null;
//            }
//
//            return process(node.getRelation(), context);
//        }
//    }

    private static class OptTraceVisitor
            extends PlanVisitor<PlanNode, OptTraceContext>
    {
        public OptTraceVisitor()
        {
        }

        private static String formatHash(Optional<Symbol>... hashes)
        {
            List<Symbol> variables = stream(hashes)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toList());

            if (variables.isEmpty()) {
                return "<empty>";
            }

            return "[" + Joiner.on(", ").join(variables) + "]";
        }

        @Override
        public PlanNode visitPlan(PlanNode planNode, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer joinId = optTrace.getJoinId(planNode);
                    optTrace.msg(planNode.getClass().getSimpleName() + " " + "(node id " + planNode.getId()
                            + ", join id " + joinId + ")", true);

                    optTrace.traceSymbols(planNode.getOutputSymbols(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(planNode, 1, "Preferred properties :");
                    optTrace.tracePlanNodeStatsEstimate(planNode, 1, "Estimated stats :");

                    int childId = 0;
                    for (PlanNode child : planNode.getSources()) {
                        optTrace.incrIndent(1);
                        optTrace.msg("Child %d :", true, childId);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(2);
                        ++childId;
                    }

                    break;
                }

                case BUILD_JOIN_STRING: {
                    if (planNode.getSources().size() > 1) {
                        int childId = 0;
                        for (PlanNode child : planNode.getSources()) {
                            if (optTraceContext.joinString == null) {
                                optTraceContext.joinString = new String();
                            }

                            if (childId == 0) {
                                optTraceContext.joinString = optTraceContext.joinString + "(";
                            }
                            else {
                                optTraceContext.joinString = optTraceContext.joinString + " ";
                            }

                            child.accept(this, optTraceContext);
                            ++childId;
                        }

                        if (childId > 0) {
                            optTraceContext.joinString = optTraceContext.joinString + ")";
                        }
                    }
                    else if (planNode.getSources().size() == 0) {
                        Integer traceId = optTrace.getTraceId(planNode);
                        if (optTraceContext.joinString == null) {
                            optTraceContext.joinString = new String();
                        }

                        optTraceContext.joinString = optTraceContext.joinString + traceId;
                    }
                    else {
                        for (PlanNode child : planNode.getSources()) {
                            child.accept(this, optTraceContext);
                        }
                    }

                    break;
                }

                default: {
                    for (PlanNode child : planNode.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        @Override
        public PlanNode visitGroupReference(GroupReference groupReference, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            requireNonNull(optTrace.lookUp(), "loopUp is null");
            Stream<PlanNode> planNodes = optTrace.lookUp().resolveGroup(groupReference);

            switch (optTraceContext.visitType) {
                case PRINT: {
                    int groupId = groupReference.getGroupId();

                    optTrace.msg("Group id " + groupId + " : ", true);

                    AtomicInteger count = new AtomicInteger(-1);
                    planNodes.forEach(member -> {
                        optTrace.incrIndent(1);
                        optTrace.msg("Member %d :", true, count.incrementAndGet());
                        optTrace.incrIndent(1);
                        member.accept(this, optTraceContext);
                        optTrace.decrIndent(2);
                    });

                    break;
                }

                default: {
                    planNodes.forEach(member -> {
                        member.accept(this, optTraceContext);
                    });

                    break;
                }
            }

            return null;
        }

        @Override
        public PlanNode visitProject(ProjectNode projectNode, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer joinId = optTrace.getJoinId(projectNode);
                    optTrace.msg("Project (node id " + projectNode.getId()
                            + ", join id " + joinId + ")", true);

                    optTrace.incrIndent(1);
                    optTrace.traceSymbols(projectNode.getOutputSymbols(), 0, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(projectNode, 0, "Preferred properties :");
                    optTrace.tracePlanNodeStatsEstimate(projectNode, 1, "Estimated stats :");

                    Assignments assignments = projectNode.getAssignments();
                    Map<Symbol, Expression> assignmentMap = assignments.getMap();
                    optTrace.msg("Assignments :", true);
                    optTrace.incrIndent(1);
                    int cnt = 0;
                    for (Map.Entry<Symbol, Expression> assignmentMapEntry : assignmentMap.entrySet()) {
                        optTrace.msg("%d : %s => %s", true, cnt, assignmentMapEntry.getKey().getName(),
                                assignmentMapEntry.getValue().toString());
                        ++cnt;
                    }
                    optTrace.decrIndent(1);

                    //optTrace.msg("Locality : %s", true, projectNode.getLocality());

                    optTrace.decrIndent(1);

                    cnt = 0;
                    optTrace.incrIndent(1);
                    for (PlanNode child : projectNode.getSources()) {
                        optTrace.msg("Child %d :", true, cnt);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(1);
                        ++cnt;
                    }
                    optTrace.decrIndent(1);

                    break;
                }

                default: {
                    for (PlanNode child : projectNode.getSources()) {
                        child.accept(this, optTraceContext);
                    }

                    break;
                }
            }

            return null;
        }

        @Override
        public PlanNode visitFilter(FilterNode filterNode, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer joinId = optTrace.getJoinId(filterNode);
                    optTrace.msg("Filter (node id " + filterNode.getId()
                            + ", join id " + joinId + ")", true);

                    optTrace.traceSymbols(filterNode.getOutputSymbols(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(filterNode, 1, "Preferred properties :");
                    optTrace.tracePlanNodeStatsEstimate(filterNode, 1, "Estimated stats :");

                    optTrace.incrIndent(1);
                    optTrace.msg("Predicate :", true);
                    optTrace.incrIndent(1);
                    optTrace.msg(filterNode.getPredicate().toString(), true, null);
                    optTrace.decrIndent(2);

                    int cnt = 0;
                    optTrace.incrIndent(1);
                    for (PlanNode child : filterNode.getSources()) {
                        optTrace.msg("Child %d :", true, cnt);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(1);
                        ++cnt;
                    }
                    optTrace.decrIndent(1);

                    break;
                }

                default: {
                    for (PlanNode child : filterNode.getSources()) {
                        child.accept(this, optTraceContext);
                    }

                    break;
                }
            }

            return null;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode exchange, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    ExchangeNode.Type type = exchange.getType();
                    ExchangeNode.Scope scope = exchange.getScope();
                    PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

                    optTrace.msg("ExchangeNode[%s] (node id %s)", true, exchange.getType(), exchange.getId());
                    optTrace.traceSymbols(exchange.getOutputSymbols(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(exchange, 1, "Preferred properties :");
                    optTrace.tracePlanNodeStatsEstimate(exchange, 1, "Estimated stats :");
                    optTrace.tracePartitioningScheme(exchange.getPartitioningScheme(), 1, "Partitioning scheme :");
                    optTrace.traceOrderingScheme(exchange.getOrderingScheme(), 1, "Ordering scheme :");
                    optTrace.incrIndent(1);

                    optTrace.msg("Inputs :", true);
                    optTrace.incrIndent(1);
                    int cnt = 0;
                    for (List<Symbol> input : exchange.getInputs()) {
                        optTrace.msg("%d : ", true, cnt);
                        optTrace.traceSymbols(input, 1, null);
                    }
                    optTrace.decrIndent(1);

                    optTrace.msg("Scope : %s", true, scope.toString());
//                    optTrace.msg("Ensure source ordering? : %s", true, exchange.isEnsureSourceOrdering());
                    optTrace.decrIndent(1);

                    int childId = 0;
                    for (PlanNode child : exchange.getSources()) {
                        optTrace.incrIndent(1);
                        optTrace.msg("Child %d :", true, childId);
                        optTrace.incrIndent(1);
                        child.accept(this, optTraceContext);
                        optTrace.decrIndent(2);
                        ++childId;
                    }

                    break;
                }

                default: {
                    for (PlanNode child : exchange.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        @Override
        public PlanNode visitJoin(JoinNode join, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(join);
                    Integer joinId = optTrace.getJoinId(join);

                    List<Expression> joinExpressions = new ArrayList<>();
                    for (JoinNode.EquiJoinClause clause : join.getCriteria()) {
                        joinExpressions.add(clause.toExpression());
                    }

                    String criteria = Joiner.on(" AND ").join(joinExpressions);

                    Pair<String, String> joinStrings = optTrace.getJoinStrings(join);
                    requireNonNull(joinStrings, "join strings are null");

                    optTrace.msg(join.getType().getJoinLabel() + " (node id " + join.getId()
                            + " , join id " + joinId + ")", true);
                    optTrace.traceSymbols(join.getOutputSymbols(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(join, 1, "Preferred properties :");
                    optTrace.tracePlanNodeStatsEstimate(join, 1, "Estimated stats :");

                    optTrace.msg("Join string : " + joinStrings.getKey() + ")", true);

                    optTrace.msg("Constraint : " + joinStrings.getValue() + ")", true);

                    optTrace.msg(join.getType().getJoinLabel() + " (node id " + join.getId() +
                            " , trace id " + traceId + ")", true);

                    Optional<JoinNode.DistributionType> distType = join.getDistributionType();
                    optTrace.incrIndent(1);

                    optTrace.msg("Criteria : %s", true, criteria);

                    if (join.getCriteria() != null && join.getCriteria().size() > 0) {
                        List<Symbol> leftVariables = join.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getLeft)
                                .collect(toImmutableList());
                        List<Symbol> rightVariables = join.getCriteria().stream()
                                .map(JoinNode.EquiJoinClause::getRight)
                                .collect(toImmutableList());

                        optTrace.traceSymbols(leftVariables, 1, "Left criteria variables :");
                        optTrace.traceSymbols(rightVariables, 1, "Right criteria variables :");
                    }

                    String formattedHash = formatHash(join.getLeftHashSymbol());
                    optTrace.msg("Left hash var. : %s", true, formattedHash);
                    formattedHash = formatHash(join.getRightHashSymbol());
                    optTrace.msg("Right hash var. : %s", true, formattedHash);
                    distType.ifPresent(dist -> optTrace.msg("Distribution type : %s", true, dist.name()));

                    optTrace.decrIndent(1);

                    optTrace.incrIndent(1);
                    optTrace.msg("Left input :", true);
                    optTrace.incrIndent(1);
                    join.getLeft().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    optTrace.incrIndent(1);
                    optTrace.msg("Right input :", true);
                    optTrace.incrIndent(1);
                    join.getRight().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    break;
                }

                case CNT_JOINS:
                    ++(optTraceContext.joinCnt);
                    break;

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String("(");
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + "(";
                    }

                    join.getLeft().accept(this, optTraceContext);
                    optTraceContext.joinString = optTraceContext.joinString + " " + join.getType() + " ";
                    join.getRight().accept(this, optTraceContext);

                    Optional<JoinNode.DistributionType> distType = join.getDistributionType();

                    optTraceContext.joinString = optTraceContext.joinString + ")";

                    String distStr = null;
                    if (distType.isPresent()) {
                        JoinNode.DistributionType joinDistType = distType.get();
                        switch (joinDistType) {
                            case PARTITIONED:
                                distStr = new String("[P]");
                                break;
                            case REPLICATED:
                                distStr = new String("[R]");
                                break;
                            default:
                                distStr = new String("[?]");
                                break;
                        }

                        optTraceContext.joinString = optTraceContext.joinString + " " + distStr;
                    }

                    break;
                }

                default: {
                    for (PlanNode child : join.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode join, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(join);
                    Integer joinId = optTrace.getJoinId(join);

                    optTrace.msg("SemiJoin (node id " + join.getId() +
                            " , join id " + traceId + ")", true);

                    optTrace.traceSymbols(join.getOutputSymbols(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(join, 1, "Preferred properties :");
                    optTrace.tracePlanNodeStatsEstimate(join, 1, "Estimated stats :");

                    Pair<String, String> joinStrings = optTrace.getJoinStrings(join);
                    requireNonNull(joinStrings, "join strings are null");

                    optTrace.msg("Join string : " + joinStrings.getKey() + ")", true);

                    optTrace.msg("Constraint : " + joinStrings.getValue() + ")", true);

                    Optional<SemiJoinNode.DistributionType> distType = join.getDistributionType();
                    optTrace.incrIndent(1);

                    distType.ifPresent(dist -> optTrace.msg("Distribution type : %s", true, dist.name()));

                    optTrace.decrIndent(1);

                    optTrace.incrIndent(1);
                    optTrace.msg("Probe :", true);
                    optTrace.incrIndent(1);
                    join.getSource().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    optTrace.incrIndent(1);
                    optTrace.msg("Build :", true);
                    optTrace.incrIndent(1);
                    join.getFilteringSource().accept(this, optTraceContext);
                    optTrace.decrIndent(2);

                    break;
                }

                case CNT_JOINS:
                    ++(optTraceContext.joinCnt);
                    break;

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String("(");
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + "(";
                    }

                    join.getSource().accept(this, optTraceContext);
                    optTraceContext.joinString = optTraceContext.joinString + " ";
                    join.getFilteringSource().accept(this, optTraceContext);

                    optTraceContext.joinString = optTraceContext.joinString + ")";

                    break;
                }

                default: {
                    for (PlanNode child : join.getSources()) {
                        child.accept(this, optTraceContext);
                    }
                }
            }

            return null;
        }

        public PlanNode visitTableScan(TableScanNode tableScan, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();
            ++(optTraceContext.tableCnt);

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer joinId = optTrace.getJoinId(tableScan);

                    if (joinId == null) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Could not find join id : %s (node id ", tableName(tableScan, optTrace)) +
                                tableScan.getId() + ")");
                    }

//                    ActualProperties actualProperties = PropertyDerivations.deriveProperties(tableScan, null, optTrace.metadata, optTrace.session,
//                            optTrace.types, optTrace.parser);
//                    optTrace.traceActualProperties(actualProperties, 0, "Actual properties :");

                    {
                        optTrace.msg(tableScan.getClass().getSimpleName() + " (" + tableName(tableScan, optTrace) + " , node id "
                                + tableScan.getId() + " , join id " + joinId + ")", true);
                    }

                    optTrace.traceSymbols(tableScan.getOutputSymbols(), 1, "Output variables :");
                    optTrace.tracePreferredPropertiesForPlanNode(tableScan, 1, "Preferred properties :");
                    optTrace.tracePlanNodeStatsEstimate(tableScan, 1, "Estimated stats :");

                    break;
                }

                case FIND_TABLES: {
                    optTraceContext.addTableScan(tableScan);
                    break;
                }

                case CNT_TABLES: {
                    break;
                }

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String(tableName(tableScan, optTrace));
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + tableName(tableScan, optTrace);
                    }
                }

                default:
            }

            return null;
        }
    }

    private class TableScanComparator
            implements Comparator<TableScanNode>
    {
        public int compare(TableScanNode tableScan1, TableScanNode tableScan2)
        {
            String str1 = tableScan1.getTable().getConnectorHandle().toString();
            String str2 = tableScan2.getTable().getConnectorHandle().toString();

            int cmp = str1.compareTo(str2);

            return cmp;
        }
    }

    private class PlanNodeTableScanCntComparator
            implements Comparator<PlanNode>
    {
        private OptTrace optTrace;

        public PlanNodeTableScanCntComparator(OptTrace optTraceParam)
        {
            requireNonNull(optTraceParam, "opt trace is null");
            optTrace = optTraceParam;
        }

        public int compare(PlanNode planNode1, PlanNode planNode2)
        {
            int cnt1 = optTrace.getTableScanCnt(planNode1);
            int cnt2 = optTrace.getTableScanCnt(planNode2);

            int cmp;

            cmp = cnt1 - cnt2;

            if (cmp == 0) {
                Integer traceId1 = optTrace.getTraceId(planNode1);
                Integer traceId2 = optTrace.getTraceId(planNode2);

                cmp = traceId1.intValue() - traceId2.intValue();
            }

            return cmp;
        }
    }

    private class TableScanNameLocationComparator
            implements Comparator<TableScanNode>
    {
        private OptTrace optTrace;

        public TableScanNameLocationComparator(OptTrace optTraceParam)
        {
            requireNonNull(optTraceParam, "opt trace is null");
            optTrace = optTraceParam;
        }

        public int compare(TableScanNode tableScanNode1, TableScanNode tableScanNode2)
        {
            String name1 = tableName(tableScanNode1, optTrace);
            String name2 = tableName(tableScanNode2, optTrace);

            int cmp = name1.compareTo(name2);

//            if (cmp == 0) {
//                if (tableScanNode1.getSourceLocation().isPresent() && tableScanNode2.getSourceLocation().isPresent()) {
//                    name1 = name1 + sourceLocationToString(tableScanNode1.getSourceLocation().get());
//                    name2 = name2 + sourceLocationToString(tableScanNode2.getSourceLocation().get());
//
//                    cmp = name1.compareTo(name2);
//                }
//            }

            return cmp;
        }
    }

    private class OptTraceContext
    {
        LinkedHashSet<TableScanNode> tableScans;
        VisitType visitType;
        String joinString;
        int tableCnt;
        int joinCnt;
        private OptTrace optTrace;

        public OptTraceContext(OptTrace optTraceParam, VisitType visitTypeParam)
        {
            requireNonNull(optTraceParam, "trace is null");
            optTrace = optTraceParam;
            visitType = visitTypeParam;
            joinString = null;
            tableCnt = 0;
            joinCnt = 0;
        }

        public void clearTableScans()
        {
            if (tableScans != null) {
                tableScans.clear();
            }
        }

        public void clearVisitType()
        {
            visitType = null;
        }

        public void clear()
        {
            clearVisitType();
            clearTableScans();
            joinString = null;
            tableCnt = 0;
        }

        public OptTrace optTrace()
        {
            return optTrace;
        }

        public void setVisitType(VisitType visitTypeParam)
        {
            visitType = visitTypeParam;
        }

        public void addTableScan(TableScanNode tableScanNode)
        {
            if (tableScans == null) {
                tableScans = new LinkedHashSet<TableScanNode>();
            }

            tableScans.add(tableScanNode);
        }

        LinkedHashSet<TableScanNode> tableScans()
        {
            return tableScans;
        }
    }
}
