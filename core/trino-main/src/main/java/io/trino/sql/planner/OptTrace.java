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

import com.google.common.base.Joiner;
import io.trino.cost.PlanCostEstimate;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Memo;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;
import org.apache.commons.math3.util.Pair;

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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
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
    HashMap<String, Integer> traceIdMap;
    Integer minusOne;
    HashMap<PlanNode, Pair<String, String>> joinStringMap;
    int currentTraceId;
    TableScanComparator tableScanComparator;
    JoinConstraintNode joinConstraintNode;

    public OptTrace(String dirPath)
    {
        initialize(dirPath, null, null);
    }

    public OptTrace(String dirPath, Lookup lookUpParam, Memo memoParam)
    {
        initialize(dirPath, lookUpParam, memoParam);
    }

    private void initialize(String dirPath, Lookup lookUpParam, Memo memoParam)
    {
        requireNonNull(dirPath, "dirPath is null");

        indent = 0;
        incrIndent = 2;
        lookUp = lookUpParam;
        memo = memoParam;
        uid = 0;
        uidStack = new Stack<Long>();
        queryInfo = null;
        traceIdMap = new HashMap<String, Integer>();
        minusOne = Integer.valueOf(-1);
        joinStringMap = new HashMap<PlanNode, Pair<String, String>>();
        currentTraceId = 0;
        tableScanComparator = new TableScanComparator();
        joinConstraintNode = null;

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

    public BasicQueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    public void setQueryInfo(BasicQueryInfo queryInfoParam)
    {
        queryInfo = queryInfoParam;
    }

    public void reinitialize(Lookup lookUpParam, Memo memoParam)
    {
        lookUp = lookUpParam;
        memo = memoParam;
    }

    public void reinitializeTraceIds()
    {
        traceIdMap.clear();
        currentTraceId = 0;
    }

    public ArrayList<TableScanNode> getTableScans(PlanNode planNode)
    {
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.FIND_TABLES);
        planNode.accept(new OptTrace.OptTraceVisitor(), optTraceContext);

        return optTraceContext.tableScans;
    }

    public Pair<String, String> getJoinStrings(PlanNode planNode)
    {
        Pair<String, String> joinStrings = this.joinStringMap.get(planNode);

        if (joinStrings == null && (planNode instanceof JoinNode || planNode instanceof SemiJoinNode ||
                planNode instanceof GroupReference)) {
            OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.BUILD_JOIN_STRING);
            planNode.accept(new OptTrace.OptTraceVisitor(), optTraceContext);
            String joinString = new String(optTraceContext.tableCnt + "-way " + optTraceContext.joinString);
            JoinConstraintNode joinConstraintNode = joinConstraintNode(planNode);
            requireNonNull(joinConstraintNode, "join constraint is null");

            String joinConstraintString = joinConstraintNode.joinConstraintString();
            requireNonNull(joinConstraintString, "join constraint string is null");

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

    private void findPlanConstraints(String queryString)
            throws IOException
    {
        joinConstraintNode = null;
        int beginConstraintPos = queryString.indexOf("/*!");
        if (beginConstraintPos != -1) {
            int endConstraintPos = queryString.indexOf("*/", beginConstraintPos);
            String constraintString = queryString.substring(beginConstraintPos + 3, endConstraintPos - 1).trim();
            joinConstraintNode = JoinConstraintNode.parse(constraintString);
        }
    }

    public void queryInfo()
    {
        this.msg("Query info :", true);
        this.incrIndent(1);
        if (queryInfo != null) {
            this.msg("Query string (id %s) :", true, queryInfo.getQueryId().getId());
            this.msg("  %s", true, queryInfo.getQuery());

            try {
                findPlanConstraints(queryInfo.getQuery());
            }
            catch (IOException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Invalid join constraint"));
            }
        }
        else {
            this.msg("<null>", true);
        }

        this.decrIndent(1);
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

    public void assignTraceIds(PlanNode planNode)
    {
        traceIdMap.clear();
        currentTraceId = 0;
        assignTableScanTraceIds(planNode);
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

    public void assignTableScanTraceIds(PlanNode node)
    {
        ArrayList<TableScanNode> tableScans = getTableScans(node);

        if (tableScans != null) {
            Collections.sort(tableScans, tableScanComparator);
            tableScans.forEach(tableScan -> getTraceId(tableScan));
        }
    }

    public Pair<ArrayList<Integer>, ArrayList<Integer>> traceIds(JoinNode joinNode)
    {
        return null;
    }

    public Integer getTraceId(PlanNode planNode)
    {
        Integer traceId = minusOne;
        String lookUpString = null;
        if (planNode instanceof TableScanNode) {
            TableScanNode tableScanNode = (TableScanNode) planNode;
            lookUpString = "Table Scan Node " + tableScanNode.getId().toString();
        }
        else if (planNode instanceof JoinNode) {
            Pair<String, String> joinStrings = getJoinStrings((JoinNode) planNode);
            requireNonNull(joinStrings, "join strings are null");
            lookUpString = joinStrings.getValue();
        }
        else if (planNode instanceof SemiJoinNode) {
            Pair<String, String> joinStrings = getJoinStrings((JoinNode) planNode);
            requireNonNull(joinStrings, "join strings are null");
            lookUpString = joinStrings.getValue();
        }
        else if (planNode instanceof GroupReference) {
            GroupReference groupReference = (GroupReference) planNode;
            Stream<PlanNode> planNodes = lookUp.resolveGroup(groupReference);
            Optional<PlanNode> groupPlanNode = planNodes.findFirst();
            if (groupPlanNode.isPresent()) {
                PlanNode tmpNode = groupPlanNode.get();
                if (tmpNode instanceof TableScanNode) {
                    traceId = getTraceId(tmpNode);
                }
                else if (tmpNode instanceof JoinNode) {
                    traceId = getTraceId(tmpNode);
                }
                else if (tmpNode instanceof SemiJoinNode) {
                    traceId = getTraceId(tmpNode);
                }
            }

            if (traceId == minusOne) {
                lookUpString = new String("Group Reference" + groupReference.getGroupId());
            }
        }

        if (lookUpString != null) {
            traceId = traceIdMap.get(lookUpString);

            if (traceId == null) {
                traceId = Integer.valueOf(currentTraceId);
                traceIdMap.put(lookUpString, traceId);
                ++currentTraceId;
            }
        }

        return traceId;
    }

    public static void begin(Optional<OptTrace> optTraceParam, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.begin(msgString, args));
    }

    public static void queryInfo(Optional<OptTrace> optTraceParam)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.queryInfo());
    }

    public static void trace(Optional<OptTrace> optTraceParam, PlanNode planNode, int indentCnt, String msgString, Object... args)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.tracePlanNode(planNode, indentCnt, msgString, args));
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

    private JoinConstraintNode joinConstraintNode(PlanNode planNode)
    {
        JoinConstraintNode joinConstraintNode = null;

        if (planNode instanceof TableScanNode || planNode instanceof GroupReference) {
            joinConstraintNode = new JoinConstraintNode(getTraceId(planNode));
        }
        else if (planNode instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) planNode;
            joinConstraintNode = new JoinConstraintNode();
            JoinConstraintNode childJoinConstraintNode = joinConstraintNode(joinNode.getLeft());
            joinConstraintNode.appendChild(childJoinConstraintNode);
            childJoinConstraintNode = joinConstraintNode(joinNode.getRight());
            joinConstraintNode.appendChild(childJoinConstraintNode);
        }
        else if (planNode instanceof SemiJoinNode) {
            SemiJoinNode semiJoinNode = (SemiJoinNode) planNode;
            joinConstraintNode = new JoinConstraintNode();
            JoinConstraintNode childJoinConstraintNode = joinConstraintNode(semiJoinNode.getSource());
            joinConstraintNode.appendChild(childJoinConstraintNode);
            childJoinConstraintNode = joinConstraintNode(semiJoinNode.getFilteringSource());
            joinConstraintNode.appendChild(childJoinConstraintNode);
        }
        else if (planNode.getSources().size() == 1) {
            Optional<PlanNode> child = planNode.getSources().stream().findFirst();
            if (child.isPresent()) {
                PlanNode childPlanNode = child.get();
                joinConstraintNode = joinConstraintNode(childPlanNode);
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unexpected number of children : %d (node id %s , %s)",
                    planNode.getSources().size(), planNode.getId().toString(),
                    planNode.getClass().getSimpleName()));
        }

        return joinConstraintNode;
    }

    public static JoinConstraintNode joinConstraintNode(Optional<OptTrace> optTraceParam, JoinNode joinNode)
    {
        JoinConstraintNode joinConstraintNode = null;
        if (optTraceParam.isPresent()) {
            joinConstraintNode = optTraceParam.get().joinConstraintNode(joinNode);
        }

        return joinConstraintNode;
    }

    public static JoinConstraintNode joinConstraintNode(Optional<OptTrace> optTraceParam, SemiJoinNode semiJoinNode)
    {
        JoinConstraintNode joinConstraintNode = null;
        if (optTraceParam.isPresent()) {
            joinConstraintNode = optTraceParam.get().joinConstraintNode(semiJoinNode);
        }

        return joinConstraintNode;
    }

    public static void assignTraceIds(Optional<OptTrace> optTraceParam, PlanNode node)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.assignTraceIds(node));
    }

    public static void reinitializeTraceIds(Optional<OptTrace> optTraceParam)
    {
        optTraceParam.ifPresent(optTrace -> optTrace.reinitializeTraceIds());
    }

    public enum VisitType
    {
        NONE, PRINT, FIND_TABLES, BUILD_JOIN_STRING
    }

    private class OptTraceContext
    {
        private OptTrace optTrace;
        ArrayList<TableScanNode> tableScans;
        VisitType visitType;
        String joinString;
        int tableCnt;

        public OptTraceContext(OptTrace optTraceParam, VisitType visitTypeParam)
        {
            requireNonNull(optTraceParam, "trace is null");
            optTrace = optTraceParam;
            visitType = visitTypeParam;
            joinString = null;
            tableCnt = 0;
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
                tableScans = new ArrayList<TableScanNode>();
            }

            tableScans.add(tableScanNode);
        }

        List<TableScanNode> tableScans()
        {
            return tableScans;
        }
    }

    public String tableScansToString(List<TableScanNode> tableScans)
    {
        requireNonNull(tableScans, "tableScans is null");
        List<String> nameList = new ArrayList<String>();
        for (TableScanNode tableScan : tableScans) {
            String name = tableName(tableScan);
            nameList.add(name);
        }

        Collections.sort(nameList);

        StringBuilder builder = new StringBuilder("[");
        boolean first = true;
        for (String name : nameList) {
            if (!first) {
                builder.append(" ");
            }

            builder.append(name);

            first = false;
        }

        builder.append("]");

        return builder.toString();
    }

    private static class OptTraceVisitor
            extends PlanVisitor<PlanNode, OptTraceContext>
    {
        public OptTraceVisitor()
        {
        }

        @Override
        public PlanNode visitPlan(PlanNode planNode, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    optTrace.msg(planNode.getClass().getSimpleName() + " " + "(node id " + planNode.getId()
                            + ")", true);

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
        public PlanNode visitExchange(ExchangeNode exchange, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    String columns;
                    if (exchange.getType() == REPARTITION) {
                        columns = Joiner.on(", ").join(exchange.getPartitioningScheme().getPartitioning().getArguments());
                    }
                    else {
                        columns = Joiner.on(", ").join(exchange.getOutputSymbols());
                    }

                    optTrace.msg("ExchangeNode[%s] (Node id %s)", true, exchange.getType(), exchange.getId());

                    optTrace.incrIndent(1);
                    if (!columns.isEmpty()) {
                        optTrace.msg("Columns : %s", true, columns);
                    }
                    else {
                        optTrace.msg("Columns : <empty>", true);
                    }
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
        public PlanNode visitJoin(JoinNode join, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(join);
                    List<Expression> joinExpressions = new ArrayList<>();
                    for (JoinNode.EquiJoinClause clause : join.getCriteria()) {
                        joinExpressions.add(clause.toExpression());
                    }

                    String criteria = Joiner.on(" AND ").join(joinExpressions);

                    Pair<String, String> joinStrings = optTrace.getJoinStrings(join);
                    requireNonNull(joinStrings, "join strings are null");

                    optTrace.msg("Node id " + join.getId() + " ("
                            + join.getType().getJoinLabel() + ")", true);

                    optTrace.msg("Join string : " + joinStrings.getKey() + ")", true);

                    optTrace.msg("Constraint : " + joinStrings.getValue() + " (trace id " + traceId + ")", true);

                    optTrace.msg(join.getType().getJoinLabel() + " (node id " + join.getId() +
                            " , trace id " + traceId + ")", true);

                    Optional<JoinNode.DistributionType> distType = join.getDistributionType();
                    optTrace.incrIndent(1);

                    optTrace.msg("Criteria : %s", true, criteria);

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

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String("(");
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + "(";
                    }

                    join.getLeft().accept(this, optTraceContext);
                    optTraceContext.joinString = optTraceContext.joinString + " ";
                    join.getRight().accept(this, optTraceContext);

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

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode join, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(join);

                    optTrace.msg("Node id " + join.getId() + " (SemiJoin)", true);

                    optTrace.msg("SemiJoin (node id " + join.getId() +
                            " , trace id " + traceId + ")", true);

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

        @Override
        public PlanNode visitTableScan(TableScanNode tableScan, OptTraceContext optTraceContext)
        {
            OptTrace optTrace = optTraceContext.optTrace();
            ++(optTraceContext.tableCnt);

            switch (optTraceContext.visitType) {
                case PRINT: {
                    Integer traceId = optTrace.getTraceId(tableScan);

                    if (traceId == null) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Could not find trace id : %s (node id ", tableName(tableScan)) +
                                tableScan.getId() + ")");
                    }

                    optTrace.msg(tableScan.getClass().getSimpleName() + " (" + tableName(tableScan) + " , node id " + tableScan.getId() + " , trace id " +
                            traceId + ")", true);

                    break;
                }

                case FIND_TABLES: {
                    optTraceContext.addTableScan(tableScan);
                    break;
                }

                case BUILD_JOIN_STRING: {
                    if (optTraceContext.joinString == null) {
                        optTraceContext.joinString = new String(tableName(tableScan));
                    }
                    else {
                        optTraceContext.joinString = optTraceContext.joinString + tableName(tableScan);
                    }
                }

                default:
            }

            return null;
        }
    }

    private static String tableName(TableScanNode tableScanNode)
    {
        String str = tableScanNode.getTable().getConnectorHandle().toString();

        String token = new String("tableName=");
        int startPos = str.indexOf(token);
        if (startPos != -1) {
            startPos += token.length();
            int endPos = str.indexOf(",", startPos);

            if (endPos != -1) {
                str = str.substring(startPos, endPos);
            }
        }

        return str;
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
                name = tableName(tableScan);
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
        if (msgString != null) {
            incrIndent(indentCnt);

            msg(msgString, true, args);
            incrIndent(1);
        }
        else {
            incrIndent(indentCnt);
        }

        int cnt = 0;
        for (PlanNode source : sources) {
            int traceId = getTraceId(source);
            String sourceStr = null;
            Pair<String, String> joinStrings = getJoinStrings(source);
            requireNonNull(joinStrings, "join strings are null");

            if (traceId != -1) {
                sourceStr = "Source %d : " + source.getClass().getSimpleName() + " " + joinStrings.getKey() + format(" (node id %s , trace id %d)",
                        source.getId(), traceId);
            }
            else {
                sourceStr = "Source %d : " + source.getClass().getSimpleName() + " " + joinStrings.getKey() + format(" (node id %s)",
                        source.getId());
            }
            msg(sourceStr, true, cnt);
            ++cnt;
        }

        if (msgString != null) {
            decrIndent(indentCnt + 1);
        }
        else {
            decrIndent(indentCnt);
        }
    }

    public void tracePlanCostEstimate(PlanCostEstimate planCostEstimate, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(planCostEstimate, "node is null");

        if (msgString != null) {
            incrIndent(indentCnt);

            msg(msgString, true, args);
        }

        incrIndent(indentCnt + 1);

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

        decrIndent(indentCnt + 1);

        if (msgString != null) {
            decrIndent(indentCnt);
        }
    }

    public void tracePlanNode(PlanNode node, int indentCnt, String msgString, Object... args)
    {
        requireNonNull(node, "node is null");

        if (msgString != null) {
            incrIndent(indentCnt);

            msg(msgString, true, args);
        }

        incrIndent(1);
        OptTraceContext optTraceContext = new OptTraceContext(this, VisitType.PRINT);
        node.accept(new OptTrace.OptTraceVisitor(), optTraceContext);
        decrIndent(1);

        if (msgString != null) {
            decrIndent(indentCnt);
        }
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

            bufferedWriter.write(String.format(msgString, args));
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

    protected void finalize()
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
}
