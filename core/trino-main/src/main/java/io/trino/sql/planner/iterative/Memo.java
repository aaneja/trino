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
package io.trino.sql.planner.iterative;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import io.airlift.log.Logger;
import io.trino.cost.PlanCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.sql.planner.iterative.Plans.resolveGroupReferences;
import static java.util.Objects.requireNonNull;

/**
 * Stores a plan in a form that's efficient to mutate locally (i.e. without
 * having to do full ancestor tree rewrites due to plan nodes being immutable).
 * <p>
 * Each node in a plan is placed in a group, and it's children are replaced with
 * symbolic references to the corresponding groups.
 * <p>
 * For example, a plan like:
 * <pre>
 *    A -> B -> C -> D
 *           \> E -> F
 * </pre>
 * would be stored as:
 * <pre>
 * root: G0
 *
 * G0 : { A -> G1 }
 * G1 : { B -> [G2, G3] }
 * G2 : { C -> G4 }
 * G3 : { E -> G5 }
 * G4 : { D }
 * G5 : { F }
 * </pre>
 * Groups are reference-counted, and groups that become unreachable from the root
 * due to mutations in a subtree get garbage-collected.
 */
public class Memo
{
    private static final Logger LOG = Logger.get(Memo.class);

    private static final int ROOT_GROUP_REF = 0;

    private final PlanNodeIdAllocator idAllocator;
    private final int rootGroup;
    private final Map<Integer, Group> groups = new HashMap<>();
    private String prevJoinOrder = "";
    private int nextGroupId = ROOT_GROUP_REF + 1;

    public Memo(PlanNodeIdAllocator idAllocator, PlanNode plan)
    {
        this.idAllocator = idAllocator;
        rootGroup = insertRecursive(plan);
        prevJoinOrder = computeJoinOrderFromRoot();
        groups.get(rootGroup).incomingReferences.add(ROOT_GROUP_REF);
    }

    public int getRootGroup()
    {
        return rootGroup;
    }

    private Group getGroup(int group)
    {
        checkArgument(groups.containsKey(group), "Invalid group: %s", group);
        return groups.get(group);
    }

    public PlanNode getNode(int group)
    {
        return getGroup(group).membership;
    }

    public PlanNode resolve(GroupReference groupReference)
    {
        return getNode(groupReference.getGroupId());
    }

    public PlanNode extract()
    {
        return extract(getNode(rootGroup));
    }

    private PlanNode extract(PlanNode node)
    {
        return resolveGroupReferences(node, Lookup.from(planNode -> Stream.of(this.resolve(planNode))));
    }

    public PlanNode replace(int groupId, PlanNode node, String reason)
    {
        Group group = getGroup(groupId);
        PlanNode old = group.membership;

        checkArgument(new HashSet<>(old.getOutputSymbols()).equals(new HashSet<>(node.getOutputSymbols())),
                "%s: transformed expression doesn't produce same outputs: %s vs %s",
                reason,
                old.getOutputSymbols(),
                node.getOutputSymbols());

        if (node instanceof GroupReference) {
            node = getNode(((GroupReference) node).getGroupId());
        }
        else {
            node = insertChildrenAndRewrite(node);
        }

        incrementReferenceCounts(node, groupId);
        group.membership = node;
        decrementReferenceCounts(old, groupId);
        evictStatisticsAndCost(group);

        logFullPlan(reason);
        return node;
    }

    private void logFullPlan(String reason)
    {
        final String newJoinOrder = computeJoinOrderFromRoot();
        if (!newJoinOrder.equals(prevJoinOrder)) {
            LOG.info("[%s] caused Join Order change : %s", reason, newJoinOrder);
            prevJoinOrder = newJoinOrder;
        }
    }

    @NotNull
    private String computeJoinOrderFromRoot()
    {
        final PlanNode rootNode = getNode(getRootGroup());

        StringBuilder joinOrder = new StringBuilder();
        traversePostOrder(rootNode, joinOrder);
        return joinOrder.toString();
    }

    private void traversePostOrder(PlanNode node, StringBuilder joinOrder)
    {
        GroupReference groupRef = null;
        if (node instanceof GroupReference) {
            //Get PlanNode if GroupNode
            groupRef = (GroupReference) node;
            node = getNode(groupRef.getGroupId());
        }

        for (PlanNode source : node.getSources()) {
            //Traverse children first, i.e. do a post order traversal, giving us a postfix expression
            //Will go left then right for Join Nodes
            traversePostOrder(source, joinOrder);
        }

        //Next print the current node
        if (node instanceof JoinNode) {
            JoinNode jNode = (JoinNode) node;
            String distributionType = jNode.getDistributionType().isPresent() ? jNode.getDistributionType().get().toString() : "NotAssigned";
            String reorderJoinStatsAndCost = jNode.getReorderJoinStatsAndCost().isPresent() ? jNode.getReorderJoinStatsAndCost().get().toString() : "NoStatsAndCost";
            joinOrder.append(String.format("Join[%s, %s, %s], ", jNode.getType(), distributionType, reorderJoinStatsAndCost));
        }
        else if (node instanceof TableScanNode) {
            TableScanNode tNode = (TableScanNode) node;

            joinOrder.append(getTableName(tNode.getTable()))
                    .append(", ");
        }
        else if (node.getSources().size() > 1) {
            joinOrder.append(String.format("Operator[%s] Operands[%d], ", node, node.getSources().size()));
        }
    }

    /*
    Gets String representation of GroupReference, if present
     */
    private String getStatsAndCost(GroupReference groupRef)
    {
        if (groupRef == null) {
            return "NoStatsAndCost";
        }
        Optional<PlanNodeStatsEstimate> stats = getStats(groupRef.getGroupId());
        Optional<PlanCostEstimate> cost = getCost(groupRef.getGroupId());

        StringBuilder out = new StringBuilder();
        stats.ifPresent(out::append);
        cost.ifPresent(out::append);

        return "" .equals(out.toString()) ? "NoStatsAndCost" : out.toString();
    }

    private String getTableName(TableHandle table)
    {
        try {
            return table.toString().split(":")[2];
        }
        catch (Exception e) {
            return table.toString();
        }
    }

    private void evictStatisticsAndCost(Group group)
    {
        group.stats = null;
        group.cost = null;
        for (int parentGroup : group.incomingReferences.elementSet()) {
            if (parentGroup != ROOT_GROUP_REF) {
                evictStatisticsAndCost(getGroup(parentGroup));
            }
        }
    }

    public Optional<PlanNodeStatsEstimate> getStats(int group)
    {
        return Optional.ofNullable(getGroup(group).stats);
    }

    public void storeStats(int groupId, PlanNodeStatsEstimate stats)
    {
        Group group = getGroup(groupId);
        if (group.stats != null) {
            evictStatisticsAndCost(group); // cost is derived from stats, also needs eviction
        }
        group.stats = requireNonNull(stats, "stats is null");
    }

    public Optional<PlanCostEstimate> getCost(int group)
    {
        return Optional.ofNullable(getGroup(group).cost);
    }

    public void storeCost(int group, PlanCostEstimate cost)
    {
        getGroup(group).cost = requireNonNull(cost, "cost is null");
    }

    private void incrementReferenceCounts(PlanNode fromNode, int fromGroup)
    {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            groups.get(group).incomingReferences.add(fromGroup);
        }
    }

    private void decrementReferenceCounts(PlanNode fromNode, int fromGroup)
    {
        Set<Integer> references = getAllReferences(fromNode);

        for (int group : references) {
            Group childGroup = groups.get(group);
            checkState(childGroup.incomingReferences.remove(fromGroup), "Reference to remove not found");

            if (childGroup.incomingReferences.isEmpty()) {
                deleteGroup(group);
            }
        }
    }

    private Set<Integer> getAllReferences(PlanNode node)
    {
        return node.getSources().stream()
                .map(GroupReference.class::cast)
                .map(GroupReference::getGroupId)
                .collect(Collectors.toSet());
    }

    private void deleteGroup(int group)
    {
        checkArgument(getGroup(group).incomingReferences.isEmpty(), "Cannot delete group that has incoming references");
        PlanNode deletedNode = groups.remove(group).membership;
        decrementReferenceCounts(deletedNode, group);
    }

    private PlanNode insertChildrenAndRewrite(PlanNode node)
    {
        return node.replaceChildren(
                node.getSources().stream()
                        .map(child -> new GroupReference(
                                idAllocator.getNextId(),
                                insertRecursive(child),
                                child.getOutputSymbols()))
                        .collect(Collectors.toList()));
    }

    private int insertRecursive(PlanNode node)
    {
        if (node instanceof GroupReference) {
            return ((GroupReference) node).getGroupId();
        }

        int group = nextGroupId();
        PlanNode rewritten = insertChildrenAndRewrite(node);

        groups.put(group, Group.withMember(rewritten));
        incrementReferenceCounts(rewritten, group);

        return group;
    }

    private int nextGroupId()
    {
        return nextGroupId++;
    }

    public int getGroupCount()
    {
        return groups.size();
    }

    private static final class Group
    {
        private final Multiset<Integer> incomingReferences = HashMultiset.create();
        private PlanNode membership;
        @Nullable
        private PlanNodeStatsEstimate stats;
        @Nullable
        private PlanCostEstimate cost;

        private Group(PlanNode member)
        {
            this.membership = requireNonNull(member, "member is null");
        }

        static Group withMember(PlanNode member)
        {
            return new Group(member);
        }
    }
}
