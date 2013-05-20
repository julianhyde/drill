/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.optiq;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import net.hydromatic.linq4j.Ord;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.Util;

import java.util.BitSet;
import java.util.List;

/**
 * Aggregation implemented in Drill.
 */
public class DrillAggregateRel extends AggregateRelBase implements DrillRel {
  /** Creates a DrillAggregateRel. */
  public DrillAggregateRel(RelOptCluster cluster, RelTraitSet traits,
      RelNode child, BitSet groupSet, List<AggregateCall> aggCalls) {
    super(cluster, traits, child, groupSet, aggCalls);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DrillAggregateRel(getCluster(), traitSet, sole(inputs),
        getGroupSet(), aggCalls);
  }

  @Override
  public int implement(DrillImplementor implementor) {
    int inputId = implementor.visitChild(this, 0, getChild());
    final List<String> childFields =
        RelOptUtil.getFieldNameList(getChild().getRowType());
    final List<String> fields = RelOptUtil.getFieldNameList(getRowType());
/*
    E.g. {
      op: "segment",
      ref: "segment",
	    exprs: ["deptId"]
	  }, {
	    op: "collapsingaggregate",
	    within: "segment",
	    carryovers: ["deptId"],
	    aggregations: [
	      {ref: "c", expr: "count(1)"}
	    ]
	  }
*/
    final ObjectNode segment = implementor.mapper.createObjectNode();
    segment.put("op", "segment");
    segment.put("input", inputId);
    // TODO: choose different name for field if there is already a field
    // called "segment"
    segment.put("ref", "segment");
    final ArrayNode exprs = implementor.mapper.createArrayNode();
    segment.put("exprs", exprs);
    for (int group : Util.toIter(groupSet)) {
      exprs.add(childFields.get(group));
    }

    final ObjectNode aggregate = implementor.mapper.createObjectNode();
    aggregate.put("op", "collapsingaggregate");
    aggregate.put("within", "segment");
    final ArrayNode carryovers = implementor.mapper.createArrayNode();
    aggregate.put("carryovers", carryovers);
    for (int group : Util.toIter(groupSet)) {
      carryovers.add(childFields.get(group));
    }
    final ArrayNode aggregations = implementor.mapper.createArrayNode();
    aggregate.put("aggregations", aggregations);
    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      final ObjectNode aggregation = implementor.mapper.createObjectNode();
      aggregation.put("ref", fields.get(groupSet.cardinality() + aggCall.i));
      aggregation.put("expr", toDrill(aggCall.e, childFields));
      aggregations.add(aggregation);
    }

    implementor.add(segment);
    return implementor.add(aggregate);
  }

  private String toDrill(AggregateCall call, List<String> fn) {
    final StringBuilder buf = new StringBuilder();
    buf.append(call.getAggregation().getName().toLowerCase())
        .append("(");
    for (Ord<Integer> arg : Ord.zip(call.getArgList())) {
      if (arg.i > 0) {
        buf.append(", ");
      }
      buf.append(fn.get(arg.e));
    }
    if (call.getArgList().isEmpty()) {
      buf.append("1"); // dummy arg to implement COUNT(*)
    }
    buf.append(")");
    return buf.toString();
  }
}

// End DrillAggregateRel.java
