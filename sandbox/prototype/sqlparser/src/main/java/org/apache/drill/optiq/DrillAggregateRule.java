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

import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;

/**
 * Rule that converts an {@link AggregateRel} to
 * a {@link DrillAggregateRel},
 * implemented by a Drill "segment" operation
 * followed by a "collapseaggregate" operation.
 */
public class DrillAggregateRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillAggregateRule();

  private DrillAggregateRule() {
    super(
        new RelOptRuleOperand(
            AggregateRel.class,
            Convention.NONE,
            new RelOptRuleOperand(RelNode.class, ANY)),
        "DrillAggregateRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final AggregateRel aggregate = (AggregateRel) call.getRels()[0];
    final RelNode input = call.getRels()[1];
    final RelTraitSet traits =
        aggregate.getTraitSet().plus(DrillRel.CONVENTION);
    final RelNode convertedInput = convert(input, traits);
    call.transformTo(
        new DrillAggregateRel(
            aggregate.getCluster(), traits, convertedInput,
            aggregate.getGroupSet(), aggregate.getAggCallList()));
  }
}

// End DrillAggregateRule.java
