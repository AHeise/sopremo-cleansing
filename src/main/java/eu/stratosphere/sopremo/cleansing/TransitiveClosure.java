/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.GlobalEnumeration;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.ValueSplit;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.IterativeOperator;
import eu.stratosphere.sopremo.operator.IterativeSopremoModule;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * 
 */
@InputCardinality(1)
@OutputCardinality(1)
public class TransitiveClosure extends IterativeOperator<TransitiveClosure> {
	/**
	 * Initializes TransitiveClosure.
	 *
	 */
	public TransitiveClosure() {
		setSolutionSetKeyExpressions(new ArrayAccess(0));
	}
	
	@Override
	public void addImplementation(IterativeSopremoModule iterativeSopremoModule, EvaluationContext context) {
		final Source edges = iterativeSopremoModule.getInput(0);

		// [v1, v2] -> [v1, v2, id]
		final GlobalEnumeration pairWithComponentId = new GlobalEnumeration().
			withInputs(edges).
			withEnumerationExpression(new ArrayAccess(2));

		// [v1, v2, id] -> [v1, id]; [v2, id] -> initial workset
		final ValueSplit vertexWithComponentId = new ValueSplit().
			withInputs(pairWithComponentId).
			withProjections(new ArrayCreation(new ArrayAccess(0), new ArrayAccess(2)),
				new ArrayCreation(new ArrayAccess(1), new ArrayAccess(2)));

		iterativeSopremoModule.setInitialSolutionSet(vertexWithComponentId);
		iterativeSopremoModule.setInitialWorkingset(vertexWithComponentId);
		
		// here starts the iteration
		// [v1, id] |X| [v1, v2] -> [v2, id]
		final Join neighborWithComponentId = new Join().withName("neighborWithComponentId").
			withInputs(iterativeSopremoModule.getWorkingSet(), edges).
			withJoinCondition(new ComparativeExpression(JsonUtil.createPath("0", "[0]"),
				BinaryOperator.EQUAL, JsonUtil.createPath("1", "[0]"))).
			withResultProjection(new ArrayCreation(JsonUtil.createPath("1", "[1]"), JsonUtil.createPath("0", "[1]")));

		// [v, id1], ..., [v, idn] -> [v, min(id1, ... idn)]
		final BatchAggregationExpression bae = new BatchAggregationExpression();
		final Grouping minCandidateId = new Grouping().withName("minCandidateId").
			withInputs(neighborWithComponentId).
			withGroupingKey(new ArrayAccess(0)).
			withResultProjection(new ArrayCreation(
				bae.add(CoreFunctions.FIRST, new ArrayAccess(0)),
				bae.add(CoreFunctions.MIN, new ArrayAccess(1))));

		// [v, current-id] |X| [v, candidate-id] -> [v, candidate-id] if candidate-id < current-id
		final Join currentAndCandidateIds = new Join().withName("currentAndCandidateIds").
			withInputs(minCandidateId, iterativeSopremoModule.getSolutionSet()).
			withJoinCondition(new ComparativeExpression(JsonUtil.createPath("0", "[0]"),
				BinaryOperator.EQUAL, JsonUtil.createPath("1", "[0]")));
		final Selection changedIds =
			new Selection().
				withInputs(currentAndCandidateIds).
				withCondition(
					new ComparativeExpression(JsonUtil.createPath("[1]", "[1]"), BinaryOperator.LESS,
						JsonUtil.createPath("[0]", "[1]"))).
				withResultProjection(new ArrayAccess(1));

		iterativeSopremoModule.setNextWorkset(changedIds);
		iterativeSopremoModule.setSolutionSetDelta(changedIds);
		iterativeSopremoModule.setMaxNumberOfIterations(3);

		final Grouping clustering = new Grouping().
			withInputs(iterativeSopremoModule.getSolutionSet()).
			withGroupingKey(new ArrayAccess(1)).
			withResultProjection(FunctionUtil.createFunctionCall(CoreFunctions.ALL, new ArrayAccess(1)));
		iterativeSopremoModule.getOutput(0).setInput(0, clustering);
	}
}
