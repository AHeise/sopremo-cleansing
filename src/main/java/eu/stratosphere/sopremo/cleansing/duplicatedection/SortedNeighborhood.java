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
package eu.stratosphere.sopremo.cleansing.duplicatedection;

import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.duplicatedection.Blocking.DirectBlocking;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
@InputCardinality(min = 1, max = 2)
@OutputCardinality(1)
@Name(noun = {"snm", "sorted neighborhood"})
public class SortedNeighborhood extends MultipassDuplicateDetectionAlgorithm {

	@Override
	protected Operator<?> createPass(Pass pass, CandidateComparison comparison) {
		return new SinglePass().
			withKeyExpression(0, pass.getBlockingKeys().get(0)).
			withKeyExpression(1, pass.getBlockingKeys().get(comparison.isInnerSource() ? 0 : 1)).
			withCondition(comparison.asCondition()).
			withResultProjection(comparison.getResultProjectionWithSimilarity());
	}

	@InputCardinality(2)
	public static class SinglePass extends ElementaryDuplicateDetectionAlgorithm<SinglePass> {
		public static class Implementation extends SopremoMatch {
			private BooleanExpression condition;

			private transient IArrayNode<IJsonNode> pair = new ArrayNode<IJsonNode>();

			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.pact.GenericSopremoMatch#match(eu.stratosphere.sopremo.type.IJsonNode,
			 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
			 */
			@Override
			protected void match(IJsonNode left, IJsonNode right, JsonCollector collector) {
				this.pair.set(0, left);
				this.pair.set(1, right);
				if (this.condition.evaluate(this.pair) == BooleanNode.TRUE)
					collector.collect(this.pair);
			}
		}
	}
}