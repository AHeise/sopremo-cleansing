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

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.aggregation.Aggregation;
import eu.stratosphere.sopremo.base.*;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.sopremo.type.*;

/**
 * 
 */
@InputCardinality(1)
@OutputCardinality(1)
@Name(verb = "global match")
public class GlobalMatching extends CompositeOperator<GlobalMatching> {
	/**
	 * 
	 */
	static final class IdMerger extends EvaluationExpression {

		@SuppressWarnings("unchecked")
		@Override
		public IJsonNode evaluate(IJsonNode node) {
			IArrayNode<IJsonNode> array = (IArrayNode<IJsonNode>) node;
			IJsonNode id = array.get(0);
			array.remove(0);
			for (int index = 0; index < array.size(); index++)
				((IArrayNode<IJsonNode>) array.get(index)).add(0, id);
			return array;
		}
	}

	private EvaluationExpression similarityExpression = new ConstantExpression(1),
			tieChooser = FunctionUtil.createFunctionCall(CoreFunctions.FIRST, EvaluationExpression.VALUE);

	public EvaluationExpression getSimilarityExpression() {
		return this.similarityExpression;
	}

	@Property
	@Name(preposition = "with")
	public void setSimilarityExpression(EvaluationExpression similarityExpression) {
		if (similarityExpression == null)
			throw new NullPointerException("similarityExpression must not be null");

		this.similarityExpression = similarityExpression;
	}

	public EvaluationExpression getTieChooser() {
		return this.tieChooser;
	}

	@Property
	@Name(verb = "tiebreak with")
	public void setTieChooser(EvaluationExpression tieChooser) {
		if (tieChooser == null)
			throw new NullPointerException("tieChooser must not be null");

		this.tieChooser = tieChooser;
	}

	public GlobalMatching withSimilarityExpression(EvaluationExpression similarityExpression) {
		setSimilarityExpression(similarityExpression);
		return this;
	}

	public GlobalMatching withTieChooser(EvaluationExpression tieChooser) {
		setTieChooser(tieChooser);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(eu.stratosphere.sopremo.operator.SopremoModule
	 * )
	 */
	@Override
	public void addImplementation(SopremoModule module) {
		final Source edges = module.getInput(0);

		// [v1, v2] -> [[v1, 0], [v2, 1]]
		final Projection positionEncodedEdges = new Projection().
			withInputs(edges).
			withResultProjection(new ArrayCreation(new ArrayCreation(new ArrayAccess(0), new ConstantExpression(0)),
				new ArrayCreation(new ArrayAccess(1), new ConstantExpression(1))));
		// -> [[v1, 0], [v2, 1], [v3, 0]]
		final TransitiveClosure closure = new TransitiveClosure().
			withInputs(positionEncodedEdges);

		// -> [id, [v1, 0], [v2, 1], [v3, 0]]
		final GlobalEnumeration ge = new GlobalEnumeration().
			withInputs(closure).
			withEnumerationExpression(GlobalEnumeration.PREPEND_ID);

		// -> [[id, v1, 0], [id, v2, 1], [id, v3, 0]]
		final Projection elementsWithId = new Projection().
			withInputs(ge).
			withResultProjection(new IdMerger());

		// -> [id, v1, 0]; [id, v2, 1]; [id, v3, 0]
		final ArraySplit splittedValues = new ArraySplit().
			withInputs(elementsWithId);

		// -> [id, v1, 0]; [id, v3, 0]
		final Selection filterLeft = new Selection().
			withInputs(splittedValues).
			withCondition(
				new ComparativeExpression(new ArrayAccess(2), BinaryOperator.EQUAL, new ConstantExpression(0)));

		// -> [id, v2, 1]
		final Selection filterRight = new Selection().
			withInputs(splittedValues).
			withCondition(
				new ComparativeExpression(new ArrayAccess(2), BinaryOperator.EQUAL, new ConstantExpression(1)));

		// [id, v1, 0] x [id, v2, 1] -> [id, v1, v2, sim]
		final TwoSourceJoin join = new TwoSourceJoin().
			withInputs(filterLeft, filterRight).
			withCondition(
				new ComparativeExpression(JsonUtil.createPath("0", "[0]"), BinaryOperator.EQUAL,
					JsonUtil.createPath("1", "[0]"))).
			withResultProjection(new ArrayCreation(
				JsonUtil.createPath("[1]", "[0]"),
				JsonUtil.createPath("[0]", "[1]"),
				JsonUtil.createPath("[1]", "[1]"),
				new ChainedSegmentExpression(
					new ArrayCreation(JsonUtil.createPath("[0]", "[1]"), JsonUtil.createPath("[1]", "[1]")),
					this.similarityExpression)));

		final Grouping bestInGroups = new Grouping().
			withInputs(join).
			withGroupingKey(new ArrayAccess(0)).
			withResultProjection(FunctionUtil.createFunctionCall(TAKE_BEST, new InputSelection(0))).
			withInnerGroupOrdering(0, new OrderingExpression(Order.DESCENDING, new ArrayAccess(3)));

		final ArraySplit bestMatches = new ArraySplit().
			withInputs(bestInGroups);
		
		module.getOutput(0).setInput(0, bestMatches);
	}
	

	public static final Aggregation TAKE_BEST = new Aggregation() {
		private transient Set<IJsonNode> matchedLeft = new HashSet<IJsonNode>(), matchedRight = new HashSet<IJsonNode>(); 
		
		private transient IArrayNode<IJsonNode> matches = new ArrayNode<IJsonNode>();
		
		@Override
		public void initialize() {
			this.matchedLeft.clear();
			this.matchedRight.clear();
			this.matches.clear();
		}
		
		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.aggregation.Aggregation#aggregate(eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void aggregate(IJsonNode element) {
			// [id, v1, v2, sim]
			IArrayNode<IJsonNode> match = (IArrayNode<IJsonNode>) element;
			if(!this.matchedLeft.contains(match.get(1)) && !this.matchedRight.contains(match.get(2))) {
				match.remove(0);
				match.remove(2);
				final IJsonNode left = match.get(0).clone();
				final IJsonNode right = match.get(1).clone();
				this.matchedLeft.add(left);
				this.matchedRight.add(right);
				this.matches.add(JsonUtil.asArray(left, right));
			}
		}
		
		@Override
		public IJsonNode getFinalAggregate() {
			this.matchedLeft.clear();
			this.matchedRight.clear();
			return this.matches;
		}
	};
}
