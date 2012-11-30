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
package eu.stratosphere.sopremo.cleansing.similarity;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class PathSimilarity<NodeType extends IJsonNode> extends AbstractSimilarity<IJsonNode> implements
		CompoundSimilarity<IJsonNode, Similarity<NodeType>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3168384409276920868L;

	private final PathSegmentExpression leftExpression, rightExpression;

	private final Similarity<NodeType> actualSimilarity;

	public PathSimilarity(PathSegmentExpression leftExpression, Similarity<NodeType> actualSimilarity,
			PathSegmentExpression rightExpression) {
		this.leftExpression = leftExpression;
		this.actualSimilarity = actualSimilarity;
		this.rightExpression = rightExpression;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	 */
	@Override
	public Class<IJsonNode> getExpectedType() {
		return IJsonNode.class;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<Similarity<NodeType>> iterator() {
		return Collections.singleton(this.actualSimilarity).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.CompoundSimilarity#getSubsimilarities()
	 */
	@Override
	public List<Similarity<NodeType>> getSubsimilarities() {
		return Collections.singletonList(this.actualSimilarity);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new PathSimilarity<NodeType>(leftExpression.clone(), actualSimilarity.clone(), rightExpression.clone());
	}

	/**
	 * Returns the leftExpression.
	 * 
	 * @return the leftExpression
	 */
	public PathSegmentExpression getLeftExpression() {
		return this.leftExpression;
	}

	/**
	 * Returns the rightExpression.
	 * 
	 * @return the rightExpression
	 */
	public PathSegmentExpression getRightExpression() {
		return this.rightExpression;
	}

	/**
	 * Returns the actualSimilarity.
	 * 
	 * @return the actualSimilarity
	 */
	public Similarity<NodeType> getActualSimilarity() {
		return this.actualSimilarity;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.Similarity#getSimilarity(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public double getSimilarity(IJsonNode node1, IJsonNode node2) {
		final NodeType left = (NodeType) this.leftExpression.evaluate(node1);
		if (left.isMissing())
			return 0;
		// two missing nodes are still not similar in contrast to two null nodes
		final NodeType right = (NodeType) this.rightExpression.evaluate(node2);
		if (right.isMissing())
			return 0;
		return this.actualSimilarity.getSimilarity(left, right);
	}
}
