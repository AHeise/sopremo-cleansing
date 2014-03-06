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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;

/**
 * @author Arvid Heise
 */
public class PathSimilarity<NodeType extends IJsonNode> extends AbstractSimilarity<IJsonNode> implements
		CompoundSimilarity<IJsonNode, Similarity<NodeType>> {
	private final PathSegmentExpression leftExpression, rightExpression;

	private final Similarity<NodeType> actualSimilarity;

	public PathSimilarity(PathSegmentExpression leftExpression, Similarity<NodeType> actualSimilarity,
			PathSegmentExpression rightExpression) {
		this.leftExpression = leftExpression;
		this.actualSimilarity = actualSimilarity;
		this.rightExpression = rightExpression;
	}

	/**
	 * Initializes PathSimilarity.
	 */
	PathSimilarity() {
		this.leftExpression = null;
		this.actualSimilarity = null;
		this.rightExpression = null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		if (this.leftExpression.equals(this.rightExpression))
			SopremoUtil.append(appendable, this.actualSimilarity, '(', this.leftExpression, ')');
		else
			SopremoUtil.append(appendable, this.actualSimilarity, '(', this.leftExpression, ',', this.rightExpression,
				')');
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.actualSimilarity.hashCode();
		result = prime * result + this.leftExpression.hashCode();
		result = prime * result + this.rightExpression.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		PathSimilarity<?> other = (PathSimilarity<?>) obj;
		return this.actualSimilarity.equals(other.actualSimilarity) &&
			this.leftExpression.equals(other.leftExpression) && this.rightExpression.equals(other.rightExpression);
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
	@SuppressWarnings("unchecked")
	@Override
	public float getSimilarity(IJsonNode node1, IJsonNode node2) {
		final NodeType left = (NodeType) this.leftExpression.evaluate(node1);
		if (left == MissingNode.getInstance())
			return 0;
		// two missing nodes are still not similar in contrast to two null nodes
		final NodeType right = (NodeType) this.rightExpression.evaluate(node2);
		if (right == MissingNode.getInstance())
			return 0;
		return this.actualSimilarity.getSimilarity(left, right);
	}
}
