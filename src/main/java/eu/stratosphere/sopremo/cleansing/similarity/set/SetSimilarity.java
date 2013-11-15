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
package eu.stratosphere.sopremo.cleansing.similarity.set;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public abstract class SetSimilarity extends AbstractSimilarity<IArrayNode<IJsonNode>> {

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Class<IArrayNode<IJsonNode>> getExpectedType() {
		return (Class) IArrayNode.class;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.Similarity#getSimilarity(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public float getSimilarity(IArrayNode<IJsonNode> node1, IArrayNode<IJsonNode> node2) {
		final boolean node1Empty = node1.isEmpty(), node2Empty = node2.isEmpty();
		if (node1Empty || node2Empty)
			return node1Empty == node2Empty ? 1 : 0;
		return this.getSetSimilarity(node1, node2);
	}

	protected abstract float getSetSimilarity(IArrayNode<IJsonNode> node1, IArrayNode<IJsonNode> node2);

	protected int getNumberOfCommonTokens(IArrayNode<IJsonNode> node1, IArrayNode<IJsonNode> node2) {
		int commonTokens = 0;
		IArrayNode<IJsonNode> minNode = node1.size() < node2.size() ? node1 : node2, maxNode = node1 == minNode ? node2
			: node1;
		for (IJsonNode node : minNode)
			if (maxNode.contains(node))
				commonTokens++;
		return commonTokens;
	}

	protected Object2IntMap<IJsonNode> getTermFrequencies(IArrayNode<IJsonNode> node1,
			Object2IntMap<IJsonNode> termFrequencies) {
		termFrequencies.clear();
		for (IJsonNode node : node1)
			termFrequencies.put(node, termFrequencies.getInt(node) + 1);
		return termFrequencies;
	}
}
