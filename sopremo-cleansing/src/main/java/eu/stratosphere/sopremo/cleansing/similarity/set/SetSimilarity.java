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
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public abstract class SetSimilarity extends AbstractSimilarity<IArrayNode> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6458014392152370570L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	 */
	@Override
	public Class<IArrayNode> getExpectedType() {
		return IArrayNode.class;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.Similarity#getSimilarity(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(IArrayNode node1, IArrayNode node2, EvaluationContext context) {
		final boolean node1Empty = node1.isEmpty(), node2Empty = node2.isEmpty();
		if (node1Empty || node2Empty)
			return node1Empty == node2Empty ? 1 : 0;
		return getSetSimilarity(node1, node2, context);
	}

	protected abstract double getSetSimilarity(IArrayNode node1, IArrayNode node2, EvaluationContext context);

	protected int getNumberOfCommonTokens(IArrayNode node1, IArrayNode node2) {
		int commonTokens = 0;
		IArrayNode minNode = node1.size() < node2.size() ? node1 : node2, maxNode = node1 == minNode ? node2 : node1;
		for (IJsonNode node : minNode)
			if (maxNode.contains(node))
				commonTokens++;
		return commonTokens;
	}

	protected Object2IntMap<IJsonNode> getTermFrequencies(IArrayNode node1) {
		final Object2IntOpenHashMap<IJsonNode> termFrequencies = new Object2IntOpenHashMap<IJsonNode>(node1.size());
		termFrequencies.defaultReturnValue(0);
		for (IJsonNode node : node1)
			termFrequencies.put(node, termFrequencies.get(node) + 1);
		return termFrequencies;
	}
}
