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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

/**
 * @author Arvid Heise
 */
public class CoercingSimilarity extends AbstractSimilarity<IJsonNode> implements
		CompoundSimilarity<IJsonNode, Similarity<IJsonNode>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 669569819842928949L;

	private final Similarity<IJsonNode> actualSimilarity;

	private final Class<IJsonNode> coercionType;

	private IJsonNode coercedNode1, coercedNode2;

	@SuppressWarnings("unchecked")
	public CoercingSimilarity(Similarity<?> actualSimilarity) {
		this.actualSimilarity = (Similarity<IJsonNode>) actualSimilarity;
		this.coercionType = (Class<IJsonNode>) actualSimilarity.getExpectedType();
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
	public Iterator<Similarity<IJsonNode>> iterator() {
		return Collections.singleton(this.actualSimilarity).iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.CompoundSimilarity#getSubsimilarities()
	 */
	@Override
	public List<Similarity<IJsonNode>> getSubsimilarities() {
		return Collections.singletonList(this.actualSimilarity);
	}

	/**
	 * Returns the actualSimilarity.
	 * 
	 * @return the actualSimilarity
	 */
	public Similarity<IJsonNode> getActualSimilarity() {
		return this.actualSimilarity;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.Similarity#getSimilarity(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(IJsonNode node1, IJsonNode node2, EvaluationContext context) {
		return this.actualSimilarity.getSimilarity(
			this.coercedNode1 = TypeCoercer.INSTANCE.coerce(node1, this.coercedNode1, this.coercionType),
			this.coercedNode2 = TypeCoercer.INSTANCE.coerce(node2, this.coercedNode2, this.coercionType), context);
	}
}
