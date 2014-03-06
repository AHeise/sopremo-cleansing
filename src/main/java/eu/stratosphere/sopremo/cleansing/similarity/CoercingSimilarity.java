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

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

/**
 * @author Arvid Heise
 */
public class CoercingSimilarity extends AbstractSimilarity<IJsonNode> implements
		CompoundSimilarity<IJsonNode, Similarity<IJsonNode>> {
	private final Similarity<IJsonNode> actualSimilarity;

	private final Class<IJsonNode> coercionType;

	private transient NodeCache cache1 = new NodeCache(), cache2 = new NodeCache();

	@SuppressWarnings("unchecked")
	public CoercingSimilarity(Similarity<?> actualSimilarity) {
		this.actualSimilarity = (Similarity<IJsonNode>) actualSimilarity;
		this.coercionType = (Class<IJsonNode>) actualSimilarity.getExpectedType();
	}

	/**
	 * Initializes CoercingSimilarity.
	 */
	CoercingSimilarity() {
		this.actualSimilarity = null;
		this.coercionType = null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		SopremoUtil.append(appendable, this.actualSimilarity, "<", this.coercionType.getSimpleName(), ">");
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.actualSimilarity.hashCode();
		result = prime * result + this.coercionType.hashCode();
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
		CoercingSimilarity other = (CoercingSimilarity) obj;
		return this.coercionType.equals(other.coercionType) && this.actualSimilarity.equals(other.actualSimilarity);
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
	public float getSimilarity(IJsonNode node1, IJsonNode node2) {
		return this.actualSimilarity.getSimilarity(
			TypeCoercer.INSTANCE.coerce(node1, this.cache1, this.coercionType),
			TypeCoercer.INSTANCE.coerce(node2, this.cache2, this.coercionType));
	}
}
