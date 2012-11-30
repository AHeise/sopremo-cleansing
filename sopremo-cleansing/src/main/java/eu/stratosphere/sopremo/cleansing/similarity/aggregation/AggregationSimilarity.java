/*
 * DuDe - The Duplicate Detection Toolkit
 * 
 * Copyright (C) 2010  Hasso-Plattner-Institut für Softwaresystemtechnik GmbH,
 *                     Potsdam, Germany 
 *
 * This file is part of DuDe.
 * 
 * DuDe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * DuDe is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with DuDe.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package eu.stratosphere.sopremo.cleansing.similarity.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.CompoundSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * <code>AggregationSimilarity</code> aggregates the individual similarity values of other {@link Similarity}s.
 * 
 * @author Arvid Heise
 */
public abstract class AggregationSimilarity extends AbstractSimilarity<IJsonNode> implements CompoundSimilarity<IJsonNode, Similarity<IJsonNode>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1611709871788111311L;

	private final List<Similarity<IJsonNode>> similarities = new ArrayList<Similarity<IJsonNode>>();

	private double[] individualSimilarities;

	private Class<IJsonNode> expectedType;

	/**
	 * Initializes AggregationSimilarity.
	 */
	public AggregationSimilarity() {
		this.individualSimilarities = new double[0];
	}

	/**
	 * Initializes a <code>AggregationSimilarity</code> with a number of sub-similarities.
	 * 
	 * @param similarities
	 *        The sub-similarities.
	 */
	@SuppressWarnings("unchecked")
	public AggregationSimilarity(Similarity<?>... similarities) {
		this.similarities.addAll((Collection<? extends Similarity<IJsonNode>>) Arrays.asList(similarities));
		this.inferExpectedType();
		this.individualSimilarities = new double[this.similarities.size()];
	}

	/**
	 * Initializes a <code>AggregationSimilarity</code> with a number of sub-similarities.
	 * 
	 * @param similarities
	 *        The sub-similarities.
	 */
	@SuppressWarnings("unchecked")
	public AggregationSimilarity(Collection<? extends Similarity<?>> similarities) {
		this.similarities.addAll((Collection<? extends Similarity<IJsonNode>>) similarities);
		this.inferExpectedType();
		this.individualSimilarities = new double[this.similarities.size()];
	}

	/**
	 * Adds a {@link Similarity}.
	 * 
	 * @param similarity
	 *        The <code>Similarity</code> that shall be added.
	 */
	@SuppressWarnings("unchecked")
	public void add(Similarity<?> similarity) {
		this.similarities.add((Similarity<IJsonNode>) similarity);
		this.inferExpectedType();
		this.individualSimilarities = new double[this.similarities.size()];
	}

	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	private void inferExpectedType() {
		this.expectedType = IJsonNode.class;
		for (Similarity<?> similarity : this.similarities)
			if (similarity.getExpectedType() != this.expectedType) {
				if (!this.expectedType.isAssignableFrom(similarity.getExpectedType()))
					throw new IllegalArgumentException(
						String.format(
							"Incompatible similarity measures used; use coercing to decide for one of the following types: %s, %s",
							this.expectedType, similarity.getExpectedType()));
				this.expectedType = (Class<IJsonNode>) similarity.getExpectedType();
			}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.Similarity#getSimilarity(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(IJsonNode node1, IJsonNode node2) {
		for (int index = 0; index < this.individualSimilarities.length; index++)
			this.individualSimilarities[index] = this.similarities.get(index).getSimilarity(node1, node2);
		return this.aggregateSimilarity(this.individualSimilarities);
	}

	protected abstract double aggregateSimilarity(double[] individualSimilarities);

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<Similarity<IJsonNode>> iterator() {
		return this.similarities.iterator();
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.CompoundSimilarity#getSubsimilarities()
	 */
	@Override
	public List<Similarity<IJsonNode>> getSubsimilarities() {
		return this.similarities;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity#isSymmetric()
	 */
	@Override
	public boolean isSymmetric() {
		return super.isSymmetric();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	 */
	@Override
	public Class<IJsonNode> getExpectedType() {
		return this.expectedType;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final AggregationSimilarity other = (AggregationSimilarity) obj;
		return this.similarities.equals(other.similarities);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.similarities.hashCode();
		return result;
	}

}
