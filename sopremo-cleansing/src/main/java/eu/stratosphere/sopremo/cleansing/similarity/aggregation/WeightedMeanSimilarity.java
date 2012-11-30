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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleListIterator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.ISopremoType;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * <code>HarmonicMeanSimilarity</code> calculates the similarities of each collected Similarity and returns the weighted
 * harmonic mean of them.
 * 
 * @author Arvid Heise
 */
public class WeightedMeanSimilarity extends AggregationSimilarity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8627665157209879569L;

	private DoubleArrayList weights = new DoubleArrayList();

	public final static double ARITHMETIC_MEAN = 1, HARMONIC_MEAN = -1, GEOMETRIC_MEAN = 0.001, QUADRATIC_MEAN = 2,
			MAXIMUM = 1000, MINIMUM = -1000;

	private double m = ARITHMETIC_MEAN, weightSum = 0;

	public double getM() {
		return this.m;
	}

	public void setM(double m) {
		this.m = m;
	}

	/**
	 * Initializes WeightedMeanSimilarity.
	 */
	public WeightedMeanSimilarity() {
	}

	public WeightedMeanSimilarity(Similarity<?>... similarities) {
		super(similarities);
		this.weights.size(similarities.length);
		Arrays.fill(this.weights.elements(), 1);
		this.weightSum = similarities.length;
	}

	public WeightedMeanSimilarity(Collection<? extends Similarity<?>> similarities, Collection<Double> weights) {
		super(similarities);
		if (similarities.size() != weights.size())
			throw new IllegalArgumentException();
		this.weights.addAll(weights);
		final DoubleListIterator iterator = this.weights.iterator();
		while (iterator.hasNext())
			this.weightSum += iterator.nextDouble();
	}

	public WeightedMeanSimilarity(Map<Similarity<?>, Double> similarities) {
		super(similarities.keySet());
		for (Double weight : similarities.values()) {
			this.weights.add(weight);
			this.weightSum += weight;
		}
	}

	/**
	 * Adds a {@link Similarity} with a weight of 1.
	 * 
	 * @param similarity
	 *        The <code>Similarity</code> that shall be added.
	 */
	@Override
	public void add(Similarity<?> similarity) {
		this.add(similarity, 1);
	}

	/**
	 * Adds a {@link Similarity} with the given weight.
	 * 
	 * @param similarity
	 *        The <code>Similarity</code> that shall be added.
	 * @param weight
	 *        The weight of the similarity
	 */
	public void add(Similarity<?> similarity, double weight) {
		super.add(similarity);
		this.weights.add(weight);
		this.weightSum += weight;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new WeightedMeanSimilarity(SopremoUtil.deepClone(getSubsimilarities()), weights);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#copyPropertiesFrom(eu.stratosphere.sopremo.ISopremoType)
	 */
	@Override
	public void copyPropertiesFrom(ISopremoType original) {
		super.copyPropertiesFrom(original);
		this.m = ((WeightedMeanSimilarity) original).getM();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.aggregation.AggregationSimilarity#aggregateSimilarity(double[],
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected double aggregateSimilarity(double[] individualSimilarities) {
		double mean = 0;
		for (int index = 0; index < individualSimilarities.length; index++)
			mean += Math.pow(individualSimilarities[index] * this.weights.get(index), this.m);
		return Math.pow(mean / this.weightSum, 1 / this.m);
	}
}
