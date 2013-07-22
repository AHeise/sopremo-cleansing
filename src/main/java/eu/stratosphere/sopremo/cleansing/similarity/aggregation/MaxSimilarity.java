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

import java.util.Collection;

import eu.stratosphere.sopremo.cleansing.similarity.Similarity;

/**
 * <code>MaxSimilarity</code> calculates the similarities of each collected Similarity and returns the maximum value.
 * 
 * @author Arvid Heise
 */
public class MaxSimilarity extends AggregationSimilarity {

	/**
	 * Initializes MaxSimilarity with no sub-similarity.
	 */
	public MaxSimilarity() {
	}

	/**
	 * Initializes MaxSimilarity with a number of sub-similarities.
	 * 
	 * @param similarities
	 *        The sub-similarities.
	 */
	public MaxSimilarity(Collection<? extends Similarity<?>> similarities) {
		super(similarities);
	}

	/**
	 * Initializes MaxSimilarity with a number of sub-similarities.
	 * 
	 * @param similarities
	 *        The sub-similarities.
	 */
	public MaxSimilarity(Similarity<?>... similarities) {
		super(similarities);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.aggregation.AggregationSimilarity#aggregateSimilarity(double[],
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	protected double aggregateSimilarity(double[] individualSimilarities) {
		double max = 0;
		for (double sim : individualSimilarities)
			max = Math.max(sim, max);
		return max;
	}
}
