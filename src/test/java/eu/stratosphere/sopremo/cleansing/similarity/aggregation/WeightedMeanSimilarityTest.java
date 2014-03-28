/*
 * DuDe - The Duplicate Detection Toolkit
 * 
 * Copyright (C) 2010  Hasso-Plattner-Institut für Softwaresystemtechnik GmbH, *                     Potsdam, Germany 
 *
 * This file is part of DuDe.
 * 
 * DuDe is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * DuDe is distributed in the hope that it will be useful, * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with DuDe.  If not, see <http://www.gnu.org/licenses/>.
 * 
 */

package eu.stratosphere.sopremo.cleansing.similarity.aggregation;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.sopremo.EqualCloneTest;
import eu.stratosphere.sopremo.cleansing.similarity.ConstantSimilarity;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * Tests the {@link HarmonicMeanComparator} class.
 * 
 * @author Matthias Pohl
 */
public class WeightedMeanSimilarityTest extends EqualCloneTest<WeightedMeanSimilarity> {

	private final static float ACCURACY = 1e-6f;

	private final IJsonNode node1 = new ObjectNode(), node2 = new ObjectNode();

	/**
	 * Tests the behavior of {@link HarmonicMeanComparator#getSimilarity(IJsonNode, IJsonNode)} without weights.
	 */
	@Test
	public void harmonicMeanShouldBeZeroIfOnePartIsZero() {
		WeightedMeanSimilarity similarity = new WeightedMeanSimilarity(WeightedMeanSimilarity.HARMONIC_MEAN);

		assertEquals(Double.NaN, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(0.0f));

		double expectedValue = 0.0;
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(1.0f));

		expectedValue = 0.0;
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);
	}

	/**
	 * Tests the behavior of {@link HarmonicMeanComparator#getSimilarity(IJsonNode, IJsonNode)} without weights.
	 */
	@Test
	public void harmonicMeanShouldWorkWithoutWeights() {
		WeightedMeanSimilarity similarity = new WeightedMeanSimilarity(WeightedMeanSimilarity.HARMONIC_MEAN);

		assertEquals(Double.NaN, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(0.1f));

		float expectedValue = 1.0f / (1.0f / 0.1f);
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(0.5f));

		expectedValue = 2.0f / ((1.0f / 0.1f) + (1.0f / 0.5f));
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(0.5f));

		expectedValue = 3.0f / ((1.0f / 0.1f) + (1.0f / 0.5f) + (1.0f / 0.5f));
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(1.0f));

		expectedValue = 4.0f / ((1.0f / 0.1f) + (1.0f / 0.5f) + (1.0f / 0.5f) + (1.0f / 1.0f));
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);
	}

	/**
	 * Tests the behavior of {@link HarmonicMeanComparator#getSimilarity(IJsonNode, IJsonNode)} with weights.
	 */
	@Test
	public void harmonicMeanShouldWorkWithWeights() {
		WeightedMeanSimilarity similarity = new WeightedMeanSimilarity(WeightedMeanSimilarity.HARMONIC_MEAN);

		assertEquals(Double.NaN, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(0.1f), 2);

		double expectedValue = (2.0) / (2.0 / 0.1);
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(0.4f), 4);

		expectedValue = (2.0 + 4.0) / ((2.0 / 0.1) + (4.0 / 0.4));
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(0.4f), 1);

		expectedValue = (2.0 + 4.0 + 1.0) / ((2.0 / 0.1) + (4.0 / 0.4) + (1.0 / 0.4));
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);

		similarity.add(new ConstantSimilarity(1.0f), 10);

		expectedValue = (2.0 + 4.0 + 1.0 + 10.0) / ((2.0 / 0.1) + (4.0 / 0.4) + (1.0 / 0.4) + (10.0 / 1.0));
		assertEquals(expectedValue, similarity.getSimilarity(this.node1, this.node2), ACCURACY);
	}

	/**
	 * Tests the behavior of {@link WeightedMeanSimilarity#getSimilarity(IJsonNode, IJsonNode)} without weights.
	 */
	@Test
	public void arithmeticMeanShouldWorkWithoutWeights() {
		WeightedMeanSimilarity similarity = new WeightedMeanSimilarity();

		assertEquals(Double.NaN, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.0f));

		assertEquals(0.0, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.1f));

		assertEquals(0.05, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.5f));

		assertEquals(0.2, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.5f));

		assertEquals(0.275, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(1.0f));

		assertEquals(0.42, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);
	}

	/**
	 * Tests the behavior of {@link WeightedMeanSimilarity#getSimilarity(IJsonNode, IJsonNode)} with weights.
	 */
	@Test
	public void arithmeticMeanShouldWorkWithWeights() {
		WeightedMeanSimilarity similarity = new WeightedMeanSimilarity();

		assertEquals(Double.NaN, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.0f), 1);

		assertEquals(0.0, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.0f), 2);

		assertEquals(0.0, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.1f), 2);

		assertEquals(0.04, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.4f), 4);

		assertEquals(0.2, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(0.4f), 1);

		assertEquals(0.22, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);

		similarity.add(new ConstantSimilarity(1.0f), 10);

		assertEquals(0.61, similarity.getSimilarity(this.node1, this.node2), WeightedMeanSimilarityTest.ACCURACY);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.EqualVerifyTest#createDefaultInstance(int)
	 */
	@Override
	protected WeightedMeanSimilarity createDefaultInstance(int index) {
		return new WeightedMeanSimilarity(index);
	}

}
