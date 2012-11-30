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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the {@link WeightedAverageComparator} class.
 * 
 * @author Matthias Pohl
 */
public class WeightedAverageComparatorTest {

	private final static double ACCURACY = 0.00000000000001;
	
	private final IJsonNodePair pair = new IJsonNodePair(new IJsonNode(new JsonRecord(), "", ""), new IJsonNode(new JsonRecord(), "", ""));
	
	/**
	 * Tests the behavior of {@link WeightedAverageComparator#compareObjects(IJsonNode, IJsonNode)} without weights.
	 */
	@Test
	public void testCompareObjectsIJsonNodeIJsonNodeWithoutWeights() {
		WeightedAverageComparator comparator = new WeightedAverageComparator();
		
		assertEquals(1.0, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.0));
		
		assertEquals(0.0, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.1));
		
		assertEquals(0.05, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.5));
		
		assertEquals(0.2, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.5));
		
		assertEquals(0.275, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(1.0));
		
		assertEquals(0.42, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
	}
	
	/**
	 * Tests the behavior of {@link WeightedAverageComparator#compareObjects(IJsonNode, IJsonNode)} with weights.
	 */
	@Test
	public void testCompareObjectsIJsonNodeIJsonNodeWithWeights() {
		WeightedAverageComparator comparator = new WeightedAverageComparator();
		
		assertEquals(1.0, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.0), 1);
		
		assertEquals(0.0, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.0), 2);
		
		assertEquals(0.0, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.1), 2);
		
		assertEquals(0.04, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.4), 4);
		
		assertEquals(0.2, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.4), 1);
		
		assertEquals(0.22, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(1.0), 10);
		
		assertEquals(0.61, comparator.compareObjects(this.pair), WeightedAverageComparatorTest.ACCURACY);
	}

}
