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
 * Tests the {@link HarmonicMeanComparator} class.
 * 
 * @author Matthias Pohl
 */
public class HarmonicMeanComparatorTest {
	
	private final static double ACCURACY = 0.00000000000001;
	
	private final IJsonNodePair pair = new IJsonNodePair(new IJsonNode(new JsonRecord(), "", ""), new IJsonNode(new JsonRecord(), "", ""));
	
	/**
	 * Tests the behavior of {@link HarmonicMeanComparator#compareObjects(IJsonNode, IJsonNode)} without weights.
	 */
	@Test
	public void testCompareObjectsIJsonNodeIJsonNodeWithoutWeights() {
		HarmonicMeanComparator comparator = new HarmonicMeanComparator();
		
		assertEquals(1.0, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.0));
		
		double expectedValue = 0.0;
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.1));
		
		expectedValue = 2.0 / (1.0/0.1);
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.5));
		
		expectedValue = 3.0 / ( (1.0/0.1) + (1.0/0.5) );
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.5));
		
		expectedValue = 4.0 / ( (1.0/0.1) + (1.0/0.5) + (1.0/0.5));
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(1.0));
		
		expectedValue = 5.0 / ( (1.0/0.1) + (1.0/0.5) + (1.0/0.5) + (1.0/1.0));
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
	}
	
	/**
	 * Tests the behavior of {@link HarmonicMeanComparator#compareObjects(IJsonNode, IJsonNode)} with weights.
	 */
	@Test
	public void testCompareObjectsIJsonNodeIJsonNodeWithWeights() {
		HarmonicMeanComparator comparator = new HarmonicMeanComparator();
		
		assertEquals(1.0, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.0), 1);

		double expectedValue = 0.0;
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.0), 2);

		expectedValue = 0.0;
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.1), 2);
		
		expectedValue = (1.0 + 2.0 + 2.0) / (2.0/0.1);
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.4), 4);

		expectedValue = (1.0 + 2.0 + 2.0 + 4.0) / ( (2.0/0.1) + (4.0/0.4) );
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(0.4), 1);

		expectedValue = (1.0 + 2.0 + 2.0 + 4.0 + 1.0) / ( (2.0/0.1) + (4.0/0.4) + (1.0/0.4) );
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
		
		comparator.add(new ConstantComparator(1.0), 10);

		expectedValue = (1.0 + 2.0 + 2.0 + 4.0 + 1.0 + 10.0) / ( (2.0/0.1) + (4.0/0.4) + (1.0/0.4) + (10.0/1.0) );
		assertEquals(expectedValue, comparator.compareObjects(this.pair), HarmonicMeanComparatorTest.ACCURACY);
	}

}
