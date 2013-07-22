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

import eu.stratosphere.sopremo.cleansing.similarity.ConstantSimilarity;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * Tests the {@link MaxSimilarity} class.
 * 
 * @author Matthias Pohl
 */
public class MaxSimilarityTest {
	
	private final IJsonNode node1 = new ObjectNode(), node2 = new ObjectNode();

	/**
	 * Tests the behavior of {@link MaxSimilarity#getSimilarity(IJsonNode, IJsonNode)}.
	 */
	@Test
	public void testCompareObjectsIJsonNodeIJsonNode() {
		MaxSimilarity similarity = new MaxSimilarity();
		
		assertEquals(Double.NaN, similarity.getSimilarity(this.node1, this.node2), 0.0);
		
		similarity.add(new ConstantSimilarity(0.0));
		
		assertEquals(0.0, similarity.getSimilarity(this.node1, this.node2), 0.0);
		
		similarity.add(new ConstantSimilarity(0.1));
		
		assertEquals(0.1, similarity.getSimilarity(this.node1, this.node2), 0.0);
		
		similarity.add(new ConstantSimilarity(0.5));
		
		assertEquals(0.5, similarity.getSimilarity(this.node1, this.node2), 0.0);
		
		similarity.add(new ConstantSimilarity(0.5));
		
		assertEquals(0.5, similarity.getSimilarity(this.node1, this.node2), 0.0);
		
		similarity.add(new ConstantSimilarity(1.0));
		
		assertEquals(1.0, similarity.getSimilarity(this.node1, this.node2), 0.0);
	}

}
