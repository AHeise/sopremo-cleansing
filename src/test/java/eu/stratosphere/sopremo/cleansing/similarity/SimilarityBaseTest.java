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

package eu.stratosphere.sopremo.cleansing.similarity;

import org.junit.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * <code>SimilarityBaseTest</code> provides {@link IJsonNode} test instances that can be used by all comparator tests.
 * 
 * @author Matthias Pohl
 */
@Ignore
@RunWith(Parameterized.class)
public abstract class SimilarityBaseTest {
	protected static final double INCOMPARABLE = Double.NaN;

	private IJsonNode node1, node2;

	private double expected;

	protected EvaluationContext context;

	public SimilarityBaseTest(Object node1, Object node2, double expected) {
		this.node1 = JsonUtil.createValueNode(node1);
		this.node2 = JsonUtil.createValueNode(node2);
		this.expected = expected;
	}

	public abstract Similarity<?> getSimilarity();

	protected static double withCoercion(double sim) {
		return -sim - 1;
	}

	@Before
	public void setup() {
		this.context = new EvaluationContext();
	}

	@Test
	public void testSimilarity() throws Exception {
		@SuppressWarnings("unchecked")
		Similarity<IJsonNode> similarity = (Similarity<IJsonNode>) getSimilarity();
		if (this.expected < 0) {
			similarity = new CoercingSimilarity(similarity);
			this.expected = -this.expected - 1;
		}
		try {
			Assert.assertEquals(this.expected, similarity.getSimilarity(this.node1, this.node2), 0.0001);
		} catch (Exception e) {
			if (!Double.isNaN(this.expected))
				throw e;
		}

		if (similarity.isSymmetric() && !Double.isNaN(this.expected))
			Assert.assertEquals("Not symmetric", this.expected, similarity.getSimilarity(this.node2, this.node1), 0.0001);
	}
}
