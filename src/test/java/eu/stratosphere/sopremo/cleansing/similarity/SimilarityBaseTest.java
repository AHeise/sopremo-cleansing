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
import eu.stratosphere.sopremo.type.JavaToJsonMapper;
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

	private TransformedValue expected;

	protected EvaluationContext context;

	public SimilarityBaseTest(Object node1, Object node2, Object expected) {
		this.node1 = JavaToJsonMapper.INSTANCE.map(node1);
		this.node2 = JavaToJsonMapper.INSTANCE.map(node2);
		this.expected = expected instanceof Number ?
			new TransformedValue(((Number) expected).doubleValue(), null) :
			(TransformedValue) expected;
	}

	public abstract Similarity<?> getSimilarity();

	protected static TransformedValue withCoercion(double sim) {
		return new TransformedValue(sim, TransformedValue.Transformation.COERCE);
	}

	protected static TransformedValue withTokenization(double sim) {
		return new TransformedValue(sim, TransformedValue.Transformation.TOKENIZE);
	}

	private final static class TransformedValue {
		private enum Transformation {
			COERCE, TOKENIZE;
		};

		private Transformation transformation;

		private double sim;

		/**
		 * Initializes TransformedValue.
		 * 
		 * @param sim
		 * @param transformation
		 */
		public TransformedValue(double sim, Transformation transformation) {
			this.sim = sim;
			this.transformation = transformation;
		}

		/**
		 * Returns the sim.
		 * 
		 * @return the sim
		 */
		public double getSim() {
			return this.sim;
		}

		/**
		 * Returns the transformation.
		 * 
		 * @return the transformation
		 */
		public Transformation getTransformation() {
			return this.transformation;
		}
	}

	@Before
	public void setup() {
		this.context = new EvaluationContext();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSimilarity() throws Exception {
		Similarity<IJsonNode> similarity = (Similarity<IJsonNode>) getSimilarity();
		if (this.expected.getTransformation() == TransformedValue.Transformation.COERCE) {
			similarity = new CoercingSimilarity(similarity);
		}
		if (this.expected.getTransformation() == TransformedValue.Transformation.TOKENIZE) {
			similarity = new CoercingSimilarity(new TokenizingSimilarity((Similarity) similarity));
		}
		try {
			Assert.assertEquals(this.expected.getSim(), similarity.getSimilarity(this.node1, this.node2), 0.0001);
		} catch (Exception e) {
			if (!Double.isNaN(this.expected.getSim()))
				throw e;
		}

		if (similarity.isSymmetric() && !Double.isNaN(this.expected.getSim()))
			Assert.assertEquals("Not symmetric", this.expected.getSim(),
				similarity.getSimilarity(this.node2, this.node1), 0.0001);
	}
}
