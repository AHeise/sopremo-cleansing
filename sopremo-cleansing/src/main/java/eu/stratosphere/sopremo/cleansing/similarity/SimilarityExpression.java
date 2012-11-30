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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class SimilarityExpression extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8785651122862947081L;

	private final Similarity<IJsonNode> similarity;

	/**
	 * Initializes SimilarityExpression.
	 * 
	 * @param similarity
	 */
	public SimilarityExpression(Similarity<IJsonNode> similarity) {
		this.similarity = similarity;
	}

	/**
	 * Returns the similarity.
	 * 
	 * @return the similarity
	 */
	public Similarity<IJsonNode> getSimilarity() {
		return this.similarity;
	}

	private final transient DoubleNode sim = new DoubleNode();

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#createCopy()
	 */
	@Override
	protected EvaluationExpression createCopy() {
		return null;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode evaluate(IJsonNode node) {
		final IArrayNode pair = (IArrayNode) node;
		this.sim.setValue(this.similarity.getSimilarity(pair.get(0), pair.get(1)));
		return this.sim;
	}

}
