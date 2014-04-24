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

import java.io.IOException;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class SimilarityExpression extends EvaluationExpression {

	private final Similarity<IJsonNode> similarity;
	private final EvaluationExpression leftPath;
	private final EvaluationExpression rightPath;

	SimilarityExpression() {
		this(null, null, null);
	}

	/**
	 * Initializes SimilarityExpression.
	 * 
	 * @param similarity
	 */
	@SuppressWarnings("unchecked")
	public SimilarityExpression(EvaluationExpression leftPath, Similarity<? extends IJsonNode> similarity,EvaluationExpression rightPath) {
		this.leftPath = leftPath;
		this.similarity = (Similarity<IJsonNode>) similarity;
		this.rightPath = rightPath;
	}

	public EvaluationExpression getLeftPath() {
		return leftPath;
	}

	public EvaluationExpression getRightPath() {
		return rightPath;
	}

	/**
	 * Returns the similarity.
	 * 
	 * @return the similarity
	 */
	public Similarity<IJsonNode> getSimilarity() {
		return this.similarity;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		this.similarity.appendAsString(appendable);
	}

	private transient DoubleNode result = new DoubleNode();

	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.similarity.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SimilarityExpression other = (SimilarityExpression) obj;
		return this.similarity.equals(other.similarity);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.
	 * stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode evaluate(IJsonNode node) {
		this.result.setValue(this.similarity.getSimilarity(leftPath.evaluate(node), rightPath.evaluate(node)));
		return this.result;
	}
}
