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

import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class ConstantSimilarity extends AbstractSimilarity<IJsonNode> {
	private float similarity;

	public ConstantSimilarity(float similarity) {
		this.checkSimilarity(similarity);

		this.similarity = similarity;
	}
	
	/**
	 * Initializes ConstantSimilarity.
	 *
	 */
	ConstantSimilarity() {
	}

	public double getSimilarity() {
		return this.similarity;
	}

	public void setSimilarity(float similarity) {
		this.checkSimilarity(similarity);

		this.similarity = similarity;
	}

	/**
	 * @param similarity2
	 */
	private void checkSimilarity(double similarity) {
		if (similarity < 0 || similarity > 1)
			throw new IllegalArgumentException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.Similarity#getExpectedType()
	 */
	@Override
	public Class<IJsonNode> getExpectedType() {
		return IJsonNode.class;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.Similarity#getSimilarity(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public float getSimilarity(IJsonNode node1, IJsonNode node2) {
		return this.similarity;
	}
}
