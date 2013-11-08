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
package eu.stratosphere.sopremo.cleansing.duplicatedection;

import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;

/**
 * @author Arvid Heise
 */
public abstract class ElementaryDuplicateDetectionAlgorithm<ImplType extends ElementaryDuplicateDetectionAlgorithm<ImplType>>
		extends ElementaryOperator<ImplType> {
	private CandidateComparison candidateComparison = new CandidateComparison();

	/**
	 * Returns the value of candidateComparison.
	 * 
	 * @return the candidateComparison
	 */
	public CandidateComparison getCandidateComparison() {
		return this.candidateComparison;
	}

	/**
	 * Sets the value of candidateComparison to the given value.
	 * 
	 * @param candidateComparison
	 *        the candidateComparison to set
	 */
	public void setCandidateComparison(CandidateComparison candidateComparison) {
		if (candidateComparison == null)
			throw new NullPointerException("candidateComparison must not be null");

		this.candidateComparison = candidateComparison;
	}

	/**
	 * Sets the value of candidateComparison to the given value.
	 * 
	 * @param candidateComparison
	 *        the candidateComparison to set
	 * @return this
	 */
	public ImplType withCandidateComparison(CandidateComparison candidateComparison) {
		this.setCandidateComparison(candidateComparison);
		return this.self();
	}
}
