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

import java.io.IOException;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public final class OrderedPairsFilter extends Preselection {
	/**
	 * 
	 */
	private final CandidateComparison candidateComparison;

	/**
	 * 
	 */
	private static final long serialVersionUID = -3698801486437497443L;

	private final EvaluationExpression leftExp, rightExp;

	/**
	 * Initializes OrderedPairsFilter.
	 * 
	 * @param candidateComparison
	 */
	public OrderedPairsFilter(CandidateComparison candidateComparison) {
		this.candidateComparison = candidateComparison;
		leftExp = this.candidateComparison.getLeftIdProjection();
		rightExp = this.candidateComparison.getRightIdProjection();
	}

	@Override
	public boolean shouldProcess(IJsonNode left, IJsonNode right) {
		return this.leftExp.evaluate(left).compareTo(this.rightExp.evaluate(right)) < 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new OrderedPairsFilter(this.candidateComparison);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("Ordered pairs filter");
	}
}