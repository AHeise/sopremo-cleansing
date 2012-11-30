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
package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionAlgorithmFactory;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * @author Arvid Heise
 */
@InputCardinality(1)
@OutputCardinality(1)
public class DuplicateDetection extends CompositeOperator<DuplicateDetection> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5097323266252206628L;

	private CandidateSelection candidateSelection = new CandidateSelection();

	private CandidateComparison comparison = new CandidateComparison().withInnerSource(true);

	/**
	 * Returns the candidateSelection.
	 * 
	 * @return the candidateSelection
	 */
	public CandidateSelection getCandidateSelection() {
		return this.candidateSelection;
	}

	/**
	 * Sets the candidateSelection to the specified value.
	 * 
	 * @param candidateSelection
	 *        the candidateSelection to set
	 */
	public void setCandidateSelection(CandidateSelection candidateSelection) {
		if (candidateSelection == null)
			throw new NullPointerException("candidateSelection must not be null");

		this.candidateSelection = candidateSelection;
	}

	/**
	 * Returns the value of comparison.
	 * 
	 * @return the comparison
	 */
	public CandidateComparison getComparison() {
		return this.comparison;
	}

	/**
	 * Sets the value of comparison to the given value.
	 * 
	 * @param comparison
	 *        the comparison to set
	 */
	public void setComparison(CandidateComparison comparison) {
		if (comparison == null)
			throw new NullPointerException("comparison must not be null");

		this.comparison = comparison;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		final CompositeDuplicateDetectionAlgorithm<?> algorithm =
			DuplicateDetectionAlgorithmFactory.INSTANCE.getAlgorithm(this.candidateSelection);
		algorithm.setComparison(this.comparison);
		module.embed(algorithm);
	}
}
