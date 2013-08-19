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

import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * @author Arvid Heise
 */
public abstract class CompositeDuplicateDetectionAlgorithm<ImplType extends CompositeDuplicateDetectionAlgorithm<ImplType>>
		extends CompositeOperator<ImplType> {
	public CompositeDuplicateDetectionAlgorithm() {
		super();
	}

	public CompositeDuplicateDetectionAlgorithm(int minInputs, int maxInputs, int minOutputs, int maxOutputs) {
		super(minInputs, maxInputs, minOutputs, maxOutputs);
	}

	public CompositeDuplicateDetectionAlgorithm(int numberOfInputs, int numberOfOutputs) {
		super(numberOfInputs, numberOfOutputs);
	}

	private CandidateComparison comparison = new CandidateComparison();

	private CandidateSelection candidateSelection = new CandidateSelection();

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
	 * Returns the candidateSelection.
	 * 
	 * @return the candidateSelection
	 */
	public CandidateSelection getCandidateSelection() {
		return this.candidateSelection;
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

	/**
	 * Sets the value of comparison to the given value.
	 * 
	 * @param comparison
	 *        the comparison to set
	 * @return this
	 */
	public ImplType withComparison(CandidateComparison comparison) {
		this.setComparison(comparison);
		return this.self();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere
	 * .sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		final CandidateComparison comparison = this.getComparison().copy();

		@SuppressWarnings({ "unchecked", "rawtypes" })
		List<Operator<?>> inputs = (List) module.getInputs();
		if (comparison.requiresEnumeration())
			inputs = comparison.addEnumeration(inputs);
		// duplicate input
		if (comparison.isInnerSource())
			inputs.add(inputs.get(0));
		comparison.setup();
		module.embed(this.getImplementation(inputs, this.candidateSelection, comparison, context));
	}

	protected abstract Operator<?> getImplementation(List<Operator<?>> inputs, CandidateSelection selection,
			CandidateComparison comparison, EvaluationContext context);
}
