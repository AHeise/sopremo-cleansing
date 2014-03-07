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
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.base.GlobalEnumeration;
import eu.stratosphere.sopremo.expressions.*;
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

	private PairFilter pairFilter = new PairFilter();

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
	 * Returns the pairFilter.
	 * 
	 * @return the pairFilter
	 */
	public PairFilter getPairFilter() {
		return this.pairFilter;
	}

	/**
	 * Sets the pairFilter to the specified value.
	 * 
	 * @param pairFilter
	 *        the pairFilter to set
	 */
	public void setPairFilter(PairFilter pairFilter) {
		if (pairFilter == null)
			throw new NullPointerException("pairFilter must not be null");

		this.pairFilter = pairFilter;
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
	public void addImplementation(SopremoModule module) {
		final CandidateComparison comparison = this.getComparison().copy();

		@SuppressWarnings({ "unchecked", "rawtypes" })
		List<Operator<?>> inputs = (List) module.getInputs();
		if (requiresEnumeration() && inputs.size() == 1 && !this.pairFilter.isConfigured())
			inputs = addEnumeration(inputs, comparison);
		module.embed(this.getImplementation(inputs, this.candidateSelection, this.pairFilter, comparison));
	}

	private List<Operator<?>> addEnumeration(List<Operator<?>> inputs, CandidateComparison comparison) {
		List<Operator<?>> outputs = new ArrayList<Operator<?>>();
		List<ObjectAccess> idPaths = new ArrayList<ObjectAccess>();
		for (int index = 0; index < inputs.size(); index++) {
			final GlobalEnumeration globalEnumeration = new GlobalEnumeration().
				withInputs(inputs.get(index));
			outputs.add(globalEnumeration);

			final ObjectAccess idAccess = globalEnumeration.getIdAccess();
			if (index == 0)
				this.pairFilter.setLeftIdProjection(idAccess);
			else
				this.pairFilter.setRightIdProjection(idAccess);
			idPaths.add(idAccess);
		}
		if (inputs.size() == 1)
			this.pairFilter.setRightIdProjection(this.pairFilter.getLeftIdProjection());

		final ChainedSegmentExpression idRemoval = new ChainedSegmentExpression();
		for (int index = 0; index < 2; index++)
			idRemoval.addExpression(new SetValueExpression(
				ExpressionUtil.makePath(new InputSelection(index),
					idPaths.get(inputs.size() == 1 ? 0 : index).clone()),
				ConstantExpression.MISSING));
		idRemoval.addExpression(comparison.getResultProjection());
		comparison.setResultProjection(idRemoval);

		return outputs;
	}

	protected BooleanExpression getCondition() {
		if (this.getNumInputs() == 1)
			return new AndExpression(this.pairFilter, this.comparison.asCondition());
		return this.comparison.asCondition();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.Operator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		appendable.append("[");
		this.comparison.appendAsString(appendable);
		appendable.append(", ");
		this.candidateSelection.appendAsString(appendable);
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.candidateSelection.hashCode();
		result = prime * result + this.comparison.hashCode();
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
		CompositeDuplicateDetectionAlgorithm<?> other = (CompositeDuplicateDetectionAlgorithm<?>) obj;
		return this.candidateSelection.equals(other.candidateSelection) && this.comparison.equals(other.comparison);
	}

	protected boolean requiresEnumeration() {
		return false;
	}

	protected abstract Operator<?> getImplementation(List<Operator<?>> inputs, CandidateSelection selection,
			PairFilter pairFilter, CandidateComparison comparison);
}
