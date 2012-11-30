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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.Pass;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * @author Arvid Heise
 */
public abstract class CompositeDuplicateDetectionAlgorithm<ImplType extends CompositeDuplicateDetectionAlgorithm<ImplType>>
		extends CompositeOperator<ImplType> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8029175167683349177L;

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

	private CandidateSelection selection = new CandidateSelection();

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
		setComparison(comparison);
		return self();
	}

	/**
	 * Returns the selection.
	 * 
	 * @return the selection
	 */
	public CandidateSelection getSelection() {
		return this.selection;
	}

	/**
	 * Sets the selection to the specified value.
	 * 
	 * @param selection
	 *        the selection to set
	 */
	public ImplType withSelection(CandidateSelection selection) {
		setSelection(selection);
		return self();
	}

	/**
	 * Sets the selection to the specified value.
	 * 
	 * @param selection
	 *        the selection to set
	 */
	public void setSelection(CandidateSelection selection) {
		if (selection == null)
			throw new NullPointerException("selection must not be null");

		this.selection = selection;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere
	 * .sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		final CandidateComparison comparison = this.getComparison();

		@SuppressWarnings({ "unchecked", "rawtypes" })
		List<Operator<?>> inputs = (List) module.getInputs();
		if (comparison.requiresEnumeration())
			inputs = comparison.addEnumeration(inputs);
		// duplicate input
		if (comparison.isInnerSource())
			inputs.add(inputs.get(0));
		comparison.setup();
		module.embed(getMultipassImplementation(inputs, selection, comparison, context));
	}

	protected Operator<?> getMultipassImplementation(List<Operator<?>> inputs, CandidateSelection selection,
			CandidateComparison comparison, EvaluationContext context) {
		if (selection.getPasses().size() <= 0)
			return getImplementation(inputs, Collections.EMPTY_LIST, comparison, context);

		List<Operator<?>> passOutput = new ArrayList<Operator<?>>();
		for (Pass pass : selection.getPasses())
			passOutput.add(getImplementation(inputs, pass.getBlockingKey(), comparison, context));
		return new Union().withInputs(passOutput);
	}

	protected abstract Operator<?> getImplementation(List<Operator<?>> inputs, List<EvaluationExpression> blockingKeys,
			CandidateComparison comparison, EvaluationContext context);
}
