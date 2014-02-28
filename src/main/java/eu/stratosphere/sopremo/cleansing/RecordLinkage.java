/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.sopremo.cleansing.duplicatedection.*;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.SelectionHint;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
@InputCardinality(2)
@OutputCardinality(1)
public class RecordLinkage extends CompositeOperator<RecordLinkage> {

	private CandidateSelection candidateSelection = new CandidateSelection();

	private CandidateComparison comparison = new CandidateComparison().withInnerSource(false);

	private DuplicateDetectionImplementation implementation;

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

	@Property
	@Name(preposition = "where")
	public void setComparisonExpression(BooleanExpression expression) {
		this.comparison.setDuplicateExpression(expression);
	}

	@Property
	@Name(preposition = "sort on")
	public void setSortingKeyExpression(EvaluationExpression expression) {
		this.candidateSelection.parse(expression, 1);
		this.candidateSelection.setSelectionHint(SelectionHint.SORT);
	}

	@Property
	@Name(preposition = "partition on")
	public void setPartitionKeyExpression(EvaluationExpression expression) {
		this.candidateSelection.parse(expression, 1);
		this.candidateSelection.setSelectionHint(SelectionHint.BLOCK);
	}
	
	@Property
	@Name(preposition = "on")
	public void setKeyExpression(EvaluationExpression expression) {
		this.candidateSelection.parse(expression, 1);
		this.candidateSelection.setSelectionHint(null);
	}

	@Property
	@Name(preposition = "with")
	public void setImplementation(DuplicateDetectionImplementation implementation) {
		this.implementation = implementation;
	}
	
	/**
	 * Returns the implementation.
	 * 
	 * @return the implementation
	 */
	public DuplicateDetectionImplementation getImplementation() {
		return this.implementation;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module) {
		final CompositeDuplicateDetectionAlgorithm<?> algorithm;
		if (this.implementation != null)
			algorithm = ReflectUtil.newInstance(this.implementation.getType());
		else
			algorithm = DuplicateDetectionFactory.getInstance().getMatchingAlgorithm(this.candidateSelection, 2);
		algorithm.setInputs(module.getInputs());
		algorithm.setCandidateSelection(this.candidateSelection);
		algorithm.setComparison(this.comparison);
		module.embed(algorithm);
	}

}
