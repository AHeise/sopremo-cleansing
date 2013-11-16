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
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.SelectionHint;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionFactory;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.cleansing.duplicatedection.NaiveDuplicateDetection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
@InputCardinality(1)
@OutputCardinality(1)
@Name(verb = "detect duplicates")
public class DuplicateDetection extends CompositeOperator<DuplicateDetection> {

	private CandidateSelection candidateSelection = new CandidateSelection();

	private CandidateComparison comparison = new CandidateComparison().withInnerSource(true);

	private CompositeDuplicateDetectionAlgorithm<?> algorithm = new NaiveDuplicateDetection();

	/**
	 * Initializes DuplicateDetection.
	 */
	public DuplicateDetection() {
		addPropertiesFrom(this.algorithm);
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
	 * Sets the candidateSelection to the specified value.
	 * 
	 * @param candidateSelection
	 *        the candidateSelection to set
	 */
	public void setCandidateSelection(CandidateSelection candidateSelection) {
		if (candidateSelection == null)
			throw new NullPointerException("candidateSelection must not be null");

		this.candidateSelection = candidateSelection;
		changeAlgorithm(DuplicateDetectionFactory.getInstance().getMatchingAlgorithm(this.candidateSelection, 1));
	}

	public DuplicateDetection withCandidateSelection(CandidateSelection candidateSelection) {
		setCandidateSelection(candidateSelection);
		return this;
	}

	@Property
	@Name(preposition = "where")
	public void setComparisonExpression(EvaluationExpression expression) {
		this.comparison.parseRules(expression);
	}

	@Property
	@Name(preposition = "sort on")
	public void setSortingKeyExpression(EvaluationExpression expression) {
		this.candidateSelection.parse(expression, 1);
		this.candidateSelection.setSelectionHint(SelectionHint.SORT);
		changeAlgorithm(DuplicateDetectionFactory.getInstance().getMatchingAlgorithm(this.candidateSelection, 1));
	}

	/**
	 * @param matchingAlgorithm
	 */
	private void changeAlgorithm(CompositeDuplicateDetectionAlgorithm<?> matchingAlgorithm) {
		removePropertiesFrom(this.algorithm);
		this.algorithm = matchingAlgorithm;
		addPropertiesFrom(matchingAlgorithm);
	}

	@Property
	@Name(preposition = "partition on")
	public void setPartitionKeyExpression(EvaluationExpression expression) {
		this.candidateSelection.parse(expression, 1);
		this.candidateSelection.setSelectionHint(SelectionHint.BLOCK);
		changeAlgorithm(DuplicateDetectionFactory.getInstance().getMatchingAlgorithm(this.candidateSelection, 1));
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
		this.algorithm = ReflectUtil.newInstance(implementation.getType());
	}

	public DuplicateDetection withImplementation(DuplicateDetectionImplementation implementation) {
		setImplementation(implementation);
		return this;
	}

	/**
	 * Returns the implementation.
	 * 
	 * @return the implementation
	 */
	public DuplicateDetectionImplementation getImplementation() {
		for (DuplicateDetectionImplementation implementation : DuplicateDetectionImplementation.values())
			if (implementation.getType().isInstance(this.algorithm))
				return implementation;
		return null;
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
	 */
	public DuplicateDetection withComparison(CandidateComparison setComparison) {
		setComparison(setComparison);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		this.algorithm.setCandidateSelection(this.candidateSelection);
		this.algorithm.setComparison(this.comparison);
		module.embed(this.algorithm);
	}
}
