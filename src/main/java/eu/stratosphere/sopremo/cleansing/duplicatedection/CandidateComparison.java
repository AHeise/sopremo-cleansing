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

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.base.GlobalEnumeration;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ChainedSegmentExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ExpressionUtil;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.SetValueExpression;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;

/**
 * @author Arvid Heise
 */
public class CandidateComparison extends AbstractSopremoType implements Setupable, Cloneable {
	/**
	 * 
	 */
	static final class ComparisonCondition extends BooleanExpression {
		private final Preselection preselect;

		private final List<DuplicateRule> duplicateRules;

		/**
		 * Initializes CandidateComparison.ComparisonCondition.
		 */
		ComparisonCondition() {
			this.preselect = null;
			this.duplicateRules = null;
		}

		ComparisonCondition(Preselection preselect, List<DuplicateRule> duplicateRules) {
			this.preselect = preselect;
			this.duplicateRules = duplicateRules;
		}

		@Override
		public BooleanNode evaluate(IJsonNode node) {
			@SuppressWarnings("unchecked")
			final IArrayNode<IJsonNode> inputPair = (IArrayNode<IJsonNode>) node;
			final IJsonNode left = inputPair.get(0), right = inputPair.get(1);
			if (!this.preselect.shouldProcess(left, right))
				return BooleanNode.FALSE;
			boolean satisfiesAll = true;
			for (int index = 0, size = this.duplicateRules.size(); satisfiesAll && index < size; index++)
				satisfiesAll = this.duplicateRules.get(index).apply(left, right);
			return BooleanNode.valueOf(satisfiesAll);
		}
	}

	/**
	 * 
	 */
	static class SimiliartiesAssembler extends EvaluationExpression {
		private final List<DuplicateRule> duplicateRules;

		SimiliartiesAssembler(List<DuplicateRule> duplicateRules) {
			this.duplicateRules = duplicateRules;
		}

		/**
		 * Initializes CandidateComparison.SimiliartiesAssembler.
		 */
		SimiliartiesAssembler() {
			this.duplicateRules = null;
		}

		private transient CachingArrayNode<DoubleNode> similarities = new CachingArrayNode<DoubleNode>();

		private transient DoubleNode similarity = new DoubleNode();

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		public IJsonNode evaluate(IJsonNode node) {
			for (int index = 0, size = this.duplicateRules.size(); index < size; index++) {
				this.similarity.setValue(this.duplicateRules.get(index).getLastSim());
				this.similarities.addClone(this.similarity);
			}
			return this.similarities;
		}

	}

	/**
	 * 
	 */
	private static final class NoPreselection extends AbstractPreselection {
		@Override
		public boolean shouldProcess(IJsonNode left, IJsonNode right) {
			return true;
		}
	}

	/**
	 * @author Arvid Heise
	 */
	final static class OrderedPairsFilter extends AbstractPreselection {

		private final EvaluationExpression leftExp, rightExp;

		OrderedPairsFilter(EvaluationExpression leftExp, EvaluationExpression rightExp) {
			super();
			this.leftExp = leftExp;
			this.rightExp = rightExp;
		}

		/**
		 * Initializes CandidateComparison.OrderedPairsFilter.
		 */
		OrderedPairsFilter() {
			this.leftExp = null;
			this.rightExp = null;
		}

		@Override
		public boolean shouldProcess(IJsonNode left, IJsonNode right) {
			return this.leftExp.evaluate(left).compareTo(this.rightExp.evaluate(right)) < 0;
		}
	}

	public static class DuplicateRule extends AbstractSopremoType {
		private Similarity<IJsonNode> similarityMeasure;

		private double threshold;

		private transient double lastSim;

		public boolean apply(IJsonNode left, IJsonNode right) {
			return (this.lastSim = this.similarityMeasure.getSimilarity(left, right)) >= this.threshold;
		}

		/**
		 * Returns the lastSim.
		 * 
		 * @return the lastSim
		 */
		public double getLastSim() {
			return this.lastSim;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
		 */
		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			SopremoUtil.append(appendable, "Sim: ", this.similarityMeasure, "; threshold: ", this.threshold);
		}
	}

	private List<DuplicateRule> duplicateRules = new ArrayList<DuplicateRule>();

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	private Preselection preselect;

	private EvaluationExpression leftIdProjection, rightIdProjection;

	private boolean outputSimilarity = false, innerSource = false;

	/**
	 * Returns the value of innerSource.
	 * 
	 * @return the innerSource
	 */
	public boolean isInnerSource() {
		return this.innerSource;
	}

	/**
	 * Sets the value of innerSource to the given value.
	 * 
	 * @param innerSource
	 *        the innerSource to set
	 */
	public void setInnerSource(boolean innerSource) {
		this.innerSource = innerSource;
	}

	/**
	 * Returns the value of outputSimilarity.
	 * 
	 * @return the outputSimilarity
	 */
	public boolean isOutputSimilarity() {
		return this.outputSimilarity;
	}

	/**
	 * Sets the value of outputSimilarity to the given value.
	 * 
	 * @param outputSimilarity
	 *        the outputSimilarity to set
	 */
	public void setOutputSimilarity(boolean outputSimilarity) {
		if (this.outputSimilarity != outputSimilarity)
			this.outputSimilarity = outputSimilarity;
	}

	/**
	 * Sets the value of outputSimilarity to the given value.
	 * 
	 * @param outputSimilarity
	 *        the outputSimilarity to set
	 * @return this
	 */
	public CandidateComparison withOutputSimilarity(boolean outputSimilarity) {
		this.setOutputSimilarity(outputSimilarity);
		return this;
	}

	/**
	 * Returns the value of resultProjection.
	 * 
	 * @return the resultProjection
	 */
	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	/**
	 * Sets the value of resultProjection to the given value.
	 * 
	 * @param resultProjection
	 *        the resultProjection to set
	 */
	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	/**
	 * Sets the value of resultProjection to the given value.
	 * 
	 * @param resultProjection
	 *        the resultProjection to set
	 * @return
	 */
	public CandidateComparison withResultProjection(EvaluationExpression resultProjection) {
		this.setResultProjection(resultProjection);
		return this;
	}

	/**
	 * Returns the value of idProjection.
	 * 
	 * @return the idProjection
	 */
	public EvaluationExpression getIdProjection() {
		if (this.leftIdProjection == this.rightIdProjection)
			return this.leftIdProjection;
		throw new IllegalStateException("May only return id projection if the same for both sources");
	}

	/**
	 * Sets the preselect to the specified value.
	 * 
	 * @param preselect
	 *        the preselect to set
	 */
	public void setPreselect(Preselection preselect) {
		if (preselect == null)
			throw new NullPointerException("preselect must not be null");

		this.preselect = preselect;
	}

	/**
	 * Sets the value of idProjection to the given value.
	 * 
	 * @param idProjection
	 *        the idProjection to set
	 */
	public void setIdProjection(EvaluationExpression idProjection) {
		if (idProjection == null)
			throw new NullPointerException("idProjection must not be null");

		this.leftIdProjection = this.rightIdProjection = idProjection;
	}

	/**
	 * Sets the value of idProjection to the given value.
	 * 
	 * @param idProjection
	 *        the idProjection to set
	 * @return this
	 */
	public CandidateComparison withIdProjection(EvaluationExpression idProjection) {
		this.setIdProjection(idProjection);
		return this;
	}

	/**
	 * Returns the value of leftIdProjection.
	 * 
	 * @return the leftIdProjection
	 */
	public EvaluationExpression getLeftIdProjection() {
		return this.leftIdProjection;
	}

	/**
	 * Sets the value of leftIdProjection to the given value.
	 * 
	 * @param leftIdProjection
	 *        the leftIdProjection to set
	 */
	public void setLeftIdProjection(EvaluationExpression leftIdProjection) {
		if (leftIdProjection == null)
			throw new NullPointerException("leftIdProjection must not be null");

		this.leftIdProjection = leftIdProjection;
	}

	/**
	 * Returns the value of rightIdProjection.
	 * 
	 * @return the rightIdProjection
	 */
	public EvaluationExpression getRightIdProjection() {
		return this.rightIdProjection;
	}

	/**
	 * Sets the value of rightIdProjection to the given value.
	 * 
	 * @param rightIdProjection
	 *        the rightIdProjection to set
	 */
	public void setRightIdProjection(EvaluationExpression rightIdProjection) {
		if (rightIdProjection == null)
			throw new NullPointerException("rightIdProjection must not be null");

		this.rightIdProjection = rightIdProjection;
	}

	public boolean requiresEnumeration() {
		return this.innerSource && (this.leftIdProjection == null || this.rightIdProjection == null);
	}

	@Override
	public void setup() {
		if (this.innerSource) {
			if (this.getLeftIdProjection() == null || this.getRightIdProjection() == null)
				throw new IllegalStateException("Requires id projection");

			this.preselect = new OrderedPairsFilter(this.leftIdProjection, this.rightIdProjection);
		} else
			this.preselect = new NoPreselection();
	}

	// public void process(IJsonNode left, IJsonNode right, JsonCollector collector) {
	// if (!this.preselect.shouldProcess(left, right))
	// return;
	//
	// boolean satisfiesAll = true;
	// for (int index = 0, size = this.duplicateRules.size(); satisfiesAll && index < size; index++)
	// satisfiesAll = this.duplicateRules.get(index).apply(left, right);
	//
	// if (satisfiesAll) {
	// this.fillResult(this.result, left, right);
	// collector.collect(this.resultProjection.evaluate(this.result));
	// }
	// }

	public BooleanExpression asCondition() {
		Preselection preselect = this.preselect;
		if (preselect == null) {
			if (this.innerSource) {
				if (this.getLeftIdProjection() == null || this.getRightIdProjection() == null)
					throw new IllegalStateException("Requires id projection or custom preselection");

				preselect = new OrderedPairsFilter(this.leftIdProjection, this.rightIdProjection);
			} else
				preselect = new NoPreselection();
		}

		return new ComparisonCondition(preselect, this.duplicateRules);
	}

	public EvaluationExpression getResultProjectionWithSimilarity() {
		if (!this.outputSimilarity)
			return this.resultProjection;

		return new ChainedSegmentExpression(this.resultProjection).withInputExpression(new ArrayCreation(
			new ArrayAccess(0),
			new ArrayAccess(1), new SimiliartiesAssembler(this.duplicateRules)));
	}

	public CandidateComparison withInnerSource(boolean innerSource) {
		this.setInnerSource(innerSource);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		SopremoUtil.append(appendable,
			"CandidateComparison [duplicateRules=", this.duplicateRules,
			", resultProjection=", this.resultProjection,
			", idProjection=", this.leftIdProjection, "|", this.rightIdProjection,
			", outputSimilarity=", this.outputSimilarity, "]");
	}

	public List<Operator<?>> addEnumeration(List<? extends Operator<?>> inputs) {
		List<Operator<?>> outputs = new ArrayList<Operator<?>>();
		List<ObjectAccess> idPaths = new ArrayList<ObjectAccess>();
		for (int index = 0; index < inputs.size(); index++) {
			final GlobalEnumeration globalEnumeration = new GlobalEnumeration().
				withIdGeneration(GlobalEnumeration.LONG_COMBINATION).
				withInputs(inputs.get(index));
			outputs.add(globalEnumeration);

			final ObjectAccess idAccess = globalEnumeration.getIdAccess();
			if (index == 0)
				this.leftIdProjection = idAccess;
			else
				this.rightIdProjection = idAccess;
			idPaths.add(idAccess);
		}
		if (inputs.size() == 1)
			this.rightIdProjection = this.leftIdProjection;

		final ChainedSegmentExpression idRemoval = new ChainedSegmentExpression();
		for (int index = 0; index < 2; index++)
			idRemoval.addExpression(new SetValueExpression(
				ExpressionUtil.makePath(new InputSelection(index),
					idPaths.get(inputs.size() == 1 ? 0 : index).clone()),
				ConstantExpression.MISSING));
		idRemoval.addExpression(this.resultProjection);
		this.resultProjection = idRemoval;

		return outputs;
	}

	public DuplicateRule parseRule(EvaluationExpression expression) {
		if (expression instanceof ComparativeExpression) {
			DuplicateRule duplicateRule = new DuplicateRule();
			final ComparativeExpression ce = (ComparativeExpression) expression;
			boolean includeEqual = true;
			switch (ce.getBinaryOperator()) {
			case GREATER:
				includeEqual = false;
			case GREATER_EQUAL:
				duplicateRule.threshold = ExpressionUtil.getConstant(ce.getExpr2(), INumericNode.class)
					.getDoubleValue();
				duplicateRule.similarityMeasure = parseSimilarity(ce.getExpr1());
				break;
			case LESS:
				includeEqual = false;
			case LESS_EQUAL:
				duplicateRule.threshold = ExpressionUtil.getConstant(ce.getExpr1(), INumericNode.class)
					.getDoubleValue();
				duplicateRule.similarityMeasure = parseSimilarity(ce.getExpr2());
				break;
			default:
				throw new IllegalArgumentException(
					"Unsupported similarity expression; must be of the form: sim > threshold");
			}

			// small hack to exclude the same value;
			// very unlikely to have results in the range [threshold-2e-126;threshold]
			if (!includeEqual)
				duplicateRule.threshold -= Float.MIN_VALUE;
			return duplicateRule;
		}
		throw new IllegalArgumentException(
			"Unsupported similarity expression; must be of the form: sim > threshold");
	}

	/**
	 * @param expr2
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Similarity<IJsonNode> parseSimilarity(EvaluationExpression similarityExpression) {
		return (Similarity<IJsonNode>) similarityExpression;
	}

	/**
	 * @param expression
	 */
	public void parseRules(EvaluationExpression expression) {
		this.duplicateRules.clear();
		if (expression instanceof AndExpression)
			for (EvaluationExpression subExpression : expression)
				this.duplicateRules.add(parseRule(subExpression));
		else
			this.duplicateRules.add(parseRule(expression));
	}

	public void addRule(DuplicateRule rule) {
		this.duplicateRules.add(rule);
	}

	public void removeRule(DuplicateRule rule) {
		this.duplicateRules.remove(rule);
	}
}
