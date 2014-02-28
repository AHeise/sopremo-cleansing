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
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class CandidateComparison extends AbstractSopremoType implements Cloneable {

	/**
	 * @author Arvid Heise
	 */
	final static class OrderedPairsFilter extends BooleanExpression {

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

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.expressions.BooleanExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		public BooleanNode evaluate(IJsonNode pair) {
			@SuppressWarnings("unchecked")
			final IArrayNode<IJsonNode> array = (IArrayNode<IJsonNode>) pair;
			return BooleanNode.valueOf(this.leftExp.evaluate(array.get(0)).compareTo(
				this.rightExp.evaluate(array.get(1))) < 0);
		}

	}

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	private BooleanExpression preselect;

	private EvaluationExpression leftIdProjection, rightIdProjection;

	private boolean innerSource = false;

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

	private transient IArrayNode<IJsonNode> pair = new ArrayNode<IJsonNode>();

	private BooleanExpression duplicateExpression = BooleanExpression.TRUE;

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
	 */
	public CandidateComparison withResultProjection(EvaluationExpression resultProjection) {
		this.setResultProjection(resultProjection);
		return this;
	}

	/**
	 * Sets the duplicateExpression to the specified value.
	 * 
	 * @param duplicateExpression
	 *        the duplicateExpression to set
	 */
	public void setDuplicateExpression(BooleanExpression duplicateExpression) {
		if (duplicateExpression == null)
			throw new NullPointerException("duplicateExpression must not be null");

		this.duplicateExpression = duplicateExpression;
	}

	/**
	 * Returns the duplicateExpression.
	 * 
	 * @return the duplicateExpression
	 */
	public BooleanExpression getDuplicateExpression() {
		return this.duplicateExpression;
	}

	public CandidateComparison withDuplicateExpression(BooleanExpression duplicateExpression) {
		setDuplicateExpression(duplicateExpression);
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
	public void setPreselect(BooleanExpression preselect) {
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

	public BooleanExpression asCondition(boolean omitSmallerPairs) {
		return new AndExpression(createSmallerPairFilter(omitSmallerPairs), this.duplicateExpression);
	}

	public void setOmitSmallerPairs(boolean omitSmallerPairs) {
		this.preselect = createSmallerPairFilter(omitSmallerPairs);
	}

	public boolean isOmitSmallerPairs() {
		return this.preselect instanceof OrderedPairsFilter;
	}

	public CandidateComparison withOmitSmallerPairs(boolean omitSmallerPairs) {
		setOmitSmallerPairs(omitSmallerPairs);
		return this;
	}

	/**
	 * @param omitSmallerPairs
	 * @return
	 */
	private BooleanExpression createSmallerPairFilter(boolean omitSmallerPairs) {
		BooleanExpression preselect = this.preselect;
		if (omitSmallerPairs && preselect == null) {
			if (this.innerSource) {
				if (this.getLeftIdProjection() == null || this.getRightIdProjection() == null)
					throw new IllegalStateException("Requires id projection or custom preselection");

				preselect = new OrderedPairsFilter(this.leftIdProjection, this.rightIdProjection);
			} else
				preselect = BooleanExpression.ensureBooleanExpression(new ConstantExpression(BooleanNode.TRUE));
		}
		return preselect;
	}

	public void performComparison(IJsonNode left, IJsonNode right, JsonCollector<IJsonNode> collector) {
		this.pair.set(0, left);
		this.pair.set(1, right);
		if (this.preselect.evaluate(this.pair) == BooleanNode.TRUE)
			collector.collect(this.pair);
	}

	public EvaluationExpression getResultProjectionWithSimilarity() {
		return this.resultProjection;
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
			"CandidateComparison [duplicateExpression=", this.duplicateExpression,
			", resultProjection=", this.resultProjection,
			", idProjection=", this.leftIdProjection, "|", this.rightIdProjection, "]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.duplicateExpression.hashCode();
		result = prime * result + (this.innerSource ? 1231 : 1237);
		result = prime * result + ((this.leftIdProjection == null) ? 0 : this.leftIdProjection.hashCode());
		result = prime * result + ((this.preselect == null) ? 0 : this.preselect.hashCode());
		result = prime * result + this.resultProjection.hashCode();
		result = prime * result + ((this.rightIdProjection == null) ? 0 : this.rightIdProjection.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CandidateComparison other = (CandidateComparison) obj;
		if (this.leftIdProjection == null) {
			if (other.leftIdProjection != null)
				return false;
		} else if (!this.leftIdProjection.equals(other.leftIdProjection))
			return false;
		if (this.preselect == null) {
			if (other.preselect != null)
				return false;
		} else if (!this.preselect.equals(other.preselect))
			return false;
		if (this.rightIdProjection == null) {
			if (other.rightIdProjection != null)
				return false;
		} else if (!this.rightIdProjection.equals(other.rightIdProjection))
			return false;
		return this.resultProjection.equals(other.resultProjection) && this.innerSource == other.innerSource &&
			this.duplicateExpression.equals(other.duplicateExpression);
	}

	public List<Operator<?>> addEnumeration(List<? extends Operator<?>> inputs) {
		List<Operator<?>> outputs = new ArrayList<Operator<?>>();
		List<ObjectAccess> idPaths = new ArrayList<ObjectAccess>();
		for (int index = 0; index < inputs.size(); index++) {
			final GlobalEnumeration globalEnumeration = new GlobalEnumeration().
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
}
