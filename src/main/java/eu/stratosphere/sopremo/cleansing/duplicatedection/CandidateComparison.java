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
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ExpressionUtil;
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


	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

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

		this.resultProjection = ExpressionUtil.replaceInputSelectionsWithArrayAccess(resultProjection);
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

	public BooleanExpression asCondition() {
		return this.duplicateExpression;
	}

	public void performComparison(IJsonNode left, IJsonNode right, JsonCollector<IJsonNode> collector) {
		this.pair.set(0, left);
		this.pair.set(1, right);
		performComparison(this.pair, collector);
	}

	public void performComparison(IArrayNode<IJsonNode> pair, JsonCollector<IJsonNode> collector) {
		if (this.duplicateExpression.evaluate(pair) == BooleanNode.TRUE)
			collector.collect(this.pair);
	}

	public EvaluationExpression getResultProjectionWithSimilarity() {
		return this.resultProjection;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		SopremoUtil.append(appendable,
			"CandidateComparison [duplicateExpression=", this.duplicateExpression,
			", resultProjection=", this.resultProjection, "]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.duplicateExpression.hashCode();
		result = prime * result + this.resultProjection.hashCode();
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
		return this.resultProjection.equals(other.resultProjection) &&
			this.duplicateExpression.equals(other.duplicateExpression);
	}
}
