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
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import javolution.text.TypeFormat;
import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.ISerializableSopremoType;
import eu.stratosphere.sopremo.ISopremoType;
import eu.stratosphere.sopremo.base.GlobalEnumeration;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * @author Arvid Heise
 */
public class CandidateComparison extends AbstractSopremoType implements ISerializableSopremoType, Setupable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5051852872132228936L;

	private Similarity<IJsonNode> similarityMeasure;

	private double threshold;

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	private Preselection preselect;

	private EvaluationExpression leftIdProjection, rightIdProjection;

	private boolean outputSimilarity = false, innerSource = false;

	private transient IArrayNode result = new ArrayNode(NullNode.getInstance(), NullNode.getInstance());

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
		if (this.outputSimilarity != outputSimilarity) {
			this.outputSimilarity = outputSimilarity;

			if (outputSimilarity)
				this.result.add(new DoubleNode());
			else
				this.result.remove(2);
		}
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.result = new ArrayNode(NullNode.getInstance(), NullNode.getInstance());
		if (this.outputSimilarity)
			this.result.add(new DoubleNode());
	}

	/**
	 * Sets the value of outputSimilarity to the given value.
	 * 
	 * @param outputSimilarity
	 *        the outputSimilarity to set
	 * @return this
	 */
	public CandidateComparison withOutputSimilarity(boolean outputSimilarity) {
		setOutputSimilarity(outputSimilarity);
		return this;
	}

	/**
	 * Returns the value of similarityMeasure.
	 * 
	 * @return the similarityMeasure
	 */
	public Similarity<?> getSimilarityMeasure() {
		return this.similarityMeasure;
	}

	/**
	 * Sets the value of similarityMeasure to the given value.
	 * 
	 * @param similarityMeasure
	 *        the similarityMeasure to set
	 */
	@SuppressWarnings("unchecked")
	public void setSimilarityMeasure(Similarity<?> similarityMeasure) {
		if (similarityMeasure == null)
			throw new NullPointerException("similarityMeasure must not be null");

		this.similarityMeasure = (Similarity<IJsonNode>) similarityMeasure;
	}

	/**
	 * Sets the value of similarityMeasure to the given value.
	 * 
	 * @param similarityMeasure
	 *        the similarityMeasure to set
	 * @return this
	 */
	public CandidateComparison withSimilarityMeasure(Similarity<?> similarityMeasure) {
		setSimilarityMeasure(similarityMeasure);
		return this;
	}

	/**
	 * Returns the value of threshold.
	 * 
	 * @return the threshold
	 */
	public double getThreshold() {
		return this.threshold;
	}

	/**
	 * Sets the value of threshold to the given value.
	 * 
	 * @param threshold
	 *        the threshold to set
	 */
	public void setThreshold(double threshold) {
		if (threshold < 0 || threshold > 1)
			throw new IllegalArgumentException("threshold must be in [0; 1]");

		this.threshold = threshold;
	}

	/**
	 * Sets the value of threshold to the given value.
	 * 
	 * @param threshold
	 *        the threshold to set
	 * @return this
	 */
	public CandidateComparison withThreshold(double threshold) {
		setThreshold(threshold);
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
		setResultProjection(resultProjection);
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
		setIdProjection(idProjection);
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

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new CandidateComparison();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#copyPropertiesFrom(eu.stratosphere.sopremo.ISopremoType)
	 */
	@Override
	public void copyPropertiesFrom(ISopremoType original) {
		super.copyPropertiesFrom(original);
		CandidateComparison comparison = (CandidateComparison) original;
		this.innerSource = comparison.innerSource;
		this.leftIdProjection = comparison.leftIdProjection.clone();
		this.outputSimilarity = comparison.outputSimilarity;
		this.preselect = (Preselection) comparison.preselect.clone();
		this.resultProjection = comparison.resultProjection.clone();
		this.rightIdProjection = comparison.rightIdProjection.clone();
		this.similarityMeasure = comparison.similarityMeasure.clone();
		this.threshold = comparison.threshold;
	}

	@Override
	public void setup() {
		if (this.innerSource) {
			if (getLeftIdProjection() == null || getRightIdProjection() == null)
				throw new IllegalStateException("Requires id projection");

			this.preselect = new OrderedPairsFilter(this);
		} else
			this.preselect = new NoPreselection();
	}

	public void process(IJsonNode left, IJsonNode right, JsonCollector collector) {
		if (!this.preselect.shouldProcess(left, right))
			return;
		double similarity = this.similarityMeasure.getSimilarity(left, right);
		if (similarity >= this.threshold) {
			fillResult(this.result, left, right, similarity);
			collector.collect(this.result);
		}
	}

	protected void fillResult(IArrayNode result, IJsonNode left, IJsonNode right, double similarity) {
		result.set(0, this.resultProjection.evaluate(left));
		result.set(1, this.resultProjection.evaluate(right));
		if (this.outputSimilarity)
			((DoubleNode) result.get(2)).setValue(similarity);
	}

	public CandidateComparison withInnerSource(boolean innerSource) {
		setInnerSource(innerSource);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("CandidateComparison [similarityMeasure=");
		this.similarityMeasure.appendAsString(appendable);

		appendable.append(", threshold=");
		TypeFormat.format(this.threshold, appendable);

		appendable.append(", resultProjection=");
		this.resultProjection.appendAsString(appendable);
		appendable.append(", idProjection=");
		this.leftIdProjection.appendAsString(appendable);
		appendable.append("|");
		this.rightIdProjection.appendAsString(appendable);
		appendable.append(", outputSimilarity=");
		TypeFormat.format(this.outputSimilarity, appendable);
		appendable.append("]");
	}

	public List<Operator<?>> addEnumeration(List<? extends Operator<?>> inputs) {
		List<Operator<?>> outputs = new ArrayList<Operator<?>>();
		for (int index = 0; index < inputs.size(); index++) {
			final GlobalEnumeration globalEnumeration = new GlobalEnumeration().
				withIdGeneration(GlobalEnumeration.LONG_COMBINATION).
				withInputs(inputs.get(index));
			outputs.add(globalEnumeration);

			if (index == 0)
				this.leftIdProjection = globalEnumeration.getIdAccess();
			else
				this.rightIdProjection = globalEnumeration.getIdAccess();
		}
		if (inputs.size() == 1)
			this.rightIdProjection = this.leftIdProjection;
		return outputs;
	}

}
