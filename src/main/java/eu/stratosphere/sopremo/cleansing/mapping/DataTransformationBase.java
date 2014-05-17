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
package eu.stratosphere.sopremo.cleansing.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.stratosphere.sopremo.cleansing.DataTransformation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * @author arvid
 */
@InputCardinality(min = 1)
@OutputCardinality(min = 1)
public abstract class DataTransformationBase<T extends CompositeOperator<T>> extends CompositeOperator<T> {

	protected Set<ObjectCreation.SymbolicAssignment> sourceFKs = new HashSet<ObjectCreation.SymbolicAssignment>();

	protected Set<ObjectCreation.SymbolicAssignment> targetFKs = new HashSet<ObjectCreation.SymbolicAssignment>();

	protected List<PathSegmentExpression> sourceKeys = new ArrayList<PathSegmentExpression>();

	protected List<PathSegmentExpression> targetKeys = new ArrayList<PathSegmentExpression>();

	protected Set<ObjectCreation.SymbolicAssignment> sourceToValueCorrespondences =
		new HashSet<ObjectCreation.SymbolicAssignment>();

	protected List<EvaluationExpression> sourceSchema = new ArrayList<EvaluationExpression>();

	protected List<EvaluationExpression> targetSchema = new ArrayList<EvaluationExpression>();

	/**
	 * Initializes DataTransformationBase.
	 */
	public DataTransformationBase() {
		super();
	}

	/**
	 * Initializes DataTransformationBase.
	 * 
	 * @param numberOfInputs
	 * @param numberOfOutputs
	 */
	public DataTransformationBase(int numberOfInputs, int numberOfOutputs) {
		super(numberOfInputs, numberOfOutputs);
	}

	/**
	 * Initializes DataTransformationBase.
	 * 
	 * @param minInputs
	 * @param maxInputs
	 * @param minOutputs
	 * @param maxOutputs
	 */
	public DataTransformationBase(int minInputs, int maxInputs, int minOutputs, int maxOutputs) {
		super(minInputs, maxInputs, minOutputs, maxOutputs);
	}

	/**
	 * Returns the sourceFKs.
	 * 
	 * @return the sourceFKs
	 */
	public Set<ObjectCreation.SymbolicAssignment> getSourceFKs() {
		return this.sourceFKs;
	}

	/**
	 * Sets the sourceFKs to the specified value.
	 * 
	 * @param sourceFKs
	 *        the sourceFKs to set
	 */
	public void setSourceFKs(Set<ObjectCreation.SymbolicAssignment> sourceFKs) {
		if (sourceFKs == null)
			throw new NullPointerException("sourceFKs must not be null");

		this.sourceFKs = sourceFKs;
	}

	/**
	 * Returns the targetFKs.
	 * 
	 * @return the targetFKs
	 */
	public Set<ObjectCreation.SymbolicAssignment> getTargetFKs() {
		return this.targetFKs;
	}

	/**
	 * Sets the targetFKs to the specified value.
	 * 
	 * @param targetFKs
	 *        the targetFKs to set
	 */
	public void setTargetFKs(Set<ObjectCreation.SymbolicAssignment> targetFKs) {
		if (targetFKs == null)
			throw new NullPointerException("targetFKs must not be null");

		this.targetFKs = targetFKs;
	}

	/**
	 * Returns the sourceToValueCorrespondences.
	 * 
	 * @return the sourceToValueCorrespondences
	 */
	public Set<ObjectCreation.SymbolicAssignment> getSourceToValueCorrespondences() {
		return this.sourceToValueCorrespondences;
	}

	/**
	 * Sets the sourceToValueCorrespondences to the specified value.
	 * 
	 * @param sourceToValueCorrespondences
	 *        the sourceToValueCorrespondences to set
	 */
	public void setSourceToValueCorrespondences(Set<ObjectCreation.SymbolicAssignment> sourceToValueCorrespondences) {
		if (sourceToValueCorrespondences == null)
			throw new NullPointerException("sourceToValueCorrespondences must not be null");

		this.sourceToValueCorrespondences = sourceToValueCorrespondences;
	}

	/**
	 * Returns the sourceSchema.
	 * 
	 * @return the sourceSchema
	 */
	public List<EvaluationExpression> getSourceSchema() {
		return this.sourceSchema;
	}

	/**
	 * Sets the sourceSchema to the specified value.
	 * 
	 * @param sourceSchema
	 *        the sourceSchema to set
	 */
	@SuppressWarnings("unchecked")
	public void setSourceSchema(List<? extends EvaluationExpression> sourceSchema) {
		if (sourceSchema == null)
			throw new NullPointerException("sourceSchema must not be null");

		this.sourceSchema = (List<EvaluationExpression>) sourceSchema;
	}

	/**
	 * Returns the targetSchema.
	 * 
	 * @return the targetSchema
	 */
	public List<EvaluationExpression> getTargetSchema() {
		return this.targetSchema;
	}

	/**
	 * Sets the targetSchema to the specified value.
	 * 
	 * @param targetSchema
	 *        the targetSchema to set
	 */
	@SuppressWarnings("unchecked")
	public void setTargetSchema(List<? extends EvaluationExpression> targetSchema) {
		if (targetSchema == null)
			throw new NullPointerException("targetSchema must not be null");

		this.targetSchema = (List<EvaluationExpression>) targetSchema;
	}

	/**
	 * Sets the sourcePKs to the specified value.
	 * 
	 * @param sourcePKs
	 *        the sourcePKs to set
	 */
	public void setSourceKeys(List<PathSegmentExpression> sourcePKs) {
		if (sourcePKs == null)
			throw new NullPointerException("sourcePKs must not be null");

		this.sourceKeys = sourcePKs;
	}

	/**
	 * Sets the targetPKs to the specified value.
	 * 
	 * @param targetPKs
	 *        the targetPKs to set
	 */
	public void setTargetKeys(List<PathSegmentExpression> targetPKs) {
		if (targetPKs == null)
			throw new NullPointerException("targetPKs must not be null");

		this.targetKeys = targetPKs;
	}

	/**
	 * Returns the targetPKs.
	 * 
	 * @return the targetPKs
	 */
	public List<PathSegmentExpression> getTargetKeys() {
		return this.targetKeys;
	}

	/**
	 * Returns the sourcePKs.
	 * 
	 * @return the sourcePKs
	 */
	public List<PathSegmentExpression> getSourceKeys() {
		return this.sourceKeys;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.sourceKeys.hashCode();
		result = prime * result + this.targetKeys.hashCode();
		result = prime * result + this.sourceFKs.hashCode();
		result = prime * result + this.targetFKs.hashCode();
		result = prime * result + this.sourceToValueCorrespondences.hashCode();
		result = prime * result + this.sourceSchema.hashCode();
		result = prime * result + this.targetSchema.hashCode();
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.Operator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		SopremoUtil.append(appendable, getClass().getSimpleName(), " [sourcePKs=", this.sourceKeys, ", targetPKs=",
			this.targetKeys, ", sourceFKs=", this.sourceFKs, ", targetFKs=", this.targetFKs,
			", sourceToValueCorrespondences=", this.sourceToValueCorrespondences, ", sourceSchema=", this.sourceSchema,
			", targetSchema=", this.targetSchema, "]");
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DataTransformationBase<?> other = (DataTransformationBase<?>) obj;
		return this.sourceKeys.equals(other.sourceKeys) &&
			this.targetKeys.equals(other.targetKeys) &&
			this.sourceFKs.equals(other.sourceFKs) &&
			this.targetFKs.equals(other.targetFKs) &&
			this.sourceToValueCorrespondences.equals(other.sourceToValueCorrespondences) &&
			this.sourceSchema.equals(other.sourceSchema) &&
			this.targetSchema.equals(other.targetSchema);
	}
}