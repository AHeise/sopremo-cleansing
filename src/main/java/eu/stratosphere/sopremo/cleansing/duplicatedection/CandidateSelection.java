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
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * @author Arvid Heise
 */
public class CandidateSelection extends AbstractSopremoType {
	private List<Pass> passes = new ArrayList<Pass>();

	private SelectionHint selectionHint;

	public static enum SelectionHint {
		BLOCK, SORT;
	}

	/**
	 * Sets the selectionHint to the specified value.
	 * 
	 * @param selectionHint
	 *        the selectionHint to set
	 */
	public void setSelectionHint(SelectionHint selectionHint) {
		this.selectionHint = selectionHint;
	}

	/**
	 * Returns the selectionHint.
	 * 
	 * @return the selectionHint
	 */
	public SelectionHint getSelectionHint() {
		return this.selectionHint;
	}

	/**
	 * Returns the passes.
	 * 
	 * @return the passes
	 */
	public List<Pass> getPasses() {
		return this.passes;
	}

	/**
	 * Sets the passes to the specified value.
	 * 
	 * @param passes
	 *        the passes to set
	 */
	public void setPasses(List<Pass> passes) {
		if (passes == null)
			throw new NullPointerException("passes must not be null");

		this.passes.clear();
		this.passes.addAll(passes);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.passes.hashCode();
		result = prime * result + ((this.selectionHint == null) ? 0 : this.selectionHint.hashCode());
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
		CandidateSelection other = (CandidateSelection) obj;
		return this.selectionHint == other.selectionHint && this.passes.equals(other.passes);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		SopremoUtil.append(appendable, "CandidateSelection [passes=", this.passes, "]");
	}

	public static class Pass extends AbstractSopremoType {

		private List<EvaluationExpression> blockingKeys = new ArrayList<EvaluationExpression>();

		/**
		 * Initializes Pass.
		 */
		public Pass() {
		}

		/**
		 * Returns the value of blockingKey.
		 * 
		 * @return the blockingKey
		 */
		public List<EvaluationExpression> getBlockingKeys() {
			return this.blockingKeys;
		}

		/**
		 * Sets the value of blockingKey to the given value.
		 * 
		 * @param blockingKeys
		 *        the blockingKey to set
		 */
		public void setBlockingKeys(List<EvaluationExpression> blockingKeys) {
			if (blockingKeys == null)
				throw new NullPointerException("blockingKey must not be null");

			this.blockingKeys.clear();
			this.blockingKeys.addAll(blockingKeys);
		}

		/**
		 * Sets the value of blockingKey to the given value.
		 * 
		 * @param blockingKeys
		 *        the blockingKey to set
		 */
		public void setBlockingKeys(EvaluationExpression... blockingKeys) {
			if (blockingKeys.length == 0)
				throw new IllegalArgumentException("blockingKey must not be empty");

			this.setBlockingKeys(Arrays.asList(blockingKeys));
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
		 */
		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			SopremoUtil.append(appendable, this.blockingKeys);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + this.blockingKeys.hashCode();
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
			Pass other = (Pass) obj;
			return this.blockingKeys.equals(other.blockingKeys);
		}

		public EvaluationExpression getBlockingKey(int index) {
			if (this.blockingKeys.size() == 1)
				return this.blockingKeys.get(0);
			return this.blockingKeys.get(index);
		}

	}

	public void parse(EvaluationExpression expression, int numSources) {
		this.passes.clear();

		if (expression instanceof ArrayCreation)
			for (EvaluationExpression passExpression : ((ArrayCreation) expression).getElements())
				this.passes.add(parsePassExpression(passExpression, numSources));
		else
			this.passes.add(parsePassExpression(expression, numSources));
	}

	private Pass parsePassExpression(EvaluationExpression expression, int numSources) {
		final Pass pass = new Pass();
		List<EvaluationExpression> expressions = new ArrayList<>();
		if (numSources == 1)
			expressions.add(expression);
		else {
			if (!(expression instanceof ObjectCreation))
				throw new IllegalArgumentException("Must use mappings for two sources");

			final ObjectCreation oc = (ObjectCreation) expression;
			if (oc.getMappings().size() != 1)
				throw new IllegalArgumentException("Mapping must contain one element");
			final Mapping<?> mapping = oc.getMapping(0);
			expressions.add(mapping.getTargetTagExpression());
			expressions.add(mapping.getExpression());
			ExpressionUtil.sortExpressionsForInputs(expressions);
		}

		ExpressionUtil.removeInputSelections(expressions);
		pass.setBlockingKeys(expressions);
		return pass;
	}

	/**
	 * @param blockingKey
	 */
	public CandidateSelection withPass(EvaluationExpression... blockingKey) {
		Pass pass = new Pass();
		pass.setBlockingKeys(blockingKey);
		this.passes.add(pass);
		return this;
	}

	public CandidateSelection withSelectionHint(SelectionHint hint) {
		setSelectionHint(hint);
		return this;
	}
}
