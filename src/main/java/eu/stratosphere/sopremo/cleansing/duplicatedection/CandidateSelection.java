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

import com.google.common.base.Predicates;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
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
		 * @param blockingKey
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
		 * @param blockingKey
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

	}

	public void parse(EvaluationExpression expression, int numSources) {
		this.passes.clear();

		if (expression instanceof ObjectCreation) {
			if (numSources != 2)
				throw new IllegalArgumentException("Can only use mappings for two sources");

			final ObjectCreation oc = (ObjectCreation) expression;
			for (int index = 0, size = oc.getMappingSize(); index < size; index++) {
				final Pass pass = new Pass();
				final Mapping<?> mapping = oc.getMapping(index);
				final EvaluationExpression key1 = mapping.getTargetTagExpression();
				final EvaluationExpression key2 = mapping.getExpression();
				if (key1.findFirst(InputSelection.class).getIndex() == key2.findFirst(InputSelection.class).getIndex())
					throw new IllegalArgumentException("The mapping " + mapping + " does not point to both sources");
				pass.setBlockingKeys(key1, key2);
				this.passes.add(pass);
			}
			return;
		}

		List<EvaluationExpression> passes = expression instanceof ArrayCreation ?
			((ArrayCreation) expression).getElements() : Arrays.asList(expression);
		for (EvaluationExpression passExpression : passes) {
			Pass pass = new Pass();
			if (numSources == 1)
				pass.setBlockingKeys(passExpression.remove(InputSelection.class));
			else
				pass.setBlockingKeys(
					passExpression.replace(Predicates.instanceOf(InputSelection.class), new InputSelection(0)),
					passExpression.replace(Predicates.instanceOf(InputSelection.class), new InputSelection(1)));
			this.passes.add(pass);
		}
	}

	/**
	 * @param blockingKey
	 */
	public void addPass(EvaluationExpression... blockingKey) {
		Pass pass = new Pass();
		pass.setBlockingKeys(blockingKey);
		this.passes.add(pass);
	}
}
