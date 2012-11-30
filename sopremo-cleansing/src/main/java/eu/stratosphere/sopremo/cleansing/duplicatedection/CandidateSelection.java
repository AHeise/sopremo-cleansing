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
import eu.stratosphere.sopremo.ISopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * @author Arvid Heise
 */
public class CandidateSelection extends AbstractSopremoType {
	private List<Pass> passes = new ArrayList<Pass>();

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

		this.passes = passes;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
	 */
	@Override
	protected AbstractSopremoType createCopy() {
		return new CandidateSelection();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#copyPropertiesFrom(eu.stratosphere.sopremo.ISopremoType)
	 */
	@Override
	public void copyPropertiesFrom(ISopremoType original) {
		super.copyPropertiesFrom(original);
		this.passes.addAll(SopremoUtil.deepClone(((CandidateSelection) original).getPasses()));
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

		private List<EvaluationExpression> blockingKey = new ArrayList<EvaluationExpression>();

		/**
		 * Initializes Pass.
		 * 
		 * @param blockingKey
		 */
		public Pass(List<EvaluationExpression> blockingKey) {
			this.blockingKey = blockingKey;
		}

		/**
		 * Initializes Pass.
		 */
		public Pass() {
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.AbstractSopremoType#createCopy()
		 */
		@Override
		protected AbstractSopremoType createCopy() {
			return new Pass(SopremoUtil.deepClone(this.blockingKey));
		}

		/**
		 * Returns the value of blockingKey.
		 * 
		 * @return the blockingKey
		 */
		public List<EvaluationExpression> getBlockingKey() {
			return this.blockingKey;
		}

		/**
		 * Sets the value of blockingKey to the given value.
		 * 
		 * @param blockingKey
		 *        the blockingKey to set
		 */
		public void setBlockingKey(List<EvaluationExpression> blockingKey) {
			if (blockingKey == null)
				throw new NullPointerException("blockingKey must not be null");

			this.blockingKey = blockingKey;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
		 */
		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			SopremoUtil.append(appendable, this.blockingKey);
		}
	}
}
