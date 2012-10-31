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

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

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

	public CompositeDuplicateDetectionAlgorithm<?> createMatchingAlgorithm() {
		return new NaiveDuplicateDetection();
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append("CandidateSelection [passes=").append(this.passes).append("]");
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

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
		 */
		@Override
		public void toString(StringBuilder builder) {
			builder.append(blockingKey);
		}

	}
}
