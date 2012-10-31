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

import eu.stratosphere.sopremo.operator.ElementaryOperator;

/**
 * @author Arvid Heise
 */
public abstract class ElementaryDuplicateDetectionAlgorithm<ImplType extends ElementaryDuplicateDetectionAlgorithm<ImplType>>
		extends ElementaryOperator<ImplType> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8029175167683349177L;

	private CandidateComparison comparison = new CandidateComparison();

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
	 * @return this
	 */
	public ImplType withComparison(CandidateComparison comparison) {
		setComparison(comparison);
		return self();
	}
}
