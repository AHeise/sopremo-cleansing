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
package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.Comparator;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * 
 */
final class ExpressionSorter implements Comparator<IJsonNode> {
	/**
	 * 
	 */
	private final EvaluationExpression leftKey;

	/**
	 * 
	 */
	private final EvaluationExpression rightKey;

	/**
	 * Initializes ExpressionSorter.
	 * 
	 * @param leftKey
	 * @param rightKey
	 */
	ExpressionSorter(EvaluationExpression sortingKey) {
		this.leftKey = sortingKey.clone();
		this.rightKey = sortingKey.clone();
	}

	@Override
	public int compare(IJsonNode left, IJsonNode right) {
		return this.leftKey.evaluate(left).compareTo(this.rightKey.evaluate(right));
	}
}