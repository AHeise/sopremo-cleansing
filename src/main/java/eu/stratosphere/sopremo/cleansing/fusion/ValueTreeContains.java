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
package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Map.Entry;

import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IStreamNode;

/**
 * @author Arvid Heise
 */
public class ValueTreeContains extends BooleanExpression {

	private final IJsonNode valueToFind;

	public ValueTreeContains(final IJsonNode valueToFind) {
		this.valueToFind = valueToFind;
	}

	/**
	 * Initializes ValueTreeContains.
	 */
	ValueTreeContains() {
		this.valueToFind = null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public BooleanNode evaluate(final IJsonNode node) {
		return BooleanNode.valueOf(this.isContainedIn(node));
	}

	/**
	 * @param node
	 * @param valueToFind2
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean isContainedIn(final IJsonNode node) {
		if (node.equals(this.valueToFind))
			return true;
		if (node instanceof IStreamNode<?>) {
			for (final IJsonNode element : (Iterable<IJsonNode>) node)
				if (this.isContainedIn(element))
					return true;
		}
		else if (node instanceof IObjectNode)
			for (final Entry<String, IJsonNode> element : (IObjectNode) node)
				if (this.isContainedIn(element.getValue()))
					return true;
		return false;
	}

}
