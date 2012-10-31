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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;

/**
 * @author Arvid Heise
 */
public class ValueTreeContains extends BooleanExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5454820392452557849L;

	private final IJsonNode valueToFind;

	public ValueTreeContains(IJsonNode valueToFind) {
		this.valueToFind = valueToFind;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
		return BooleanNode.valueOf(isContainedIn(node));
	}

	/**
	 * @param node
	 * @param valueToFind2
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private boolean isContainedIn(IJsonNode node) {
		if (node.equals(this.valueToFind))
			return true;
		if (node.isArray()) {
			for (IJsonNode element : (Iterable<IJsonNode>) node)
				if (isContainedIn(element))
					return true;
		}
		else if (node.isObject())
			for (Entry<String, IJsonNode> element : (IObjectNode) node)
				if (isContainedIn(element.getValue()))
					return true;
		return false;
	}

}
