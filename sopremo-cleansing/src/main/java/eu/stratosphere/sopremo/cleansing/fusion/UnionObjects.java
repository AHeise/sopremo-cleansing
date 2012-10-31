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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * <tt>[{attr1: "val1_1", attr2: "val2_1"}, {attr1: "val1_2", attr2: "val2_2"}]</tt> <br />
 * <tt>-&gt; {attr1: ["val1_1", "val1_2"], attr2: ["val2_1", "val2_2"]}</tt>
 * 
 * @author Arvid Heise
 */
public class UnionObjects extends EvaluationExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4998967360875190779L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
		IArrayNode
		return null;
	}

}
