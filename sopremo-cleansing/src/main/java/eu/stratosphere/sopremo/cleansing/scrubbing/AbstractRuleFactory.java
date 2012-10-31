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
package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

/**
 * @author Arvid Heise
 */
public abstract class AbstractRuleFactory implements RuleFactory {
	private Class<?> ruleType;

	/**
	 * Initializes AbstractRuleFactory.
	 */
	public AbstractRuleFactory(Class<?> ruleType) {
		this.ruleType = ruleType;
	}

	/**
	 * Initializes AbstractRuleFactory.
	 */
	public AbstractRuleFactory() {
		this(null);
	}

	@Override
	public EvaluationExpression createRule(EvaluationExpression expression, RuleContext context) {
		if (this.ruleType != null && this.ruleType.isInstance(expression))
			return expression;
		return this.transform(expression, context);
	}

	protected abstract EvaluationExpression transform(EvaluationExpression expression, RuleContext context );
}
