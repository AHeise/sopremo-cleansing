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

import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JavaToJsonMapper;

/**
 * @author arv
 */
public class ReadConfigurationExpression extends EvaluationExpression {
	private final String key;

	private final IJsonNode target;

	private boolean initialized;

	public ReadConfigurationExpression(String key, IJsonNode target) {
		this.key = key;
		this.target = target;
	}

	/**
	 * Initializes ReadConfigurationExpression.
	 */
	ReadConfigurationExpression() {
		this.key = null;
		this.target = null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode evaluate(IJsonNode node) {
		if (!this.initialized) {
			JavaToJsonMapper.INSTANCE.map(SopremoEnvironment.getInstance().getConfiguration().getString(this.key, ""),
				this.target, this.target.getType());
			this.initialized = true;
		}

		return this.target;
	}

}
