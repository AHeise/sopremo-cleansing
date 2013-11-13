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
package eu.stratosphere.sopremo.cleansing;

import java.io.IOException;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Tommy Neubert
 */
public class ConstraintMacro extends MacroBase {
	private final Class<? extends EvaluationExpression> constraintClass;

	public ConstraintMacro(Class<? extends EvaluationExpression> constraintClass, int numberOfParameters) {
		super(numberOfParameters);
		this.constraintClass = constraintClass;
	}

	/**
	 * Initializes ConstraintMacro.
	 */
	public ConstraintMacro(Class<? extends EvaluationExpression> wrappedExpressionClass) {
		this(wrappedExpressionClass, 1);
	}

	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append(this.getClass().getSimpleName());
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.function.MacroBase#process(eu.stratosphere.sopremo.expressions.EvaluationExpression[])
	 */
	@Override
	protected EvaluationExpression process(EvaluationExpression[] params) {
		return ReflectUtil.newInstance(this.constraintClass, params[0].evaluate(MissingNode.getInstance()));
	}

	/**
	 * Returns the constraintClass.
	 * 
	 * @return the constraintClass
	 */
	public Class<? extends EvaluationExpression> getConstraintClass() {
		return this.constraintClass;
	}

}
