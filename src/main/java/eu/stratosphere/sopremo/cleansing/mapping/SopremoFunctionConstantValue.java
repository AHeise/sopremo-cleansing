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
package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ConstantValue;
import it.unibas.spicy.model.correspondence.ISourceValue;

/**
 * 
 */
public class SopremoFunctionConstantValue extends ConstantValue implements ISourceValue {
	private SopremoFunctionExpression sopremoFunctionExpression;

	public SopremoFunctionConstantValue(SopremoFunctionExpression sfe) {
		super("0");
		this.sopremoFunctionExpression = sfe;
	}
	
	/**
	 * Returns the sopremoFunctionExpression.
	 * 
	 * @return the sopremoFunctionExpression
	 */
	public SopremoFunctionExpression getSopremoFunctionExpression() {
		return this.sopremoFunctionExpression;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result +  sopremoFunctionExpression.hashCode();
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SopremoFunctionConstantValue other = (SopremoFunctionConstantValue) obj;
		return sopremoFunctionExpression.equals(other.sopremoFunctionExpression);
	}

	
}
