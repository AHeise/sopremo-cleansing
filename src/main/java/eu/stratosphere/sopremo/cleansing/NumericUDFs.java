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

import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.function.SopremoFunction2;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.INumericNode;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * @author Arvid Heise, Fabian Tschirschnitz
 */
public class NumericUDFs implements BuiltinProvider {

	@Name(noun = "absDiff")
	public static SopremoFunction ABS_DIFF = new SopremoFunction2<IJsonNode, IJsonNode>() {
		@Override
		protected IntNode call(IJsonNode element1, IJsonNode element2) {
			if (element1 instanceof IntNode
					&& element2 instanceof IntNode) {
				final int element1IntValue = ((IntNode) element1).getBigIntegerValue().intValue();
				final int element2IntValue = ((IntNode) element2).getBigIntegerValue().intValue();
				final int diff = element1IntValue-element2IntValue;
				final int absDiff = Math.abs(diff);
				return IntNode.valueOf(absDiff);
			}
			return IntNode.ZERO;
		}
	};
	
	@Name(noun = "diff")
	public static SopremoFunction DIFF = new SopremoFunction2<IJsonNode, IJsonNode>() {
		@Override
		protected IntNode call(IJsonNode element1, IJsonNode element2) {
			if (element1 instanceof IntNode
					&& element2 instanceof IntNode) {
				final int element1IntValue = ((IntNode) element1).getBigIntegerValue().intValue();
				final int element2IntValue = ((IntNode) element2).getBigIntegerValue().intValue();
				final int diff = element1IntValue-element2IntValue;
				return IntNode.valueOf(diff);
			}
			return IntNode.ZERO;
		}
	};
	
	@Name(verb = "abs")
	public static final SopremoFunction ABS = new SopremoFunction1<INumericNode>() {
		/**
		 * returns the absolute value of a number
		 * 
		 * @param number
		 *        the number the absolute value should be returned
		 * @return absolute value of the number
		 */
		@Override
		protected IJsonNode call(final INumericNode number) {
			return new DoubleNode(Math.abs(number.getDoubleValue()));
		}
	};
}