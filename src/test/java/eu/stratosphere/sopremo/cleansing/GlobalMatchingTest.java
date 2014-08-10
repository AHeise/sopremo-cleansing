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

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * 
 */
public class GlobalMatchingTest {
	@Test
	public void test1() {
		GlobalMatching gm = new GlobalMatching()
			.withSimilarityExpression(new ArithmeticExpression(
				new ArrayAccess(0), ArithmeticOperator.SUBTRACTION,
				new ArrayAccess(1)));
		SopremoTestPlan testPlan = new SopremoTestPlan(gm);

		testPlan.getInput(0).addArray(1, 3).addArray(2, 3);
		testPlan.getExpectedOutput(0).addArray(2, 3);

		testPlan.trace();
		testPlan.run();
	}

	@Test
	public void test2() {
		GlobalMatching gm = new GlobalMatching()
			.withSimilarityExpression(new ArithmeticExpression(
				new ArrayAccess(0), ArithmeticOperator.SUBTRACTION,
				new ArrayAccess(1)));
		SopremoTestPlan testPlan = new SopremoTestPlan(gm);

		testPlan.getInput(0).addArray(1, 5).addArray(1, 4).addArray(1, 3)
			.addArray(1, 2).addArray(2, 3);
		testPlan.getExpectedOutput(0).addArray(2, 2).addArray(1, 3);

		testPlan.trace();
		testPlan.run();
	}

	@Test
	public void testWithThresholdA() {
		GlobalMatching gm = new GlobalMatching()
			.withSimilarityExpression(new ComparativeExpression(new ArithmeticExpression(new ConstantExpression(IntNode.ZERO),
				ArithmeticOperator.SUBTRACTION, new FunctionCall(
					NumericUDFs.ABS, new ArithmeticExpression(
						new ArrayAccess(0),
						ArithmeticOperator.SUBTRACTION,
						new ArrayAccess(1)))), BinaryOperator.GREATER, new ConstantExpression(IntNode.valueOf(-5))));
		SopremoTestPlan testPlan = new SopremoTestPlan(gm);

		testPlan.getInput(0).addArray(1, 2).addArray(2.1, 2).addArray(2.1, 4)
			.addArray(4.1, 4).addArray(4.1, 6);
		testPlan.getExpectedOutput(0).addArray(2.1, 2).addArray(4.1, 4);

		testPlan.trace();
		testPlan.run();
	}

	@Test
	public void testWithThresholdB() {
		GlobalMatching gm = new GlobalMatching()
			.withSimilarityExpression(new ComparativeExpression(new ArithmeticExpression(new ConstantExpression(IntNode.ZERO),
				ArithmeticOperator.SUBTRACTION, new FunctionCall(
					NumericUDFs.ABS, new ArithmeticExpression(
						new ArrayAccess(0),
						ArithmeticOperator.SUBTRACTION,
						new ArrayAccess(1)))), BinaryOperator.GREATER_EQUAL, new ConstantExpression(IntNode.valueOf(-7))));
		SopremoTestPlan testPlan = new SopremoTestPlan(gm);

		testPlan.getInput(0).addArray(1, 2).addArray(2.1, 2).addArray(2.1, 4)
			.addArray(4.1, 4).addArray(4.1, 6);
		testPlan.getExpectedOutput(0).addArray(2.1, 2).addArray(4.1, 4).addArray(1, 6);

		testPlan.trace();
		testPlan.run();
	}

	@Test
	public void testWithoutThreshold() {
		GlobalMatching gm = new GlobalMatching()
			.withSimilarityExpression(new ArithmeticExpression(new ConstantExpression(IntNode.ZERO),
				ArithmeticOperator.SUBTRACTION, new FunctionCall(
					NumericUDFs.ABS, new ArithmeticExpression(
						new ArrayAccess(0),
						ArithmeticOperator.SUBTRACTION,
						new ArrayAccess(1)))));
		SopremoTestPlan testPlan = new SopremoTestPlan(gm);

		testPlan.getInput(0).addArray(1, 2).addArray(2.1, 2).addArray(2.1, 4)
			.addArray(4.1, 4).addArray(4.1, 6);
		testPlan.getExpectedOutput(0).addArray(2.1, 2).addArray(4.1, 4).addArray(1, 6);

		testPlan.trace();
		testPlan.run();
	}
}
