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

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * 
 */
public class TransitiveClosureTest {
	@Test
	public void shouldCloseTriad() {
		TransitiveClosure transitiveClosure = new TransitiveClosure();
		SopremoTestPlan testPlan = new SopremoTestPlan(getSortedResults(transitiveClosure));

		testPlan.getInput(0).
			addArray("a", "b").
			addArray("b", "c");
		testPlan.getExpectedOutput(0).
			addArray("a", "b", "c");

		testPlan.run();
	}

	@Test
	public void shouldCloseTranstively() {
		TransitiveClosure transitiveClosure = new TransitiveClosure();
		SopremoTestPlan testPlan = new SopremoTestPlan(getSortedResults(transitiveClosure));

		testPlan.getInput(0).
			addArray("a", "b").
			addArray("b", "c").
			addArray("c", "d").
			addArray("d", "e");
		testPlan.getExpectedOutput(0).
			addArray("a", "b", "c", "d", "e");

		testPlan.run();
	}

	@Test
	public void shouldClose2Components() {
		TransitiveClosure transitiveClosure = new TransitiveClosure();
		SopremoTestPlan testPlan = new SopremoTestPlan(getSortedResults(transitiveClosure));

		testPlan.getInput(0).
			addArray("a", "b").
			addArray("b", "c").
			addArray("c", "d").
			addArray("w", "x").
			addArray("u", "v").
			addArray("u", "x");
		testPlan.getExpectedOutput(0).
			addArray("a", "b", "c", "d").
			addArray("u", "v", "w", "x");

		testPlan.run();
	}

	@Test
	public void shouldClose2UnorderedComponents() {
		TransitiveClosure transitiveClosure = new TransitiveClosure();
		SopremoTestPlan testPlan = new SopremoTestPlan(getSortedResults(transitiveClosure));

		testPlan.getInput(0).
			addArray("a", "b").
			addArray("b", "c").
			addArray("c", "d").
			addArray("w", "x").
			addArray("v", "u").
			addArray("x", "u");
		testPlan.getExpectedOutput(0).
			addArray("a", "b", "c", "d").
			addArray("u", "v", "w", "x");

		testPlan.run();
	}

	@Test
	public void shouldSupportVarlength() {
		TransitiveClosure transitiveClosure = new TransitiveClosure();
		SopremoTestPlan testPlan = new SopremoTestPlan(getSortedResults(transitiveClosure));

		testPlan.getInput(0).
			addArray("aa", "b").
			addArray("b", "ac").
			addArray("ac", "d");
		testPlan.getExpectedOutput(0).
			addArray("aa", "ac", "b", "d");

		testPlan.run();
	}
	
	@Test
	public void shouldRetainSourcePosition() {
		TransitiveClosure transitiveClosure = new TransitiveClosure().withSourcePositionRetained(true);
		SopremoTestPlan testPlan = new SopremoTestPlan(getSortedResults(transitiveClosure));

		testPlan.getInput(0).
			addArray("aa", "b").
			addArray("ac", "b");
		testPlan.getExpectedOutput(0).
			addArray(JsonUtil.createArrayNode("aa", 0), JsonUtil.createArrayNode("ac", 0), JsonUtil.createArrayNode("b", 1));

		testPlan.run();
	}

	private Operator<?> getSortedResults(TransitiveClosure transitiveClosure) {
		return new Projection().
			withInputs(transitiveClosure).
			withResultProjection(FunctionUtil.createFunctionCall(CoreFunctions.SORT, EvaluationExpression.VALUE));
	}
}
