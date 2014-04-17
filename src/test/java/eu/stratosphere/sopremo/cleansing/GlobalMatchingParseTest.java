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

import org.junit.Test;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * @author arv
 */
public class GlobalMatchingParseTest extends MeteorParseTest {

	@Test
	public void testNaive() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$pairs = read from 'file:///input1.json';" +
			"$cluster = global match $pair in $pairs " +
			"  with $pair[0].value - $pair[1].value;" +
			"write $cluster to 'file:///output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file:/input1.json");
		final GlobalMatching duplicateDetection = new GlobalMatching().
			withInputs(input1).
			withSimilarityExpression(getExpression());

		final Sink output = new Sink("file:/output.json").withInputs(duplicateDetection);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	private EvaluationExpression getExpression() {
		return new ArithmeticExpression(JsonUtil.createPath("0", "[0]", "value") , ArithmeticOperator.SUBTRACTION,
				JsonUtil.createPath("0", "[1]", "value"));
	}
}
