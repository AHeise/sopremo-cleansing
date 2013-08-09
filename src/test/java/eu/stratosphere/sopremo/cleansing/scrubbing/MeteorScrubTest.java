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

import org.junit.Test;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.cleansing.Scrubbing;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.TernaryExpression;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * 
 */
public class MeteorScrubTest extends MeteorParseTest {
	@Test @org.junit.Ignore
	public void testFullScrub() {
		final SopremoPlan actualPlan = parseScript(
			"normalizeName = javaudf('" + MeteorScrubTest.class.getName() +  ".normalizeName');\n" +
			"$dirty = read from 'file://person.json';\n" +
			"$clean = scrub $dirty with rules {\n" +
			"	firstName: [required, &normalizeName],\n" +
			"	lastName: required,\n" +
			"};\n" +
			"write $clean to 'file://output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("file://person.json");
		final Scrubbing scrubbing = new Scrubbing().withInputs(input);
		scrubbing.addRule(new NonNullRule(), new ObjectAccess("firstName"));
		final EvaluationContext context = expectedPlan.getEvaluationContext();
		context.getFunctionRegistry().put(MeteorScrubTest.class);
		scrubbing.addRule(new FunctionCall("normalizeName", context, EvaluationExpression.VALUE), new ObjectAccess("firstName"));
		scrubbing.addRule(new NonNullRule(), new ObjectAccess("lastName"));
		scrubbing.addRule(new TernaryExpression(new ComparativeExpression(new ObjectAccess("year"), BinaryOperator.GREATER, new ConstantExpression(1900)), EvaluationExpression.VALUE, FilterRecord.Expression), new ObjectAccess("birthDay"));
		final Sink output = new Sink("file://output.json").withInputs(scrubbing);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}
	
	public static TextNode normalizeName(TextNode name) {
		return name;
	}
}
