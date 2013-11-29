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
package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * @author arv
 */
public class DDParseTest extends MeteorParseTest {

	@Test
	public void testNaive() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$persons = read from 'file:///input';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7;" +
			"write $duplicates to 'file:///output';");

		final SopremoPlan expectedPlan = new SopremoPlan();
//		final Source input = new Source("file://person.json");
//		final DuplicateDetection duplicateDetection = new DuplicateDetection().
//				withInputs(input).
//				withComparison(new CandidateComparison().withInnerSource(true).)
//				
//		final Sink output = new Sink("file://output.json").withInputs(duplicateDetection);
//		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testBlocking() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$persons = read from 'file:///input';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7" +
			"  partition on $persons.age;" +
			"write $duplicates to 'file:///output';");

	}

	@Test
	public void testSorting() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$persons = read from 'file:///input';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7" +
			"  sort on $persons.age" +
			"  with window 20;" +
			"write $duplicates to 'file:///output';");

	}
}
