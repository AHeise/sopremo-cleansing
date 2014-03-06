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
import java.math.BigDecimal;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.sopremo.cleansing.DuplicateDetection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.*;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.SelectionHint;
import eu.stratosphere.sopremo.cleansing.similarity.CoercingSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.PathSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityExpression;
import eu.stratosphere.sopremo.cleansing.similarity.text.LevenshteinSimilarity;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author arv
 */
public class DDParseTest extends MeteorParseTest {

	@Test
	public void testNaive() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$persons = read from 'file:///input.json';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7;" +
			"write $duplicates to 'file:///output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("file:/input.json");
		final DuplicateDetection duplicateDetection = new DuplicateDetection().
			withInputs(input).
			withComparison(getComparison());

		final Sink output = new Sink("file:/output.json").withInputs(duplicateDetection);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	private CandidateComparison getComparison() {
		final SimilarityExpression similarityExpression = new SimilarityExpression(new PathSimilarity<IJsonNode>(
			new ObjectAccess("firstName"),
			new CoercingSimilarity(new LevenshteinSimilarity()),
			new ObjectAccess("firstName")));
		return new CandidateComparison().
			withDuplicateExpression(
				new ComparativeExpression(similarityExpression,
					ComparativeExpression.BinaryOperator.GREATER_EQUAL,
					new ConstantExpression(new BigDecimal("0.7"))));
	}

	@Test
	public void testBlocking() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$persons = read from 'file:///input.json';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7" +
			"  partition on $persons.age;" +
			"write $duplicates to 'file:///output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("file:/input.json");
		final DuplicateDetection duplicateDetection = new DuplicateDetection().
			withImplementation(DuplicateDetectionImplementation.BLOCKING).
			withInputs(input).
			withComparison(getComparison()).
			withCandidateSelection(new CandidateSelection().withSelectionHint(SelectionHint.BLOCK).withPass(new ObjectAccess("age")));

		final Sink output = new Sink("file:/output.json").withInputs(duplicateDetection);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testSorting() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$persons = read from 'file:///input.json';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7" +
			"  sort on $persons.age" +
			"  with window 20;" +
			"write $duplicates to 'file:///output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("file:/input.json");
		final DuplicateDetection duplicateDetection = new DuplicateDetection().
			withImplementation(DuplicateDetectionImplementation.SNM).
			withInputs(input).
			withComparison(getComparison()).
			withCandidateSelection(new CandidateSelection().withSelectionHint(SelectionHint.SORT).withPass(new ObjectAccess("age")));
		((SortedNeighborhood) duplicateDetection.getAlgorithm()).setWindowSize(20);

		final Sink output = new Sink("file:/output.json").withInputs(duplicateDetection);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}
}
