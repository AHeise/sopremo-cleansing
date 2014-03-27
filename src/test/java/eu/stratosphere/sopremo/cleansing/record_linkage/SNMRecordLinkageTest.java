package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.Blocking;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm;
import eu.stratosphere.sopremo.cleansing.duplicatedection.SortedNeighborhood;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Tests {@link SortedNeighborhood} with two data sources.
 * 
 * @author Arvid Heise
 */
public class SNMRecordLinkageTest extends RecordLinkageTestBase<Blocking> {

	private final EvaluationExpression[] leftSortingKeys, rightSortingKeys;

	private final int windowSize;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param blockingKeys
	 */
	public SNMRecordLinkageTest(final EvaluationExpression resultProjection, final int windowSize,
			final String[][] blockingKeys) {
		super(resultProjection);

		this.windowSize = windowSize;

		this.leftSortingKeys = new EvaluationExpression[blockingKeys[0].length];
		for (int index = 0; index < this.leftSortingKeys.length; index++)
			this.leftSortingKeys[index] =
				new ArrayCreation(new ObjectAccess(blockingKeys[0][index]), new ObjectAccess("id"));
		this.rightSortingKeys = new EvaluationExpression[blockingKeys[1].length];
		for (int index = 0; index < this.rightSortingKeys.length; index++)
			this.rightSortingKeys[index] =
				new ArrayCreation(new ObjectAccess(blockingKeys[1][index]), new ObjectAccess("id2"));

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.record_linkage.DuplicateDetectionTestBase#getCandidateSelection()
	 */
	@Override
	protected CandidateSelection getCandidateSelection() {
		final CandidateSelection candidateSelection = super.getCandidateSelection();
		for (int index = 0; index < this.leftSortingKeys.length; index++)
			candidateSelection.withPass(this.leftSortingKeys[index], this.rightSortingKeys[index]);
		return candidateSelection;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkageTestBase#generateExpectedPairs(eu.stratosphere.
	 * sopremo.testing.SopremoTestPlan.Input, eu.stratosphere.sopremo.SopremoTestPlan.Input,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison)
	 */
	@Override
	protected void generateExpectedPairs(List<IJsonNode> leftInput, List<IJsonNode> rightInput,
			CandidateComparison comparison) {
		for (int keyIndex = 0; keyIndex < this.leftSortingKeys.length; keyIndex++) {
			final EvaluationExpression leftKeyExpression = this.leftSortingKeys[keyIndex], rightSortingKey =
				this.rightSortingKeys[keyIndex];
			Collections.sort(leftInput, new ExpressionSorter(leftKeyExpression));
			Collections.sort(rightInput, new ExpressionSorter(rightSortingKey));

			int rightIndex = 0;
			for (int leftIndex = 0, leftSize = leftInput.size(), rightSize = rightInput.size(); leftIndex < leftSize; leftIndex++) {
				IJsonNode left = leftInput.get(leftIndex);
				final IJsonNode leftKey = leftKeyExpression.evaluate(left);
				for (; rightIndex < rightInput.size(); rightIndex++)
					if (leftKey.compareTo(rightSortingKey.evaluate(rightInput.get(rightIndex))) < 0)
						break;

				for (int index = Math.max(0, rightIndex - this.windowSize + 1); index < Math.min(rightSize, rightIndex +
					this.windowSize - 1); index++) {
					IJsonNode right = rightInput.get(index);
					comparison.performComparison(left, right, this.duplicateCollector);
				}
			}
		}

	}

	@Override
	protected CompositeDuplicateDetectionAlgorithm<?> getImplementation() {
		return new SortedNeighborhood().withWindowSize(this.windowSize);
	}
	
	@Override
	public String toString() {
		return String.format("%s, leftSortingKeys=%s, rightSortingKeys=%s, window=%s", super.toString(),
			Arrays.toString(this.leftSortingKeys), Arrays.toString(this.rightSortingKeys), this.windowSize);
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[] projections = { null,
			new ArrayCreation(getAggregativeProjection1(), getAggregativeProjection2()), };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression projection : projections)
			for (final String[][] combinedBlockingKeys : TestKeys.CombinedBlockingKeys)
				for (final int windowSize : new int[] { 2, 3 })
					parameters.add(new Object[] { projection, windowSize, combinedBlockingKeys });

		return parameters;
	}
}
