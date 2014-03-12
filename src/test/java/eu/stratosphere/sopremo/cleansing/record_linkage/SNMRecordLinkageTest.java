package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.*;

import org.junit.Ignore;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.*;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Tests {@link SortedNeighborhood} with two data sources.
 * 
 * @author Arvid Heise
 */
@Ignore
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
			Collections.sort(leftInput, new ExpressionSorter(this.leftSortingKeys[keyIndex]));
			Collections.sort(rightInput, new ExpressionSorter(this.rightSortingKeys[keyIndex]));

			for (int leftIndex = 0, size = leftInput.size(); leftIndex < size; leftIndex++) {
				IJsonNode left = leftInput.get(leftIndex);
				final IJsonNode leftKey = this.leftSortingKeys[keyIndex].evaluate(left);
				int rightIndex = 0;
				for (; rightIndex < rightInput.size() - 1; rightIndex++)
					if (leftKey.compareTo(this.rightSortingKeys[keyIndex].evaluate(rightInput.get(rightIndex))) >= 0)
						break;

				for (int index = Math.max(0, rightIndex - this.windowSize); index < Math.min(size, rightIndex +
					this.windowSize - 1); index++) {
					IJsonNode right = rightInput.get(index);
					comparison.performComparison(left, right, this.duplicateCollector);
				}
			}
		}

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkageTestBase#getImplementation()
	 */
	@Override
	protected DuplicateDetectionImplementation getImplementation() {
		return DuplicateDetectionImplementation.SNM;
	}

	@Override
	public String toString() {
		return String.format("%s, leftSortingKeys=%s, rightSortingKeys=%s", super.toString(),
			Arrays.toString(this.leftSortingKeys), Arrays.toString(this.rightSortingKeys));
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
