package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Ignore;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.Blocking;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Tests {@link DisjunctPartitioning} {@link InterSourceRecordLinkage} within one data source.
 * 
 * @author Arvid Heise
 */
public class SNMDuplicateDetectionTest extends DuplicateDetectionTestBase<Blocking> {
	private final EvaluationExpression[] sortingKeys;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param projection
	 * @param useId
	 * @param blockingKeys
	 */
	public SNMDuplicateDetectionTest(final EvaluationExpression projection,
			final boolean useId, final String[][] sortingKeys) {
		super(projection, useId);

		this.sortingKeys = new EvaluationExpression[sortingKeys[0].length];
		for (int index = 0; index < this.sortingKeys.length; index++)
			this.sortingKeys[index] = new ObjectAccess(sortingKeys[0][index]);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.record_linkage.DuplicateDetectionTestBase#getCandidateSelection()
	 */
	@Override
	protected CandidateSelection getCandidateSelection() {
		final CandidateSelection candidateSelection = super.getCandidateSelection();
		for (EvaluationExpression blockingKey : this.sortingKeys)
			candidateSelection.addPass(blockingKey);
		return candidateSelection;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.record_linkage.DuplicateDetectionTestBase#generateExpectedPairs(eu.stratosphere.sopremo.SopremoTestPlan.Input, eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison)
	 */
	@Override
	protected void generateExpectedPairs(List<IJsonNode> input, CandidateComparison comparison) {
		if (comparison.getIdProjection() == null) {
			comparison.setPreselect(new NodeOrderSelector(input));
		}
		
//		Collections.sort(input, new Comparator<IJsonNode>() {
//		});
		final BooleanExpression condition = comparison.asCondition();
		for (final IJsonNode left : input) {
			for (final IJsonNode right : input) {
				boolean inSameBlockingBin = false;
				for (int index = 0; index < this.sortingKeys.length && !inSameBlockingBin; index++)
					if (this.sortingKeys[index].evaluate(left).equals(this.sortingKeys[index].evaluate(right)))
						inSameBlockingBin = true;
				if (inSameBlockingBin && condition.evaluate(JsonUtil.asArray(left, right)).getBooleanValue())
						this.emitCandidate(left, right);
			}
		}
	}

	@Override
	protected DuplicateDetectionImplementation getImplementation() {
		return DuplicateDetectionImplementation.SNM;
	}

	@Override
	public String toString() {
		return String.format("%s, blockingKeys=%s", super.toString(), Arrays.toString(this.sortingKeys));
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[] projections = { null, getAggregativeProjection() };
		final boolean[] useIds = { /*false,*/ true };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression projection : projections)
			for (final String[][] combinedBlockingKeys : TestKeys.CombinedBlockingKeys)
				for (final boolean useId : useIds)
					parameters.add(new Object[] { projection, useId, combinedBlockingKeys });

		return parameters;
	}

}
