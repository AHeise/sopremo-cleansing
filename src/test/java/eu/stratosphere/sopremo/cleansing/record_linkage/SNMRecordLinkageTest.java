package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Ignore;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.Blocking;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Tests {@link DisjunctPartitioning} {@link InterSourceRecordLinkage} with two data sources.
 * 
 * @author Arvid Heise
 */
@Ignore
public class SNMRecordLinkageTest extends RecordLinkageTestBase<Blocking> {

	private final EvaluationExpression[] leftBlockingKeys, rightBlockingKeys;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param resultProjection1
	 * @param resultProjection2
	 * @param blockingKeys
	 */
	public SNMRecordLinkageTest(final EvaluationExpression resultProjection, final String[][] blockingKeys) {
		super(resultProjection);

		this.leftBlockingKeys = new EvaluationExpression[blockingKeys[0].length];
		for (int index = 0; index < this.leftBlockingKeys.length; index++)
			this.leftBlockingKeys[index] = new ObjectAccess(blockingKeys[0][index]);
		this.rightBlockingKeys = new EvaluationExpression[blockingKeys[1].length];
		for (int index = 0; index < this.rightBlockingKeys.length; index++)
			this.rightBlockingKeys[index] = new ObjectAccess(blockingKeys[1][index]);

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.record_linkage.DuplicateDetectionTestBase#getCandidateSelection()
	 */
	@Override
	protected CandidateSelection getCandidateSelection() {
		final CandidateSelection candidateSelection = super.getCandidateSelection();
		for (int index = 0; index < this.leftBlockingKeys.length; index++)
			candidateSelection.addPass(this.leftBlockingKeys[index], this.rightBlockingKeys[index]);
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
	protected void generateExpectedPairs(Input leftInput, Input rightInput, CandidateComparison candidateComparison) {
		for (final IJsonNode left : leftInput)
			for (final IJsonNode right : rightInput) {
				boolean inSameBlockingBin = false;
				for (int index = 0; index < this.leftBlockingKeys.length && !inSameBlockingBin; index++)
					if (this.leftBlockingKeys[index].evaluate(left).equals(
						this.rightBlockingKeys[index].evaluate(right)))
						inSameBlockingBin = true;
				if (inSameBlockingBin)
					this.emitCandidate(left, right);
			}

	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkageTestBase#getImplementation()
	 */
	@Override
	protected DuplicateDetectionImplementation getImplementation() {
		return DuplicateDetectionImplementation.BLOCKING;
	}

	@Override
	public String toString() {
		return String.format("%s, leftBlockingKeys=%s, rightBlockingKeys=%s", super.toString(),
			Arrays.toString(this.leftBlockingKeys), Arrays.toString(this.rightBlockingKeys));
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
				parameters.add(new Object[] { projection, combinedBlockingKeys });

		return parameters;
	}
}
