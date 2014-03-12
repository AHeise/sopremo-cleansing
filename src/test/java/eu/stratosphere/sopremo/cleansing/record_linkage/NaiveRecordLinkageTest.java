package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.cleansing.duplicatedection.NaiveDuplicateDetection;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Tests {@link NaiveDuplicateDetection} with two data sources.
 * 
 * @author Arvid Heise
 */
public class NaiveRecordLinkageTest extends RecordLinkageTestBase<NaiveDuplicateDetection> {

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 */
	public NaiveRecordLinkageTest(final EvaluationExpression resultProjection) {
		super(resultProjection);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkageTestBase#generateExpectedPairs(eu.stratosphere.
	 * sopremo.testing.SopremoTestPlan.Input, eu.stratosphere.sopremo.SopremoTestPlan.Input,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison)
	 */
	@Override
	protected void generateExpectedPairs(List<IJsonNode> leftInput, List<IJsonNode> rightInput, CandidateComparison candidateComparison) {
		for (final IJsonNode left : leftInput)
			for (final IJsonNode right : rightInput)
				emitCandidate(left, right);
	}

	@Override
	protected DuplicateDetectionImplementation getImplementation() {
		return DuplicateDetectionImplementation.NAIVE;
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
			parameters.add(new Object[] { projection });

		return parameters;
	}
}
