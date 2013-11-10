package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.cleansing.duplicatedection.NaiveDuplicateDetection;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Tests {@link Naive} {@link InterSourceRecordLinkage} with one data source.
 * 
 * @author Arvid Heise
 */
public class NaiveDuplicateDetectionTest extends DuplicateDetectionTestBase<NaiveDuplicateDetection> {

	/**
	 * Initializes NaiveRecordLinkageIntraSourceTest.
	 * 
	 * @param resultProjection
	 * @param useId
	 */
	public NaiveDuplicateDetectionTest(
			final EvaluationExpression resultProjection, final boolean useId) {
		super(resultProjection, useId);
	}

	@Override
	protected DuplicateDetectionImplementation getImplementation() {
		return DuplicateDetectionImplementation.NAIVE;
	}

	@Override
	protected void generateExpectedPairs(List<IJsonNode> input, CandidateComparison comparison) {
		if (comparison.getIdProjection() == null) {
			comparison.setPreselect(new NodeOrderSelector(input));
		}
		
		final BooleanExpression condition = comparison.asCondition(true);
		for (final IJsonNode left : input) {
			for (final IJsonNode right : input) {
				if (condition.evaluate(JsonUtil.asArray(left, right)).getBooleanValue())
					this.emitCandidate(left, right);
			}
		}
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[] projections = { null, getAggregativeProjection() };
		final boolean[] useIds = { false, true };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression projection : projections)
			for (final boolean useId : useIds)
				parameters.add(new Object[] { projection, useId });

		return parameters;
	}
}
