package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm;
import eu.stratosphere.sopremo.cleansing.duplicatedection.NaiveDuplicateDetection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.PairFilter;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Tests {@link NaiveDuplicateDetection} with one data source.
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
	protected CompositeDuplicateDetectionAlgorithm<?> getImplementation() {
		return new NaiveDuplicateDetection();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.record_linkage.DuplicateDetectionTestBase#generateExpectedPairs(java.util.List,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.PairFilter,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison)
	 */
	@Override
	protected void generateExpectedPairs(List<IJsonNode> input, PairFilter pairFilter, CandidateComparison comparison) {
		BooleanExpression preselect;
		if (pairFilter.isConfigured())
			preselect = pairFilter;
		else
			preselect = new NodeOrderSelector(input);

		final BooleanExpression condition = comparison.asCondition();
		for (final IJsonNode left : input) {
			for (final IJsonNode right : input) {
				final IArrayNode<IJsonNode> pair = JsonUtil.asArray(left, right);
				if (preselect.evaluate(pair).getBooleanValue() && condition.evaluate(pair).getBooleanValue())
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
