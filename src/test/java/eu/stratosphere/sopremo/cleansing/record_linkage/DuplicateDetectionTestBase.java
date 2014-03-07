package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.cleansing.DuplicateDetection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm;
import eu.stratosphere.sopremo.cleansing.duplicatedection.PairFilter;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Base for inner source {@link DuplicateDetection} test cases within one source.
 * 
 * @author Arvid Heise
 * @param <P>
 *        the {@link CompositeDuplicateDetectionAlgorithm}
 */
@RunWith(Parameterized.class)
@Ignore
public abstract class DuplicateDetectionTestBase<P extends CompositeDuplicateDetectionAlgorithm<P>> {
	private final EvaluationExpression resultProjection;

	private final boolean useId;

	private SopremoTestPlan sopremoTestPlan;

	/**
	 * Initializes IntraSourceRecordLinkageTestBase.
	 * 
	 * @param resultProjection
	 * @param useId
	 */
	public DuplicateDetectionTestBase(EvaluationExpression resultProjection, boolean useId) {
		this.resultProjection = resultProjection;
		this.useId = useId;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("%s with %s to %s", getClass().getSimpleName(), getCandidateSelection(),
			this.resultProjection);
	}

	protected JsonCollector<IJsonNode> duplicateCollector = new JsonCollector<IJsonNode>(SopremoEnvironment.getInstance().getEvaluationContext()) {
				/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.Collector#collect(java.lang.Object)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void collect(IJsonNode record) {
			final IArrayNode<IJsonNode> array = (IArrayNode<IJsonNode>) record;
			System.out.println(array);
			emitCandidate(array.get(0), array.get(1));
		}
	};

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test
	public void pactCodeShouldPerformLikeStandardImplementation() {

		final CompositeDuplicateDetectionAlgorithm<?> dd = getImplementation();
		dd.setCandidateSelection(getCandidateSelection());
		dd.setDegreeOfParallelism(2);
		if (this.useId) {
			dd.getPairFilter().setIdProjection(new ObjectAccess("id"));
		}
		if (this.resultProjection != null)
			dd.getComparison().setResultProjection(this.resultProjection);

		this.sopremoTestPlan = createTestPlan(dd);

		this.generateExpectedPairs(Lists.newArrayList(this.sopremoTestPlan.getInput(0)),		
			dd.getPairFilter(), dd.getComparison());

		try {
			this.sopremoTestPlan.trace();
			this.sopremoTestPlan.run();
		} catch (final AssertionError error) {
			throw new AssertionError(String.format("For test %s: %s", this, error.getMessage()));
		}
	}

	protected CandidateSelection getCandidateSelection() {
		return new CandidateSelection();
	}

	/**
	 * Generates the expected pairs and invokes {@link #emitCandidate(IJsonNode, IJsonNode)}.
	 * 
	 * @param input
	 * @param pairFilter 
	 */
	protected abstract void generateExpectedPairs(List<IJsonNode> input, PairFilter pairFilter, CandidateComparison comparison);

	/**
	 * Emit the candidate.
	 * 
	 * @param left
	 * @param right
	 */
	protected void emitCandidate(IJsonNode left, IJsonNode right) {
		EvaluationExpression resultProjection = this.resultProjection;
		if (resultProjection == null)
			resultProjection = EvaluationExpression.VALUE;

		this.sopremoTestPlan.getExpectedOutput(0).add(resultProjection.evaluate(JsonUtil.asArray(left, right)).clone());
	}

	/**
	 * Returns the context of the test plan.
	 * 
	 * @return the context
	 */
	protected EvaluationContext getContext() {
		return this.sopremoTestPlan.getCompilationContext();
	}

	/**
	 * Creates the algorithm with the similarityFunction and threshold
	 * 
	 * @return the configured algorithm
	 */
	protected abstract CompositeDuplicateDetectionAlgorithm<?> getImplementation();

	/**
	 * Creates a test plan for the record linkage operator.
	 * 
	 * @return the generated test plan
	 */
	protected SopremoTestPlan createTestPlan(CompositeDuplicateDetectionAlgorithm<?> dd) {
		if (!this.useId) {
			ArrayCreation swap = new ArrayCreation(new ArrayAccess(1), new ArrayAccess(0));
			TernaryExpression smallerIdFirst = new TernaryExpression(
				new ComparativeExpression(JsonUtil.createPath("[0]", "id"), BinaryOperator.LESS_EQUAL, JsonUtil.createPath("[1]", "id")),
				EvaluationExpression.VALUE, swap
				);
			dd.getComparison().setResultProjection(new ChainedSegmentExpression(
				smallerIdFirst, dd.getComparison().getResultProjection()));
		}

		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(dd);
		sopremoTestPlan.getInput(0).
			addObject("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80).
			addObject("id", 1, "first name", "berta", "last name", "typo", "age", 70).
			addObject("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70).
			addObject("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75).
			addObject("id", 4, "first name", "elma", "last name", "first nameDiffers", "age", 60).
			addObject("id", 5, "first name", "albert", "last name", "perfect duplicate", "age", 80).
			addObject("id", 6, "first name", "berta", "last name", "tpyo", "age", 70).
			addObject("id", 7, "first name", "charles", "last name", "age inaccurate", "age", 69).
			addObject("id", 8, "first name", "elmar", "last name", "first nameDiffers", "age", 60);
		return sopremoTestPlan;
	}

	/**
	 * Returns a duplicate projection expression that aggregates some fields to arrays.
	 * 
	 * @return an aggregating expression
	 */
	protected static EvaluationExpression getAggregativeProjection() {
		final ObjectCreation aggregating = new ObjectCreation();
		aggregating.addMapping("name", new ArrayProjection(new ObjectAccess("first name")));
		aggregating.addMapping("id", new ArrayProjection(new ObjectAccess("id")));

		return aggregating;
	}
}
