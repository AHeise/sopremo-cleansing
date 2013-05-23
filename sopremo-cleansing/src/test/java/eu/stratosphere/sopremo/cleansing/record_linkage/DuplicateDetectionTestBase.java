package eu.stratosphere.sopremo.cleansing.record_linkage;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.DuplicateDetection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Base for inner source {@link InterSourceRecordLinkage} test cases within one source.
 * 
 * @author Arvid Heise
 * @param <P>
 *        the {@link RecordLinkageAlgorithm}
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

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test
	public void pactCodeShouldPerformLikeStandardImplementation() {

		final DuplicateDetection dd = new DuplicateDetection();
		dd.setCandidateSelection(getCandidateSelection());
		dd.setImplementation(getImplementation());
		if (this.useId) {
			dd.getComparison().setIdProjection(new ObjectAccess("id"));
		}
		if (this.resultProjection != null)
			dd.getComparison().setResultProjection(this.resultProjection);
		
		this.sopremoTestPlan = createTestPlan(dd);

		this.generateExpectedPairs(this.sopremoTestPlan.getInput(0), dd.getComparison());

		try {
			this.sopremoTestPlan.run();
		} catch (final AssertionError error) {
			throw new AssertionError(String.format("For test %s: %s", this, error.getMessage()));
		}
	}

	/**
	 * @return
	 */
	protected CandidateSelection getCandidateSelection() {
		return new CandidateSelection();
	}

	/**
	 * Generates the expected pairs and invokes {@link #emitCandidate(KeyValuePair, KeyValuePair)}.
	 * 
	 * @param input
	 */
	protected abstract void generateExpectedPairs(Input input, CandidateComparison comparison);

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

		this.sopremoTestPlan.getExpectedOutput(0).add(resultProjection.evaluate(JsonUtil.asArray(left, right)));
	}

	/**
	 * Returns the context of the test plan.
	 * 
	 * @return the context
	 */
	protected EvaluationContext getContext() {
		return this.sopremoTestPlan.getEvaluationContext();
	}

	/**
	 * Creates the algorithm with the similarityFunction and threshold
	 * 
	 * @return the configured algorithm
	 */
	protected abstract DuplicateDetectionImplementation getImplementation();

	/**
	 * Creates a test plan for the record linkage operator.
	 * 
	 * @param recordLinkage
	 * @param useId
	 * @param projection
	 * @return the generated test plan
	 */
	protected SopremoTestPlan createTestPlan(DuplicateDetection dd) {

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
		aggregating.addMapping("name", new ObjectAccess("first name"));
		aggregating.addMapping("id", new ObjectAccess("id"));

		return aggregating;
	}
}
