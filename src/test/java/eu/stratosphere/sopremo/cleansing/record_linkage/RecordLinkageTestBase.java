package eu.stratosphere.sopremo.cleansing.record_linkage;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.RecordLinkage;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CompositeDuplicateDetectionAlgorithm;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.testing.SopremoTestPlan.Input;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Base for inner source {@link RecordLinkage} test cases between at least two sources.
 * 
 * @author Arvid Heise
 * @param <P>
 *        the {@link CompositeDuplicateDetectionAlgorithm}
 */
@RunWith(Parameterized.class)
@Ignore
public abstract class RecordLinkageTestBase<P extends CompositeDuplicateDetectionAlgorithm<P>> {
	private final EvaluationExpression resultProjection;

	private SopremoTestPlan sopremoTestPlan;

	/**
	 * Initializes InterSourceRecordLinkageAlgorithmTestBase.
	 */
	protected RecordLinkageTestBase(EvaluationExpression resultProjection) {
		this.resultProjection = resultProjection;
	}

	/**
	 * Performs the naive record linkage in place and compares with the Pact code.
	 */
	@Test 
	public void pactCodeShouldPerformLikeStandardImplementation() {
		final RecordLinkage recordLinkage = new RecordLinkage();
		recordLinkage.setImplementation(getImplementation());
		recordLinkage.setCandidateSelection(getCandidateSelection());
		if (this.resultProjection != null)
			recordLinkage.getComparison().setResultProjection(this.resultProjection);
		
		this.sopremoTestPlan = createTestPlan(recordLinkage);

		this.generateExpectedPairs(this.sopremoTestPlan.getInput(0), this.sopremoTestPlan.getInput(1), recordLinkage.getComparison());

		try {
			this.sopremoTestPlan.trace();
			this.sopremoTestPlan.run();
		} catch (final AssertionError error) {
			throw new AssertionError(String.format("For test %s: %s", this, error.getMessage()));
		}
	}

	/**
	 * Generates the expected pairs and invokes {@link #emitCandidate(IJsonNode, IJsonNode)}.
	 * 
	 * @param leftInput
	 * @param rightInput
	 */
	protected abstract void generateExpectedPairs(Input leftInput, Input rightInput,
			CandidateComparison candidateComparison);

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

		this.sopremoTestPlan.getExpectedOutput(0).add(
			resultProjection.evaluate(JsonUtil.asArray(left, right)).clone());
	}

	@Override
	public String toString() {
		return String.format("resultProjection=%s", this.resultProjection);
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
	protected abstract DuplicateDetectionImplementation getImplementation();

	/**
	 * Creates a test plan for the record linkage operator.
	 * 
	 * @param recordLinkage
	 * @return the generated test plan
	 */
	protected static SopremoTestPlan createTestPlan(RecordLinkage recordLinkage) {
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(recordLinkage);
		sopremoTestPlan.getInput(0).
			addObject("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80).
			addObject("id", 1, "first name", "berta", "last name", "typo", "age", 70).
			addObject("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70).
			addObject("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75).
			addObject("id", 4, "first name", "elma", "last name", "firstNameDiffers", "age", 60);
		sopremoTestPlan.getInput(1).
			addObject("id2", 10, "firstName", "albert", "lastName", "perfect duplicate", "age", 80).
			addObject("id2", 11, "firstName", "berta", "lastName", "tpyo", "age", 70).
			addObject("id2", 12, "firstName", "charles", "lastName", "age inaccurate", "age", 69).
			addObject("id2", 14, "firstName", "elmar", "lastName", "firstNameDiffers", "age", 60);
		return sopremoTestPlan;
	}

	protected CandidateSelection getCandidateSelection() {
		return new CandidateSelection();
	}

	/**
	 * Returns a duplicate projection expression that collects some fields.
	 * 
	 * @return an aggregating expression
	 */
	protected static EvaluationExpression getAggregativeProjection1() {
		final ObjectCreation aggregating = new ObjectCreation();
		aggregating.addMapping("name", JsonUtil.createPath("0", "first name"));
		aggregating.addMapping("id", JsonUtil.createPath("0", "id"));

		return aggregating;
	}

	/**
	 * Returns a duplicate projection expression that collects some fields.
	 * 
	 * @return an aggregating expression
	 */
	protected static EvaluationExpression getAggregativeProjection2() {
		final ObjectCreation aggregating = new ObjectCreation();
		aggregating.addMapping("name", JsonUtil.createPath("1", "firstName"));
		aggregating.addMapping("id", JsonUtil.createPath("1", "id2"));

		return aggregating;
	}

}
