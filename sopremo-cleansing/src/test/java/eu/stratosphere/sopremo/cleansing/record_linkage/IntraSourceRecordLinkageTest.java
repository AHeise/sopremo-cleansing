package eu.stratosphere.sopremo.cleansing.record_linkage;

import static eu.stratosphere.sopremo.JsonUtil.createIArrayNode;
import static eu.stratosphere.sopremo.JsonUtil.createObjectNode;
import static eu.stratosphere.sopremo.JsonUtil.createPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import uk.ac.shef.wit.simmetrics.similaritymetrics.Levenshtein;
import eu.stratosphere.sopremo.DefaultFunctions;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.cleansing.similarity_condition.NumericDifference;
import eu.stratosphere.sopremo.cleansing.similarity_condition.SimmetricFunction;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Tests {@link IntraSourceRecordLinkage}.
 * 
 * @author Arvid Heise
 */
@RunWith(Parameterized.class)
public class IntraSourceRecordLinkageTest {
	private PathExpression similarityFunction;

	private boolean useId;

	private List<IJsonNode> inputs = new ArrayList<IJsonNode>();

	private EvaluationExpression resultProjection;

	/**
	 * Initializes IntraSourceRecordLinkageTest.
	 * 
	 * @param resultProjection
	 * @param useId
	 */
	public IntraSourceRecordLinkageTest(EvaluationExpression resultProjection, boolean useId) {
		this.useId = useId;
		this.resultProjection = resultProjection;

		final SimmetricFunction firstNameLev = new SimmetricFunction(new Levenshtein(),
			createPath("0", "first name"), createPath("1", "first name"));
		final SimmetricFunction lastNameJaccard = new SimmetricFunction(new Levenshtein(),
			createPath("0", "last name"), createPath("1", "last name"));
		final EvaluationExpression ageDiff = new NumericDifference(createPath("0", "age"), createPath("1", "age"), 10);
		final ArrayCreation fieldSimExpr = new ArrayCreation(firstNameLev, lastNameJaccard, ageDiff);
		this.similarityFunction = new PathExpression(fieldSimExpr, DefaultFunctions.AVERAGE.asExpression());
		this.inputs.add(createObjectNode("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80));
		this.inputs.add(createObjectNode("id", 1, "first name", "berta", "last name", "typo", "age", 70));
		this.inputs
			.add(createObjectNode("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70));
		this.inputs.add(createObjectNode("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75));
		this.inputs
			.add(createObjectNode("id", 4, "first name", "elma", "last name", "first nameDiffers", "age", 60));
		this.inputs.add(createObjectNode("id", 5, "first name", "albert", "last name", "perfect duplicate", "age", 80));
		this.inputs.add(createObjectNode("id", 6, "first name", "berta", "last name", "tpyo", "age", 70));
		this.inputs
			.add(createObjectNode("id", 7, "first name", "charles", "last name", "age inaccurate", "age", 69));
		this.inputs.add(createObjectNode("id", 8, "first name", "elmar", "last name", "first nameDiffers", "age", 60));
		this.inputs.add(createObjectNode("id", 9, "first name", "frank", "last name", "transitive", "age", 65));
		this.inputs.add(createObjectNode("id", 10, "first name", "frank", "last name", "transitive", "age", 60));
		this.inputs.add(createObjectNode("id", 11, "first name", "frank", "last name", "transitive", "age", 70));
	}

	private IJsonNode arrayOfElement(SopremoTestPlan testPlan, int... ids) {
		Object[] array = new IJsonNode[ids.length];
		EvaluationExpression resultProjection = this.resultProjection;
		if (resultProjection == null)
			resultProjection = EvaluationExpression.VALUE;

		for (int index = 0; index < array.length; index++)
			array[index] = resultProjection
				.evaluate(this.inputs.get(ids[index]), testPlan.getEvaluationContext());

		Arrays.sort(array);
		return createIArrayNode(array);
	}

	private SopremoTestPlan createTestPlan(final LinkageMode mode) {
		IntraSourceRecordLinkage recordLinkage = new IntraSourceRecordLinkage().
			withAlgorithm(new Naive()).
			withDuplicateCondition(this.getDuplicateCondition()).
			withLinkageMode(mode);

		Projection sortedArrays = new Projection().
			withValueTransformation(DefaultFunctions.SORT.asExpression()).
			withInputs(recordLinkage);
		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(sortedArrays);
		if (this.useId)
			recordLinkage.getRecordLinkageInput(0).setIdProjection(new ObjectAccess("id"));
		if (this.resultProjection != null)
			recordLinkage.getRecordLinkageInput(0).setResultProjection(this.resultProjection);

		for (IJsonNode object : this.inputs)
			sopremoTestPlan.getInput(0).add(object);
		return sopremoTestPlan;
	}

	private BooleanExpression getDuplicateCondition() {
		return new ComparativeExpression(this.similarityFunction, BinaryOperator.GREATER_EQUAL,
			new ConstantExpression(0.7));
	}

	/**
	 * Tests {@link LinkageMode#ALL_CLUSTERS_FLAT}
	 */
	@Test
	public void shouldAddSinglesClusters() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.ALL_CLUSTERS_FLAT);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 5)).
			add(this.arrayOfElement(testPlan, 1, 6)).
			add(this.arrayOfElement(testPlan, 2, 7)).
			add(this.arrayOfElement(testPlan, 4, 8)).
			add(this.arrayOfElement(testPlan, 9, 10, 11)).
			add(this.arrayOfElement(testPlan, 3)); // <-- single node

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#ALL_CLUSTERS_PROVENANCE}
	 */
	@Test
	public void shouldAddSinglesClustersWithProvenance() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.ALL_CLUSTERS_PROVENANCE);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 5)).
			add(this.arrayOfElement(testPlan, 1, 6)).
			add(this.arrayOfElement(testPlan, 2, 7)).
			add(this.arrayOfElement(testPlan, 4, 8)).
			add(this.arrayOfElement(testPlan, 9, 10, 11)).
			add(this.arrayOfElement(testPlan, 3)); // <-- single node

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#TRANSITIVE_LINKS}
	 */
	@Test
	public void shouldFindAdditionalLinks() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.TRANSITIVE_LINKS);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 5)).
			add(this.arrayOfElement(testPlan, 1, 6)).
			add(this.arrayOfElement(testPlan, 2, 7)).
			add(this.arrayOfElement(testPlan, 4, 8)).
			add(this.arrayOfElement(testPlan, 9, 10)).
			add(this.arrayOfElement(testPlan, 9, 11)).
			add(this.arrayOfElement(testPlan, 10, 11)); // <-- new

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#DUPLICATE_CLUSTERS_FLAT}
	 */
	@Test
	public void shouldFindClusters() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.DUPLICATE_CLUSTERS_FLAT);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 5)).
			add(this.arrayOfElement(testPlan, 1, 6)).
			add(this.arrayOfElement(testPlan, 2, 7)).
			add(this.arrayOfElement(testPlan, 4, 8)).
			add(this.arrayOfElement(testPlan, 9, 10, 11)); // <-- cluster

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#DUPLICATE_CLUSTERS_PROVENANCE}
	 */
	@Test
	public void shouldFindClustersWithProvenance() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.DUPLICATE_CLUSTERS_PROVENANCE);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 5)).
			add(this.arrayOfElement(testPlan, 1, 6)).
			add(this.arrayOfElement(testPlan, 2, 7)).
			add(this.arrayOfElement(testPlan, 4, 8)).
			add(this.arrayOfElement(testPlan, 9, 10, 11)); // <-- cluster

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#LINKS_ONLY}
	 */
	@Test
	public void shouldFindLinksOnly() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.LINKS_ONLY);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 5)).
			add(this.arrayOfElement(testPlan, 1, 6)).
			add(this.arrayOfElement(testPlan, 2, 7)).
			add(this.arrayOfElement(testPlan, 4, 8)).
			add(this.arrayOfElement(testPlan, 9, 10)).
			add(this.arrayOfElement(testPlan, 9, 11));

		testPlan.run();
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
