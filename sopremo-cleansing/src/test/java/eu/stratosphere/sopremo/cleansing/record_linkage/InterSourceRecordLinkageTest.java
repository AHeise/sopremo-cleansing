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
import eu.stratosphere.sopremo.expressions.ArrayAccess;
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
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Tests {@link InterSourceRecordLinkage}.
 * 
 * @author Arvid Heise
 */
@RunWith(Parameterized.class)
public class InterSourceRecordLinkageTest {
	private PathExpression similarityFunction;

	private boolean useId;

	/**
	 * Initializes InterSourceRecordLinkageTest.
	 * 
	 * @param resultProjections
	 * @param useId
	 */
	public InterSourceRecordLinkageTest(EvaluationExpression[] resultProjections, boolean useId) {
		this.useId = useId;
		this.resultProjections = resultProjections;

		final SimmetricFunction firstNameLev = new SimmetricFunction(new Levenshtein(),
			createPath("0", "first name"), createPath("1", "firstName"));
		final SimmetricFunction lastNameJaccard = new SimmetricFunction(new Levenshtein(),
			createPath("0", "last name"), createPath("1", "lastName"));
		final EvaluationExpression ageDiff = new NumericDifference(createPath("0", "age"), createPath("1", "age"), 10);
		final ArrayCreation fieldSimExpr = new ArrayCreation(firstNameLev, lastNameJaccard, ageDiff);
		this.similarityFunction = new PathExpression(fieldSimExpr, DefaultFunctions.AVERAGE.asExpression());

		List<IJsonNode> inputs1 = new ArrayList<IJsonNode>();
		inputs1.add(createObjectNode("id", 0, "first name", "albert", "last name", "perfect duplicate", "age", 80));
		inputs1.add(createObjectNode("id", 1, "first name", "berta", "last name", "typo", "age", 70));
		inputs1.add(createObjectNode("id", 2, "first name", "charles", "last name", "age inaccurate", "age", 70));
		inputs1.add(createObjectNode("id", 3, "first name", "dagmar", "last name", "unmatched", "age", 75));
		inputs1.add(createObjectNode("id", 4, "first name", "elma", "last name", "firstNameDiffers", "age", 60));
		inputs1.add(createObjectNode("id", 5, "first name", "frank", "last name", "transitive", "age", 65));

		List<IJsonNode> inputs2 = new ArrayList<IJsonNode>();
		inputs2.add(createObjectNode("id2", 10, "firstName", "albert", "lastName", "perfect duplicate", "age", 80));
		inputs2.add(createObjectNode("id2", 11, "firstName", "berta", "lastName", "tpyo", "age", 70));
		inputs2.add(createObjectNode("id2", 12, "firstName", "charles", "lastName", "age inaccurate", "age", 69));
		inputs2.add(createObjectNode("id2", 14, "firstName", "elmar", "lastName", "firstNameDiffers", "age", 60));
		inputs2.add(createObjectNode("id2", 151, "firstName", "frank", "lastName", "transitive", "age", 60));
		inputs2.add(createObjectNode("id2", 152, "firstName", "frank", "lastName", "transitive", "age", 70));

		this.inputs.add(inputs1);
		this.inputs.add(inputs2);

		this.idProjections = new EvaluationExpression[] {
			new ObjectAccess("id"),
			new ObjectAccess("id2") };
	}

	private List<List<IJsonNode>> inputs = new ArrayList<List<IJsonNode>>();

	private EvaluationExpression[] resultProjections, idProjections;

	/**
	 * Tests {@link LinkageMode#LINKS_ONLY}
	 */
	@Test
	public void shouldFindLinksOnly() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.LINKS_ONLY);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 10)).
			add(this.arrayOfElement(testPlan, 1, 11)).
			add(this.arrayOfElement(testPlan, 2, 12)).
			add(this.arrayOfElement(testPlan, 4, 14)).
			add(this.arrayOfElement(testPlan, 5, 151)).
			add(this.arrayOfElement(testPlan, 5, 152));

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#TRANSITIVE_LINKS}
	 */
	@Test
	public void shouldFindAdditionalLinks() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.TRANSITIVE_LINKS);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 10)).
			add(this.arrayOfElement(testPlan, 1, 11)).
			add(this.arrayOfElement(testPlan, 2, 12)).
			add(this.arrayOfElement(testPlan, 4, 14)).
			add(this.arrayOfElement(testPlan, 5, 151)).
			add(this.arrayOfElement(testPlan, 5, 152)).
			add(this.flatArrayOfElements(testPlan, new int[0], new int[] { 151, 152 })); // <-- new

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#DUPLICATE_CLUSTERS_FLAT}
	 */
	@Test
	public void shouldFindClusters() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.DUPLICATE_CLUSTERS_FLAT);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 10)).
			add(this.arrayOfElement(testPlan, 1, 11)).
			add(this.arrayOfElement(testPlan, 2, 12)).
			add(this.arrayOfElement(testPlan, 4, 14)).
			add(this.flatArrayOfElements(testPlan, new int[] { 5 }, new int[] { 151, 152 })); // <-- cluster

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#DUPLICATE_CLUSTERS_PROVENANCE}
	 */
	@Test
	public void shouldFindClustersWithProvenance() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.DUPLICATE_CLUSTERS_PROVENANCE);

		testPlan.getExpectedOutput(0).
			add(this.deepArrayOfElements(testPlan, new int[] { 0 }, new int[] { 10 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 1 }, new int[] { 11 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 2 }, new int[] { 12 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 4 }, new int[] { 14 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 5 }, new int[] { 151, 152 })); // <-- cluster

		testPlan.run();
	}

	/**
	 * Tests {@link LinkageMode#ALL_CLUSTERS_FLAT}
	 */
	@Test
	public void shouldAddSinglesClusters() {
		final SopremoTestPlan testPlan = this.createTestPlan(LinkageMode.ALL_CLUSTERS_FLAT);

		testPlan.getExpectedOutput(0).
			add(this.arrayOfElement(testPlan, 0, 10)).
			add(this.arrayOfElement(testPlan, 1, 11)).
			add(this.arrayOfElement(testPlan, 2, 12)).
			add(this.arrayOfElement(testPlan, 4, 14)).
			add(this.flatArrayOfElements(testPlan, new int[] { 5 }, new int[] { 151, 152 })).
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
			add(this.deepArrayOfElements(testPlan, new int[] { 0 }, new int[] { 10 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 1 }, new int[] { 11 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 2 }, new int[] { 12 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 4 }, new int[] { 14 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 5 }, new int[] { 151, 152 })).
			add(this.deepArrayOfElements(testPlan, new int[] { 3 }, new int[] {}));

		testPlan.run();
	}

	private IJsonNode arrayOfElement(SopremoTestPlan testPlan, int... ids) {
		Object[] array = new IJsonNode[ids.length];

		for (int index = 0; index < array.length; index++) {
			EvaluationExpression resultProjection = this.resultProjections[index];
			if (resultProjection == null)
				resultProjection = EvaluationExpression.VALUE;
			array[index] = resultProjection.evaluate(this.findTuple(testPlan, index, ids[index]),
				testPlan.getEvaluationContext());
		}

		Arrays.sort(array);
		return createIArrayNode(array);
	}

	private IJsonNode deepArrayOfElements(SopremoTestPlan testPlan, int[]... ids) {
		Object[][] array = new IJsonNode[ids.length][];

		for (int sourceIndex = 0; sourceIndex < array.length; sourceIndex++) {
			array[sourceIndex] = new IJsonNode[ids[sourceIndex].length];

			EvaluationExpression resultProjection = this.resultProjections[sourceIndex];
			if (resultProjection == null)
				resultProjection = EvaluationExpression.VALUE;
			for (int tupleIndex = 0; tupleIndex < array[sourceIndex].length; tupleIndex++)
				array[sourceIndex][tupleIndex] = resultProjection.evaluate(
					this.findTuple(testPlan, sourceIndex, ids[sourceIndex][tupleIndex]),
					testPlan.getEvaluationContext());
			Arrays.sort(array[sourceIndex]);
		}
		return createIArrayNode((Object[]) array);
	}

	private IJsonNode flatArrayOfElements(SopremoTestPlan testPlan, int[]... ids) {
		ArrayList<IJsonNode> array = new ArrayList<IJsonNode>();

		for (int sourceIndex = 0; sourceIndex < ids.length; sourceIndex++) {
			EvaluationExpression resultProjection = this.resultProjections[sourceIndex];
			if (resultProjection == null)
				resultProjection = EvaluationExpression.VALUE;
			for (int tupleIndex = 0; tupleIndex < ids[sourceIndex].length; tupleIndex++)
				array.add(resultProjection.evaluate(
					this.findTuple(testPlan, sourceIndex, ids[sourceIndex][tupleIndex]),
					testPlan.getEvaluationContext()));
		}

		final IJsonNode[] nodes = array.toArray(new IJsonNode[array.size()]);
		Arrays.sort(nodes);
		return new IArrayNode(nodes);
	}

	private IJsonNode findTuple(SopremoTestPlan testPlan, int sourceIndex, int id) {
		for (IJsonNode object : this.inputs.get(sourceIndex))
			if (((IntNode) this.idProjections[sourceIndex].evaluate(object, testPlan.getEvaluationContext()))
				.getIntValue() == id)
				return object;
		throw new IllegalStateException();
	}

	private SopremoTestPlan createTestPlan(final LinkageMode mode) {
		InterSourceRecordLinkage recordLinkage = new InterSourceRecordLinkage().
			withAlgorithm(new Naive()).
			withDuplicateCondition(getDuplicateCondition()).
			withLinkageMode(mode);

		Projection sortedArrays;
		if (!mode.getClosureMode().isProvenance())
			sortedArrays = new Projection().
				withValueTransformation(DefaultFunctions.SORT.asExpression()).
				withInputs(recordLinkage);
		else {
			EvaluationExpression[] sorts = new EvaluationExpression[this.inputs.size()];
			for (int index = 0; index < sorts.length; index++)
				sorts[index] = new PathExpression(new ArrayAccess(index), DefaultFunctions.SORT.asExpression());
			sortedArrays = new Projection().
				withValueTransformation(new ArrayCreation(sorts)).
				withInputs(recordLinkage);
		}

		final SopremoTestPlan sopremoTestPlan = new SopremoTestPlan(sortedArrays);
		if (this.useId) {
			recordLinkage.getRecordLinkageInput(0).setIdProjection(new ObjectAccess("id"));
			recordLinkage.getRecordLinkageInput(1).setIdProjection(new ObjectAccess("id2"));
		}
		for (int index = 0; index < this.resultProjections.length; index++)
			if (this.resultProjections[index] != null)
				recordLinkage.getRecordLinkageInput(index).setResultProjection(this.resultProjections[index]);

		for (int index = 0; index < this.inputs.size(); index++)
			for (IJsonNode object : this.inputs.get(index))
				sopremoTestPlan.getInput(index).add(object);
		return sopremoTestPlan;
	}

	private BooleanExpression getDuplicateCondition() {
		return new ComparativeExpression(this.similarityFunction, BinaryOperator.GREATER_EQUAL,
			new ConstantExpression(0.7));
	}

	/**
	 * Returns a duplicate projection expression that aggregates some fields to arrays.
	 * 
	 * @return an aggregating expression
	 */
	protected static EvaluationExpression getAggregativeProjection1() {
		final ObjectCreation aggregating = new ObjectCreation();
		aggregating.addMapping("name", new ObjectAccess("first name"));
		aggregating.addMapping("id", new ObjectAccess("id"));

		return aggregating;
	}

	/**
	 * Returns a duplicate projection expression that aggregates some fields to arrays.
	 * 
	 * @return an aggregating expression
	 */
	protected static EvaluationExpression getAggregativeProjection2() {
		final ObjectCreation aggregating = new ObjectCreation();
		aggregating.addMapping("name", new ObjectAccess("firstName"));
		aggregating.addMapping("id", new ObjectAccess("id2"));

		return aggregating;
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[][] projections = { new EvaluationExpression[2],
			new EvaluationExpression[] { getAggregativeProjection1(), getAggregativeProjection2() } };
		final boolean[] useIds = { true, false };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression[] projection : projections)
			for (final boolean useId : useIds)
				parameters.add(new Object[] { projection, useId });

		return parameters;
	}
}
