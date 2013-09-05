package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.mapping.MappingTask;

import java.util.HashMap;

import org.junit.Test;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class SpicyMappingTransformationTest extends
		SopremoOperatorTestBase<SpicyMappingTransformation> {

	@Override
	protected SpicyMappingTransformation createDefaultInstance(final int index) {
		return new SpicyMappingTransformation();
		// setter, e.g. condition
	}

	@Test
	public void shouldPerformMapping() {

		MappingTask taskFactory = new SpicyMappingFactory().create();
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory);

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan
				.getInput(0)
				.addObject("id", "usCongress1", "name", "Andrew Adams",
						"biography", "A000029")
				.addObject("id", "usCongress2", "name", "John Adams",
						"biography", "A000039")
				.addObject("id", "usCongress3", "name", "John Doe",
						"biography", "A000059");
		sopremoPlan.getInput(1)
				.addObject("biographyId", "A000029", "worksFor", "CompanyXYZ")
				.addObject("biographyId", "A000059", "worksFor", "CompanyUVW")
				.addObject("biographyId", "A000049", "worksFor", "CompanyABC");

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ")))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW")));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyABC", "name", "CompanyABC")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingNested() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateNesting(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan
				.getInput(0)
				.addObject("id", "usCongress1", "name", "Andrew Adams",
						"biography", "A000029")
				.addObject("id", "usCongress2", "name", "John Adams",
						"biography", "A000039")
				.addObject("id", "usCongress3", "name", "John Doe",
						"biography", "A000059");
		sopremoPlan.getInput(1)
				.addObject("biographyId", "A000029", "worksFor", "CompanyXYZ")
				.addObject("biographyId", "A000059", "worksFor", "CompanyUVW")
				.addObject("biographyId", "A000049", "worksFor", "CompanyABC");

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("fullName",
								new ObjectNode().put("nestedName",
										TextNode.valueOf("Andrew Adams")))
						.put("worksFor", TextNode.valueOf("CompanyXYZ")))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("fullName",
								new ObjectNode().put("nestedName",
										TextNode.valueOf("John Doe")))
						.put("worksFor", TextNode.valueOf("CompanyUVW")));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyABC", "name", "CompanyABC")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithConcat() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateConcat(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		final EvaluationContext context = sopremoPlan.getEvaluationContext();
		context.getFunctionRegistry().put(CoreFunctions.class);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan
				.getInput(0)
				.addObject("id", "usCongress1", "name", "Andrew Adams",
						"biography", "A000029")
				.addObject("id", "usCongress2", "name", "John Adams",
						"biography", "A000039")
				.addObject("id", "usCongress3", "name", "John Doe",
						"biography", "A000059");
		sopremoPlan.getInput(1)
				.addObject("biographyId", "A000029", "worksFor", "CompanyXYZ")
				.addObject("biographyId", "A000059", "worksFor", "CompanyUVW")
				.addObject("biographyId", "A000049", "worksFor", "CompanyABC");

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id",
								TextNode.valueOf("usCongress1---Andrew Adams"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ")))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3---John Doe"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW")));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyABC", "name", "CompanyABC")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		// sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithSubstring() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateSubstring(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		final EvaluationContext context = sopremoPlan.getEvaluationContext();
		context.getFunctionRegistry().put(CoreFunctions.class);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan
				.getInput(0)
				.addObject("id", "usCongress1", "name", "Andrew Adams",
						"biography", "A000029")
				.addObject("id", "usCongress2", "name", "John Adams",
						"biography", "A000039")
				.addObject("id", "usCongress3", "name", "John Doe",
						"biography", "A000059");
		sopremoPlan.getInput(1)
				.addObject("biographyId", "A000029", "worksFor", "CompanyXYZ")
				.addObject("biographyId", "A000059", "worksFor", "CompanyUVW")
				.addObject("biographyId", "A000049", "worksFor", "CompanyABC");

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("An"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ")))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("Jo"))
						.put("worksFor", TextNode.valueOf("CompanyUVW")));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyABC", "name", "CompanyABC")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithJoinConcat() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateJoinWithConcat(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		final EvaluationContext context = sopremoPlan.getEvaluationContext();
		context.getFunctionRegistry().put(CoreFunctions.class);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan
				.getInput(0)
				.addObject("id", "usCongress1", "name", "Andrew Adams",
						"biography", "A000029")
				.addObject("id", "usCongress2", "name", "John Adams",
						"biography", "A000039")
				.addObject("id", "usCongress3", "name", "John Doe",
						"biography", "A000059");
		sopremoPlan.getInput(1)
				.addObject("biographyId", "A000029", "worksFor", "CompanyXYZ")
				.addObject("biographyId", "A000059", "worksFor", "CompanyUVW")
				.addObject("biographyId", "A000049", "worksFor", "CompanyABC");

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ---")))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW---")));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ---", "name", "CompanyXYZ")
				. // don't always use concat
				addObject("id", "CompanyABC---", "name", "CompanyABC")
				.addObject("id", "CompanyUVW---", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithSum() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateSum(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		final EvaluationContext context = sopremoPlan.getEvaluationContext();
		context.getFunctionRegistry().put(CoreFunctions.class);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan
				.getInput(0)
				.addObject(
						"id",
						"usCongress1",
						"name",
						"Andrew Adams",
						"incomes",
						new ArrayNode<IJsonNode>().add(IntNode.valueOf(3000))
								.add(IntNode.valueOf(3500))
								.add(IntNode.valueOf(4000)), "biography",
						"A000029")
				.addObject(
						"id",
						"usCongress2",
						"name",
						"John Adams",
						"incomes",
						new ArrayNode<IJsonNode>().add(IntNode.valueOf(4500))
								.add(IntNode.valueOf(3500))
								.add(IntNode.valueOf(4000)), "biography",
						"A000039")
				.addObject(
						"id",
						"usCongress3",
						"name",
						"John Doe",
						"incomes",
						new ArrayNode<IJsonNode>().add(IntNode.valueOf(3000))
								.add(IntNode.valueOf(5000))
								.add(IntNode.valueOf(7000)), "biography",
						"A000059");
		sopremoPlan.getInput(1)
				.addObject("biographyId", "A000029", "worksFor", "CompanyXYZ")
				.addObject("biographyId", "A000059", "worksFor", "CompanyUVW")
				.addObject("biographyId", "A000049", "worksFor", "CompanyABC");

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ"))
						.put("income", IntNode.valueOf(10500)))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW"))
						.put("income", IntNode.valueOf(15000)));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyABC", "name", "CompanyABC")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	private SpicyMappingTransformation generateSopremoPlan(MappingTask task) {
		SpicyMappingTransformation operator = new SpicyMappingTransformation();
		operator.setMappingTask(task);

		HashMap<String, Integer> inputIndex = new HashMap<String, Integer>(2);
		inputIndex.put("usCongressMembers", 0);
		inputIndex.put("usCongressBiographies", 1);
		operator.setInputIndex(inputIndex);

		HashMap<String, Integer> outputIndex = new HashMap<String, Integer>(2);
		outputIndex.put("persons", 0);
		outputIndex.put("legalEntities", 1);
		operator.setOutputIndex(outputIndex);

		return operator;
	}
}
