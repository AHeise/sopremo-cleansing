package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.mapping.MappingTask;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class SpicyMappingTransformationWithKeysTest extends
		SopremoOperatorTestBase<SpicyMappingTransformation> {

	@Override
	protected SpicyMappingTransformation createDefaultInstance(final int index) {
		SpicyMappingTransformation foo = new SpicyMappingTransformation();
		Map<String, Integer> inputIndices = new HashMap<String, Integer>();
		inputIndices.put("key", index);
		foo.setInputIndex(inputIndices);
		return foo;
	}

	private void addDefaultPersonsToPlan(SopremoTestPlan plan) {
		plan.getInput(0)
				.addObject("id", "usCongress1", "name", "Andrew Adams",
						"biography", "A000029", "incomes",
						new ArrayNode<IJsonNode>().add(IntNode.ONE))
				.addObject("id", "usCongress2", "name", "John Adams",
						"biography", "A000039", "incomes",
						new ArrayNode<IJsonNode>().add(IntNode.ONE))
				.addObject("id", "usCongress3", "name", "John Doe",
						"biography", "A000059", "incomes",
						new ArrayNode<IJsonNode>().add(IntNode.ONE));
	}

	private void addDefaultBiographiesToPlan(SopremoTestPlan plan) {
		plan.getInput(1)
				.addObject("biographyId", "A000029", "worksFor", "CompanyXYZ")
				.addObject("biographyId", "A000059", "worksFor", "CompanyUVW")
				.addObject("biographyId", "A000049", "worksFor", "CompanyABC");
	}

	@Test
	public void shouldPerformMappingDefault() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode().put("id", NullNode.getInstance())
						.put("name", NullNode.getInstance())
						.put("worksFor", TextNode.valueOf("CompanyABC"))
						.put("income", NullNode.getInstance()))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyABC", "name", "CompanyABC")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithSwitchedTarget() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateTargetJoinSwitch(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode().put("id", NullNode.getInstance())
						.put("name", NullNode.getInstance())
						.put("worksFor", TextNode.valueOf("CompanyABC"))
						.put("income", NullNode.getInstance()))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyABC", "name", "CompanyABC")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		// sopremoPlan.trace();
		sopremoPlan.run();
	}


	@Test
	public void shouldPerformMappingWithSwitchedTargetAndSource() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateTargetJoinSwitch(true);
		taskFactory.setCreateSourceJoinSwitch(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.addObject("id", "usCongress2", "name", "John Adams", "income", new ArrayNode<IJsonNode>().add(IntNode.ONE), "worksFor", NullNode.getInstance())
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)));
		sopremoPlan.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		// sopremoPlan.trace();
		sopremoPlan.run();
	}
	
	@Test
	public void shouldPerformMappingWithSwitchedSource() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateSourceJoinSwitch(true);
		taskFactory.setTargetJoinMandatory(true);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress2"))
						.put("name", TextNode.valueOf("John Adams"))
						.put("worksFor", NullNode.getInstance())
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)));
		sopremoPlan
				.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", NullNode.getInstance(), "name",
						NullNode.getInstance())
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		// TODO used skolem function here in worksFor

		sopremoPlan.trace();
		sopremoPlan.run();
	}
	
	@Test
	public void shouldPerformMappingWithSwitchedSourceTargetJoinNotMandatory() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setCreateSourceJoinSwitch(true);
		taskFactory.setTargetJoinMandatory(false);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress2"))
						.put("name", TextNode.valueOf("John Adams"))
						.put("worksFor", NullNode.getInstance())
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)));
		sopremoPlan
				.getExpectedOutput(1)
				.addObject("id", "CompanyXYZ", "name", "CompanyXYZ")
				.addObject("id", "CompanyUVW", "name", "CompanyUVW");

		// TODO used skolem function here in worksFor

		sopremoPlan.trace();
		sopremoPlan.run();
	}
	
	/**
	 * Full outer join semantics - ignored because we need input from Paolo for this case...
	 */
	@Test @Ignore
	public void shouldPerformMappingSourceAndTargetJoinNotMandatory() {

		SpicyMappingFactory taskFactory = new SpicyMappingFactory();
		taskFactory.setTargetJoinMandatory(false);
		taskFactory.setSourceJoinMandatory(false);
		SpicyMappingTransformation mapping = generateSopremoPlan(taskFactory
				.create());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan
				.getExpectedOutput(0)
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress1"))
						.put("name", TextNode.valueOf("Andrew Adams"))
						.put("worksFor", TextNode.valueOf("CompanyXYZ"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress2"))
						.put("name", TextNode.valueOf("John Adams"))
						.put("worksFor", NullNode.getInstance())
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)))
				.add(new ObjectNode()
						.put("id", TextNode.valueOf("usCongress3"))
						.put("name", TextNode.valueOf("John Doe"))
						.put("worksFor", TextNode.valueOf("CompanyUVW"))
						.put("income",
								new ArrayNode<IJsonNode>().add(IntNode.ONE)));
		sopremoPlan
				.getExpectedOutput(1)
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
