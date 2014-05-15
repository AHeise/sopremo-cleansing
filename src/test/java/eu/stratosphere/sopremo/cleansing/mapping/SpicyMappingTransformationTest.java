package eu.stratosphere.sopremo.cleansing.mapping;

import org.junit.Test;

import com.google.common.collect.Lists;

import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.*;

public class SpicyMappingTransformationTest extends
		SopremoOperatorTestBase<SpicyMappingTransformation> {

	@Override
	protected SpicyMappingTransformation createDefaultInstance(final int index) {
		SpicyMappingTransformation foo = new SpicyMappingTransformation();
		ObjectCreation sourceSchema = new ObjectCreation();
		sourceSchema.addMapping("key", new ConstantExpression(index));
		foo.setSourceSchema(Lists.newArrayList(sourceSchema));
		return foo;
	}

	private void addDefaultPersonsToPlan(SopremoTestPlan plan) {
		plan.getInput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"biography", "A000029", "incomes", JsonUtil.createArrayNode(1)).
			addObject("id", "usCongress2", "name", "John Adams",
				"biography", "A000039", "incomes", JsonUtil.createArrayNode(1)).
			addObject("id", "usCongress3", "name", "John Doe",
				"biography", "A000059", "incomes", JsonUtil.createArrayNode(1));
	}

	private void addDefaultBiographiesToPlan(SopremoTestPlan plan) {
		plan.getInput(1).
			addObject("biographyId", "A000029", "worksFor", "CompanyXYZ").
			addObject("biographyId", "A000059", "worksFor", "CompanyUVW").
			addObject("biographyId", "A000049", "worksFor", "CompanyABC");
	}

	@Test
	public void shouldPerformMapping() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		SpicyMappingTransformation mapping = taskFactory.create();
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"worksFor", "CompanyXYZ", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "name", null,
				"worksFor", "CompanyABC", "income", null).
			addObject("id", "usCongress3", "name", "John Doe", "worksFor",
				"CompanyUVW", "income", JsonUtil.createArrayNode(1));
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingNested() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateNesting(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "worksFor", "CompanyXYZ",
				"fullName", "nestedName", "Andrew Adams", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "worksFor", "CompanyABC",
				"fullName", "nestedName", null, "income", null).
			addObject("id", "usCongress3", "worksFor", "CompanyUVW",
				"fullName", "nestedName", "John Doe", "income", JsonUtil.createArrayNode(1));

		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithConcat() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateConcat(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1Andrew Adams", "worksFor", "CompanyXYZ",
				"name", "Andrew Adams", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "worksFor", "CompanyABC",
				"name", null, "income", null).
			addObject("id", "usCongress3John Doe", "worksFor", "CompanyUVW",
				"name", "John Doe", "income", JsonUtil.createArrayNode(1));

		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithSubstring() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateSubstring(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "worksFor", "CompanyXYZ",
				"name", "An", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "worksFor", "CompanyABC",
				"name", null, "income", null).
			addObject("id", "usCongress3", "worksFor", "CompanyUVW",
				"name", "Jo", "income", JsonUtil.createArrayNode(1));

		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithJoinConcat() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateJoinWithConcat(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"worksFor", "CompanyXYZ---", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "name", null,
				"worksFor", "CompanyABC---", "income", null).
			addObject("id", "usCongress3", "name", "John Doe",
				"worksFor", "CompanyUVW---", "income", JsonUtil.createArrayNode(1));

		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ---", "name", "CompanyXYZ").
			// don't always use concat
			addObject("id", "CompanyABC---", "name", "CompanyABC").
			addObject("id", "CompanyUVW---", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	public void shouldPerformMappingWithSum() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateSum(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan.getInput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"incomes", JsonUtil.createArrayNode(3000, 3500, 4000), "biography", "A000029").
			addObject("id", "usCongress2", "name", "John Adams",
				"incomes", JsonUtil.createArrayNode(4500, 3500, 4000), "biography", "A000039").
			addObject("id", "usCongress3", "name", "John Doe",
				"incomes", JsonUtil.createArrayNode(3000, 5000, 7000), "biography", "A000059");

		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"worksFor", "CompanyXYZ", "income", 10500).
			addObject("id", null, "name", null,
				"worksFor", "CompanyABC", "income", null).
			addObject("id", "usCongress3", "name", "John Doe",
				"worksFor", "CompanyUVW", "income", 15000);
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}
	

	@Test
	public void shouldPerformMappingWithSwitchedTarget() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateTargetJoinSwitch(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"worksFor", "CompanyXYZ", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "name", null,
				"worksFor", "CompanyABC", "income", null).
			addObject("id", "usCongress3", "name", "John Doe", "worksFor",
				"CompanyUVW", "income", JsonUtil.createArrayNode(1));
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		// sopremoPlan.trace();
		sopremoPlan.run();
	}


	@Test
	public void shouldPerformMappingWithSwitchedTargetAndSource() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateTargetJoinSwitch(true);
		taskFactory.setCreateSourceJoinSwitch(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"worksFor", "CompanyXYZ", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "name", null,
				"worksFor", "CompanyABC", "income", null).
			addObject("id", "usCongress3", "name", "John Doe", "worksFor",
				"CompanyUVW", "income", JsonUtil.createArrayNode(1));
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		// sopremoPlan.trace();
		sopremoPlan.run();
	}
	
	@Test
	public void shouldPerformMappingWithSwitchedSource() {

		SpicyMappingTransformationFactory taskFactory = new SpicyMappingTransformationFactory();
		taskFactory.setCreateSourceJoinSwitch(true);
		SpicyMappingTransformation mapping = taskFactory.create();

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);

		this.addDefaultPersonsToPlan(sopremoPlan);
		this.addDefaultBiographiesToPlan(sopremoPlan);

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams",
				"worksFor", "CompanyXYZ", "income", JsonUtil.createArrayNode(1)).
			addObject("id", null, "name", null,
				"worksFor", "CompanyABC", "income", null).
			addObject("id", "usCongress3", "name", "John Doe", "worksFor",
				"CompanyUVW", "income", JsonUtil.createArrayNode(1));
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyABC", "name", "CompanyABC").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	

}
