package eu.stratosphere.sopremo.cleansing.mapping;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.TestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class SpicyMappingTransformationFromTGDFileTest extends TestBase {

	@Test
	public void shouldPerformExampleMappingGenerated() {
		SpicyMappingTransformation mapping = new SpicyMappingTransformation();
		mapping.setTaskPath(getResource("mapping/usCongress.tgd").toURI().toString());

		// System.out.println(task.toString());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping.getOutput(0));
		sopremoPlan.getOutputOperator(1).setInputs(mapping.getOutput(1));
		sopremoPlan.getInput(0).
			addObject("id_o", "usCongress1", "name_o", "Andrew Adams", "biography_o", "A000029").
			addObject("id_o", "usCongress3", "name_o", "John Doe", "biography_o", "A000059");
		sopremoPlan.getInput(1).
			addObject("biographyId_o", "A000029", "worksFor_o", "CompanyXYZ").
			addObject("biographyId_o", "A000059", "worksFor_o", "CompanyUVW");

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "CompanyXYZ").
			addObject("id", "usCongress3", "name", "John Doe", "worksFor", "CompanyUVW");
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}

	@Test
	@Ignore
	public void shouldPerformExampleMappingGeneratedWithSKFunction() {
		SpicyMappingTransformation mapping = new SpicyMappingTransformation();
		mapping.setTaskPath(getResource("mapping/usCongressMinRenamed.tgd").toURI().toString());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping.getOutput(0));
		sopremoPlan.getOutputOperator(1).setInputs(mapping.getOutput(1));
		sopremoPlan.getInput(0).
			addObject("id_o", "usCongress1", "name_o", "Andrew Adams", "biography_o", "A000029").
			addObject("id_o", "usCongress3", "name_o", "John Doe", "biography_o", "A000059");
		sopremoPlan.getInput(1).
			addObject("biographyId_o", "A000029", "worksFor_o", "CompanyXYZ").
			addObject("biographyId_o", "A000059", "worksFor_o", "CompanyUVW");

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", null).
			addObject("id", "usCongress3", "name", "John Doe", "worksFor", null);
		// TODO define skolem function for id and worksFor, for now is just null
		sopremoPlan.getExpectedOutput(1).
			addObject("id", null, "name", "CompanyXYZ").
			addObject("id", null, "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}
}
