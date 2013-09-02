package eu.stratosphere.sopremo.cleansing.mapping;

import org.junit.Test;

import eu.stratosphere.sopremo.cleansing.mapping.learn.ExampleMapping;
import eu.stratosphere.sopremo.cleansing.mapping.learn.TGDNested;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class GeneratedSchemaMappingNestedTest extends SopremoOperatorTestBase<ExampleMapping> {
	@Override
	protected ExampleMapping createDefaultInstance(final int index) {
		return new ExampleMapping();
		//setter, e.g. condition
	}

	@Test
	public void shouldPerformMappingFromTGDCorrespondences() {
		
		GeneratedSchemaMapping mapping = (GeneratedSchemaMapping) new TGDNested().run(); 
		
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping); 
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan.getInput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029").
			addObject("id", "usCongress2", "name", "John Adams", "biography", "A000039").
			addObject("id", "usCongress3", "name", "John Doe", "biography", "A000059");
		sopremoPlan.getInput(1).
			addObject("biographyId", "A000029", "worksFor", "CompanyXYZ").
			addObject("biographyId", "A000059", "worksFor", "CompanyUVW").
			addObject("biographyId", "A000049", "worksFor", "CompanyABC");

		sopremoPlan.getExpectedOutput(0).
		add(new ObjectNode().put("id", TextNode.valueOf("usCongress1")).
							put("fullName", new ObjectNode().put("nestedName", TextNode.valueOf("Andrew Adams"))).
							put("worksFor", TextNode.valueOf("A000029"))
			).	
		add(new ObjectNode().put("id", TextNode.valueOf("usCongress3")).
							put("fullName", new ObjectNode().put("nestedName", TextNode.valueOf("John Doe"))).
							put("worksFor", TextNode.valueOf("A000059"))
			);
		sopremoPlan.getExpectedOutput(1).
		addObject("id", "A000029", "name", "CompanyXYZ").
		addObject("id", "A000059", "name", "CompanyUVW").
		addObject("id", null, "name", "CompanyABC");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	
}
