package eu.stratosphere.sopremo.cleansing.mapping;

import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.operator.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.ObjectNodeTest;
import eu.stratosphere.sopremo.type.TextNode;

public class ExampleMappingTest extends SopremoOperatorTestBase<ExampleMapping> {
	@Override
	protected ExampleMapping createDefaultInstance(final int index) {
		return new ExampleMapping();
		//setter, e.g. condition
	}

	@Test
	public void shouldPerformSchemaMapping() {
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(2, 2); //TODO

		ExampleMapping mapping = new ExampleMapping(); //setter
		mapping.setInputs(sopremoPlan.getInputOperators(0, 2));
		
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan.getInput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029").
			addObject("id", "usCongress2", "name", "John Adams", "biography", "A000039");
		sopremoPlan.getInput(1).
			addObject("biographyId", "A000029", "worksFor", "CompanyXYZ").
			addObject("biographyId", "A000049", "worksFor", "CompanyABC");
		
		//expected output for join
//		sopremoPlan.getExpectedOutput(0).
//			addObject("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029", "biographyId", "A000029", "worksFor", "CompanyXYZ").
//			addObject("id", "usCongress2", "name", "John Adams", "biography", "A000039", "biographyId", "A000039", "worksFor", "CompanyABC");	
//		add(new ObjectNode().put("uscongressMember", new ObjectNode().
//										put("id", TextNode.valueOf("usCongress1")).
//										put("name", TextNode.valueOf("Andrew Adams")).
//										put("biography", TextNode.valueOf("A000029")) 
//								).put("uscongressBiography", new ObjectNode().
//										put("biographyId", TextNode.valueOf("A000029")).
//										put("worksFor", TextNode.valueOf("CompanyXYZ"))
//								));
						
		
		//expected output for antijoin = remainingFromBio
//		sopremoPlan.getExpectedOutput(0).
//			addObject("id", null, "name", "CompanyABC"); 
		
		//expected output for tgdWithFK
//		sopremoPlan.getExpectedOutput(0).
//		add(new ObjectNode().put("person", new ObjectNode().
//									put("id", TextNode.valueOf("usCongress1")).
//									put("name", TextNode.valueOf("Andrew Adams")).
//									put("worksFor", TextNode.valueOf("A000029")) //TODO skolem f.
//							).put("legalEntity", new ObjectNode().	
//									put("id", TextNode.valueOf("A000029")).
//									put("name", TextNode.valueOf("CompanyXYZ"))									
//							));
		
		//expected output for final LE only
//		sopremoPlan.getExpectedOutput(0).
//			addObject("id", "A000029", "name", "CompanyXYZ").
//			addObject("id", null, "name", "CompanyABC");
		
		//expected final output
		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "A000029"); //TODO skolem f.				
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "A000029", "name", "CompanyXYZ").
			addObject("id", null, "name", "CompanyABC");

		sopremoPlan.trace();
		sopremoPlan.run();
	}


}
