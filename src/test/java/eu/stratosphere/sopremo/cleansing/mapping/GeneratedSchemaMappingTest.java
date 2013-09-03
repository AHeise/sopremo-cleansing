package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.mapping.MappingTask;

import java.util.HashMap;

import org.junit.Test;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class GeneratedSchemaMappingTest extends SopremoOperatorTestBase<GeneratedSchemaMapping> {
	@Override
	protected GeneratedSchemaMapping createDefaultInstance(final int index) {
		return new GeneratedSchemaMapping();
		//setter, e.g. condition
	}

	@Test
	public void shouldPerformMappingNested() {
		
		MappingTask taskFactory = new MappingTaskFactory().create();
		GeneratedSchemaMapping mapping = generateSopremoPlan(taskFactory); 
		
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
							put("worksFor", TextNode.valueOf("CompanyXYZ"))
			).	
		add(new ObjectNode().put("id", TextNode.valueOf("usCongress3")).
							put("fullName", new ObjectNode().put("nestedName", TextNode.valueOf("John Doe"))).
							put("worksFor", TextNode.valueOf("CompanyUVW"))
			);
		sopremoPlan.getExpectedOutput(1).
		addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
		addObject("id", "CompanyABC", "name", "CompanyABC").
		addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	
	
	@Test
	public void shouldPerformMappingWithConcat() {
		
		MappingTaskFactory taskFactory = new MappingTaskFactory();
		taskFactory.setCreateConcat(true);
		GeneratedSchemaMapping mapping = generateSopremoPlan(taskFactory.create()); 
		
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping); 
		final EvaluationContext context = sopremoPlan.getEvaluationContext();
		context.getFunctionRegistry().put(CoreFunctions.class);
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
							put("worksFor", TextNode.valueOf("CompanyXYZ---"))
			).	
		add(new ObjectNode().put("id", TextNode.valueOf("usCongress3")).
							put("fullName", new ObjectNode().put("nestedName", TextNode.valueOf("John Doe"))).
							put("worksFor", TextNode.valueOf("CompanyUVW---"))
			);
		sopremoPlan.getExpectedOutput(1).
		addObject("id", "CompanyXYZ---", "name", "CompanyXYZ---").
		addObject("id", "CompanyABC---", "name", "CompanyABC---").
		addObject("id", "CompanyUVW---", "name", "CompanyUVW---");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	
	
	private GeneratedSchemaMapping generateSopremoPlan(MappingTask task) {
		GeneratedSchemaMapping plan = new GeneratedSchemaMapping();
		plan.setMappingTask(task);
		
		HashMap<String, Integer> inputIndex = new HashMap<String, Integer>(2);
		inputIndex.put("usCongressMembers", 0);
		inputIndex.put("usCongressBiographies", 1);
		plan.setInputIndex(inputIndex);
		
		HashMap<String, Integer> outputIndex = new HashMap<String, Integer>(2);
		outputIndex.put("persons", 0);
		outputIndex.put("legalEntities", 1);
		plan.setOutputIndex(outputIndex);
		
		return plan;
	}
}
