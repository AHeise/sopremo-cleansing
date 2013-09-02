package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.paths.PathExpression;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.mapping.learn.ExampleMapping;
import eu.stratosphere.sopremo.cleansing.mapping.learn.TGD;
import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class GeneratedSchemaMappingFromCorrespondencesTest extends SopremoOperatorTestBase<ExampleMapping> {
	@Override
	protected ExampleMapping createDefaultInstance(final int index) {
		return new ExampleMapping();
		//setter, e.g. condition
	}

//	@Test
	public void shouldPerformMappingFromTGDWithFunction() {
		
		GeneratedSchemaMapping mapping = (GeneratedSchemaMapping) new TGD().run(); 
		
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
		addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "A000029").	
		addObject("id", "usCongress3", "name", "John Doe", "worksFor", "A000059");
		sopremoPlan.getExpectedOutput(1).
		addObject("id", "A000029", "name", "CompanyXYZ---").
		addObject("id", "A000059", "name", "CompanyUVW---").
		addObject("id", null, "name", "CompanyABC---");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	
	
//	@Test
	public void shouldPerformMappingFromLargeTree() {
		
		/*
		 * JoinCondition sourceJoinCondition = new JoinCondition(slp1, slp2, true);
		 * sourceJoinCondition.setMandatory(false);
		 * 
		 * JoinCondition targetJoinCondition = new JoinCondition(tlp1, tlp2, true);
		 * targetJoinCondition.setMandatory(true);
		 */
		
		//TODO
	}
	
	@Test
	public void shouldPerformMappingFrom5TGDCorrespondences() {
		
		/*
		 * JoinCondition sourceJoinCondition = new JoinCondition(slp1, slp2, true);
		 * sourceJoinCondition.setMandatory(true);
		 * 
		 * JoinCondition targetJoinCondition = new JoinCondition(tlp1, tlp2, true);
		 * targetJoinCondition.setMandatory(true);
		 */
		
		GeneratedSchemaMapping mapping = (GeneratedSchemaMapping) new TGD().run(); 
		
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
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "CompanyXYZ").	
			addObject("id", "usCongress3", "name", "John Doe", "worksFor", "CompanyUVW");
//			addObject("id", "usCongress2", "name", "John Adams", "worksFor", null);  //not retrieved if source join conditions setMandatory(true)
//			addObject("id", null, "name", null, "worksFor", "CompanyABC");
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyUVW", "name", "CompanyUVW").
			addObject("id", "CompanyABC", "name", "CompanyABC");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	
	
//	@Test
	public void shouldPerformMappingFromSwappedSourceJoinCondition() {
		
		/*if sourceJoinCondition is swapped: Foreign Key bio points to members
		 * List<String> list1 = new ArrayList<String>();
		list1.add("usCongress.usCongressMembers.usCongressMember.biography");
		PathExpression fromPath1 = new PathExpression(list1);
		List<PathExpression> slp1 = new ArrayList<PathExpression>();
		slp1.add(fromPath1);
		
		List<String> list2 = new ArrayList<String>();
		list2.add("usCongress.usCongressBiographies.usCongressBiography.biographyId");
		PathExpression toPath1 = new PathExpression(list2);
		List<PathExpression> slp2 = new ArrayList<PathExpression>();
		slp2.add(toPath1);
		
		JoinCondition sourceJoinCondition = new JoinCondition(slp2, slp1, true); //here swapped
		sourceJoinCondition.setMandatory(true);
		
		JoinCondition targetJoinCondition = new JoinCondition(tlp1, tlp2, true);
		targetJoinCondition.setMandatory(true);
		 */
		
		GeneratedSchemaMapping mapping = (GeneratedSchemaMapping) new TGD().run(); 
		
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
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "CompanyXYZ").
			addObject("id", "usCongress3", "name", "John Doe", "worksFor", "CompanyUVW");
//			addObject("id", "usCongress2", "name", "John Adams", "worksFor", null); //not referenced by a FK
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");
//			addObject("id", "CompanyABC", "name", "CompanyABC"); //no join partner

		sopremoPlan.trace();
		sopremoPlan.run();
	}
	
//	@Test
	public void shouldPerformMapping2() {
		
		/*
		JoinCondition sourceJoinCondition = new JoinCondition(slp1, slp2, true); 
		sourceJoinCondition.setMandatory(true/false);
		
		JoinCondition targetJoinCondition = new JoinCondition(tlp2, tlp1, true); //here swapped
		targetJoinCondition.setMandatory(true);
		 */
		
		GeneratedSchemaMapping mapping = (GeneratedSchemaMapping) new TGD().run(); 
		
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
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "CompanyXYZ").
			addObject("id", "usCongress3", "name", "John Doe", "worksFor", "CompanyUVW");
//			addObject("id", "usCongress2", "name", "John Adams", "worksFor", null); //not referenced by a FK
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyUVW", "name", "CompanyUVW").
			addObject("id", "CompanyABC", "name", "CompanyABC"); //no join partner

		sopremoPlan.trace();
		sopremoPlan.run();
	}
	
//	@Test
	public void shouldPerformMappingWithDuplicate() {
		
		GeneratedSchemaMapping mapping = (GeneratedSchemaMapping) new TGD().run(); 
		
		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping); 
		final EvaluationContext context = sopremoPlan.getEvaluationContext();
		context.getFunctionRegistry().put(CoreFunctions.class);
		sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan.getInput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029").
			addObject("id", "usCongress2", "name", "John Adams", "biography", "A000029"). //here duplicate id
			addObject("id", "usCongress3", "name", "John Doe", "biography", "A000059");
		sopremoPlan.getInput(1).
			addObject("biographyId", "A000029", "worksFor", "CompanyXYZ").
			addObject("biographyId", "A000059", "worksFor", "CompanyUVW");

		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "CompanyXYZ").	
			addObject("id", "usCongress2", "name", "John Adams", "worksFor", "CompanyXYZ").
			addObject("id", "usCongress3", "name", "John Doe", "worksFor", "CompanyUVW");
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "CompanyXYZ", "name", "CompanyXYZ").
			addObject("id", "CompanyUVW", "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	
}
