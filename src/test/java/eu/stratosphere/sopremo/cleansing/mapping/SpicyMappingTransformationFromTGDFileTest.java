package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTaskTgds;

import java.util.HashMap;

import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class SpicyMappingTransformationFromTGDFileTest extends SopremoOperatorTestBase<SpicyMappingTransformation> {
	
	@Override
	protected SpicyMappingTransformation createDefaultInstance(final int index) {
		return new SpicyMappingTransformation();
	}
	
	@Test
	public void shouldPerformExampleMappingGenerated() {

		MappingTask task = null;
		String input = "src/test/resources/mapping/usCongress.tgd"; 
		HashMap<String, Integer> inputIndex = new HashMap<String, Integer>(2);
		inputIndex.put("uscongressMembers", 0);
		inputIndex.put("uscongressBiographies", 1);
		
		HashMap<String, Integer> outputIndex = new HashMap<String, Integer>(2);
		outputIndex.put("persons", 0);
		outputIndex.put("legalEntities", 1);
		
		try {
			//read file, create task
			task = new DAOMappingTaskTgds().loadMappingTask(input);
		} catch (DAOException e) {
			e.printStackTrace();
		}
		
		SpicyMappingTransformation mapping = new SpicyMappingTransformation(); 
		mapping.setMappingTask(task);
		mapping.setInputIndex(inputIndex);
		mapping.setOutputIndex(outputIndex);
		
//		System.out.println(task.toString());
		
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
	
	@Ignore
	@Test
	public void shouldPerformExampleMappingGeneratedWithSKFunction() {

		MappingTask task = null;
		String input = "src/test/resources/mapping/usCongressMinRenamed.tgd"; 
		HashMap<String, Integer> inputIndex = new HashMap<String, Integer>(2);
		inputIndex.put("uscongressMembers", 0);
		inputIndex.put("uscongressBiographies", 1);
		
		HashMap<String, Integer> outputIndex = new HashMap<String, Integer>(2);
		outputIndex.put("persons", 0);
		outputIndex.put("legalEntities", 1);
		
		try {
			//read file, create task
			task = new DAOMappingTaskTgds().loadMappingTask(input);
		} catch (DAOException e) {
			e.printStackTrace();
		}
		
		SpicyMappingTransformation mapping = new SpicyMappingTransformation(); 
		mapping.setMappingTask(task);
		mapping.setInputIndex(inputIndex);
		mapping.setOutputIndex(outputIndex);
		
//		System.out.println(task.toString());
		
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
		sopremoPlan.getExpectedOutput(1).
		addObject("id", null, "name", "CompanyXYZ"). //TODO define skolem function for id and worksFor, for now is just null
		addObject("id", null, "name", "CompanyUVW");

		sopremoPlan.trace();
		sopremoPlan.run();
	}	
}
