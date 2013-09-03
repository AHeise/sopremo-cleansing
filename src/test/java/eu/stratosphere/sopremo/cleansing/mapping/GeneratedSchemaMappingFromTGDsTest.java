package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTaskTgds;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Test;

import eu.stratosphere.sopremo.testing.SopremoOperatorTestBase;
import eu.stratosphere.sopremo.testing.SopremoTestPlan;

public class GeneratedSchemaMappingFromTGDsTest extends SopremoOperatorTestBase<GeneratedSchemaMapping> {
	@Override
	protected GeneratedSchemaMapping createDefaultInstance(final int index) {
		return new GeneratedSchemaMapping();
		// setter, e.g. condition
	}

	@Test
	public void shouldPerformExampleMappingGenerated() throws IOException {

		MappingTask task = null;
		String input =
			GeneratedSchemaMappingFromTGDsTest.class.getClassLoader().getResources("mapping/usCongressMinRenamed.tgd").nextElement().toString();
		HashMap<String, Integer> inputIndex = new HashMap<String, Integer>(2);
		inputIndex.put("uscongressMembers", 0);
		inputIndex.put("uscongressBiographies", 1);

		HashMap<String, Integer> outputIndex = new HashMap<String, Integer>(2);
		outputIndex.put("persons", 0);
		outputIndex.put("legalEntities", 1);

		try {
			// read file, create task
			task = new DAOMappingTaskTgds().loadMappingTask(input);
		} catch (DAOException e) {
			e.printStackTrace();
		}

		GeneratedSchemaMapping mapping = new GeneratedSchemaMapping();
		mapping.setMappingTask(task);
		mapping.setInputIndex(inputIndex);
		mapping.setOutputIndex(outputIndex);

		System.out.println(task.toString());

		final SopremoTestPlan sopremoPlan = new SopremoTestPlan(mapping);
		sopremoPlan.getOutputOperator(0).setInputs(mapping.getOutput(0));
		sopremoPlan.getOutputOperator(1).setInputs(mapping.getOutput(1));
		// sopremoPlan.getOutputOperator(0).setInputs(mapping);
		sopremoPlan.getInput(0).
			addObject("id_o", "usCongress1", "name_o", "Andrew Adams", "biography_o", "A000029").
			addObject("id_o", "usCongress2", "name_o", "John Adams", "biography_o", "A000039").
			addObject("id_o", "usCongress3", "name_o", "John Doe", "biography_o", "A000059");
		sopremoPlan.getInput(1).
			addObject("biographyId_o", "A000029", "worksFor_o", "CompanyXYZ").
			addObject("biographyId_o", "A000059", "worksFor_o", "CompanyUVW").
			addObject("biographyId_o", "A000049", "worksFor_o", "CompanyABC");

		// expected output for join
		// sopremoPlan.getExpectedOutput(0).
		// add(new ObjectNode().put("v0", new ObjectNode().
		// put("id_o", TextNode.valueOf("usCongress1")).
		// put("name_o", TextNode.valueOf("Andrew Adams")).
		// put("biography_o", TextNode.valueOf("A000029"))
		// ).put("v1", new ObjectNode().
		// put("biographyId_o", TextNode.valueOf("A000029")).
		// put("worksFor_o", TextNode.valueOf("CompanyXYZ"))
		// )).
		// add(new ObjectNode().put("v0", new ObjectNode().
		// put("id_o", TextNode.valueOf("usCongress3")).
		// put("name_o", TextNode.valueOf("John Doe")).
		// put("biography_o", TextNode.valueOf("A000059"))
		// ).put("v1", new ObjectNode().
		// put("biographyId_o", TextNode.valueOf("A000059")).
		// put("worksFor_o", TextNode.valueOf("CompanyUVW"))
		// ));

		// expected output for antijoin = remainingFromBio
		// sopremoPlan.getExpectedOutput(0).
		// addObject("v1",
		// new ObjectNode().put("biographyId_o", TextNode.valueOf("A000049")).put("worksFor_o",
		// TextNode.valueOf("CompanyABC"))
		// );

		// expected output for transform_1 after join
		// sopremoPlan.getExpectedOutput(0).
		// add(new ObjectNode().put("v3", new ObjectNode().
		// put("id", TextNode.valueOf("usCongress1")).
		// put("name", TextNode.valueOf("Andrew Adams")).
		// put("worksFor", TextNode.valueOf("A000029")) //TODO skolem f.
		// ).put("v2", new ObjectNode().
		// put("id", TextNode.valueOf("A000029")).
		// put("name", TextNode.valueOf("CompanyXYZ"))
		// )).
		// add(new ObjectNode().put("v3", new ObjectNode().
		// put("id", TextNode.valueOf("usCongress3")).
		// put("name", TextNode.valueOf("John Doe")).
		// put("worksFor", TextNode.valueOf("A000059")) //TODO skolem f.
		// ).put("v2", new ObjectNode().
		// put("id", TextNode.valueOf("A000059")).
		// put("name", TextNode.valueOf("CompanyUVW"))
		// ));

		// expected output for transform_0 after join and difference
		// sopremoPlan.getExpectedOutput(0).
		// add(new ObjectNode().put("v2", new ObjectNode().
		// put("id", NullNode.getInstance()).
		// put("name", TextNode.valueOf("CompanyABC"))
		// ));

		// expected final output for union
		// sopremoPlan.getExpectedOutput(0).
		// add(new ObjectNode().put("persons", new ObjectNode().
		// put("id", TextNode.valueOf("usCongress1")).
		// put("name", TextNode.valueOf("Andrew Adams")).
		// put("worksFor", TextNode.valueOf("A000029")) //TODO skolem f.
		// ).put("legalEntities", new ObjectNode().
		// put("id", TextNode.valueOf("A000029")).
		// put("name", TextNode.valueOf("CompanyXYZ"))
		// )).
		// add(new ObjectNode().put("persons", new ObjectNode().
		// put("id", TextNode.valueOf("usCongress3")).
		// put("name", TextNode.valueOf("John Doe")).
		// put("worksFor", TextNode.valueOf("A000059")) //TODO skolem f.
		// ).put("legalEntities", new ObjectNode().
		// put("id", TextNode.valueOf("A000059")).
		// put("name", TextNode.valueOf("CompanyUVW"))
		// )).
		// add(new ObjectNode().put("legalEntities", new ObjectNode().
		// put("id", NullNode.getInstance()).
		// put("name", TextNode.valueOf("CompanyABC"))
		// ));

		// expected output for final person only
		// sopremoPlan.getExpectedOutput(0).
		// addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "A000029").
		// addObject("id", "usCongress3", "name", "John Doe", "worksFor", "A000059");

		// expected output for final LE only
		// sopremoPlan.getExpectedOutput(0).
		// addObject("id", "A000029", "name", "CompanyXYZ").
		// addObject("id", "A000059", "name", "CompanyUVW").
		// addObject("id", null, "name", "CompanyABC");
		//
		// expected final output
		sopremoPlan.getExpectedOutput(0).
			addObject("id", "usCongress1", "name", "Andrew Adams", "worksFor", "A000029").// TODO skolem f.
			addObject("id", "usCongress3", "name", "John Doe", "worksFor", "A000059");
		sopremoPlan.getExpectedOutput(1).
			addObject("id", "A000029", "name", "CompanyXYZ").
			addObject("id", "A000059", "name", "CompanyUVW").
			addObject("id", null, "name", "CompanyABC");

		sopremoPlan.trace();
		sopremoPlan.run();
	}
}
