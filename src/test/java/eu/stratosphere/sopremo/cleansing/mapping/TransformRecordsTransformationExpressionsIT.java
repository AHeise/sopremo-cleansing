package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * This test shall introduce new design concepts for the EntityMapping operator.
 * 
 * @author fabian
 */

public class TransformRecordsTransformationExpressionsIT extends MeteorIT {
	private File originalPersons, originalJobs, person;

	@Before
	public void createFiles() throws IOException {
		this.originalPersons = this.testServer.createFile("originalPersons.json",
			createObjectNode("id", 1, "name", "Arvid", "street", "Prof. Dr. Helmert", "zip", 14482, "job", 1,
				"pastJobs", new int[] { 3, 4 }),
			createObjectNode("id", 2, "name", "Tommy", "street", "Babelsberger Straße", "zip", 14485, "job", 2,
				"pastJobs", new int[] {}));
		this.originalJobs = this.testServer.createFile("originalJobs.json",
			createObjectNode("id", 1, "title", "wimi"),
			createObjectNode("id", 2, "title", "hiwi"));
		this.person = this.testServer.getOutputFile("person.json");
	}

	@Test
	public void testMappingWithFunction() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"$originalJobs = read from '" + this.originalJobs.toURI() + "';\n" +
			"\n" +
			"$persons  = transform records $originalPersons," +
			" $originalJobs where ($originalPersons.job == $originalJobs.id)\n" +
			"	into [\n" +
			"	    entity $originalPersons identified by $originalPersons.id with {\n" +
			"	    	name: concat_strings($originalPersons.name, '-', $originalJobs.title)\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $persons to '" + this.person.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("persons.json",
			createObjectNode("id", 1, "name", "Arvid-wimi"),
			createObjectNode("id", 2, "name", "Tommy-hiwi"));
	}

	@Test
	public void testMappingWithArrayAccess() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"$originalJobs = read from '" + this.originalJobs.toURI() + "';\n" +
			"\n" +
			"$persons  = transform records $originalPersons," +
			" $originalJobs where ($originalPersons.job == $originalJobs.id)\n" +
			"	into [\n" +
			"	    entity $originalPersons identified by $originalPersons.id with {\n" +
			"	    	name: concat_strings($originalPersons.name, '-', $originalJobs.title),\n" +
			"	    	firstJob: $originalPersons.pastJobs[0]\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $persons to '" + this.person.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("persons.json",
			createObjectNode("id", 1, "name", "Arvid-wimi", "firstJob", 3),
			createObjectNode("id", 2, "name", "Tommy-hiwi"));
	}

	@Test
	public void testMappingWithTernaryExpression() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"$originalJobs = read from '" + this.originalJobs.toURI() + "';\n" +
			"\n" +
			"$persons  = transform records $originalPersons," +
			" $originalJobs where ($originalPersons.job == $originalJobs.id)\n" +
			"	into [\n" +
			"	    entity $originalPersons identified by $originalPersons.id with {\n" +
			"	    	name: concat_strings($originalPersons.name, '-', $originalJobs.title),\n" +
			"	    	firstJob: $originalPersons.pastJobs[1] ? : null\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $persons to '" + this.person.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("persons.json",
			createObjectNode("id", 1, "name", "Arvid-wimi", "firstJob", 4),
			createObjectNode("id", 2, "name", "Tommy-hiwi", "firstJob", null));
	}
}
