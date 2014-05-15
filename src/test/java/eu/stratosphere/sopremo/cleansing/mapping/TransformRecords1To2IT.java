package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;

/**
 * This test shall test mapping from one source to two entities.
 * 
 * @author fabian
 */

public class TransformRecords1To2IT extends MeteorIT {
	private File originalPersons, person, companies;

	@Before
	public void createFiles() throws IOException {
		this.originalPersons = this.testServer.createFile("originalPersons.json",
			createObjectNode("id", 1, "name", "Arvid", "worksFor", "HPI"),
			createObjectNode("id", 2, "name", "Tommy", "worksFor", "Arvid Inc."),
			createObjectNode("id", 3, "name", "Fabian", "worksFor", "SAP"));
		this.person = this.testServer.getOutputFile("person.json");
		this.companies = this.testServer.getOutputFile("companies.json");
	}

	@Test
	public void testMappingFromOneSourceToTwoEntities() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$persons, $companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $persons with {\n" +
			"	    	name: $originalPersons.name,\n" +
			"	    	worksFor: $companies.id\n" +
			"	    },\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $persons to '" + this.person.toURI() + "';\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Arvid", "name", "Arvid", "worksFor", "HPI"),
			createObjectNode("id", "Tommy", "name", "Tommy", "worksFor", "Arvid Inc."),
			createObjectNode("id", "Fabian", "name", "Fabian", "worksFor", "SAP"));
		this.testServer.checkContentsOf("companies.json",
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc."),
			createObjectNode("id", "HPI", "name", "HPI"),
			createObjectNode("id", "SAP", "name", "SAP"));
	}

	@Test
	public void testSwitchedOutputs() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$persons, $companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor\n" +
			"	    },\n" +
			"	    entity $persons with {\n" +
			"	    	name: $originalPersons.name,\n" +
			"	    	worksFor: $companies.id\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $persons to '" + this.person.toURI() + "';\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Arvid", "name", "Arvid", "worksFor", "HPI"),
			createObjectNode("id", "Tommy", "name", "Tommy", "worksFor", "Arvid Inc."),
			createObjectNode("id", "Fabian", "name", "Fabian", "worksFor", "SAP"));
		this.testServer.checkContentsOf("companies.json",
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc."),
			createObjectNode("id", "HPI", "name", "HPI"),
			createObjectNode("id", "SAP", "name", "SAP"));
	}
}
