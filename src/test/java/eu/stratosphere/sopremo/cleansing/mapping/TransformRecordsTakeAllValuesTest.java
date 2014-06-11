package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * This test shall test mapping from one source to two entities.
 * 
 * @author fabian
 */

public class TransformRecordsTakeAllValuesTest extends MeteorIT {
	private File originalPersons, companies;

	@Before
	public void createFiles() throws IOException {
		this.originalPersons = this.testServer.createFile("originalPersons.json",
			createObjectNode("id", 1, "name", "Arvid", "worksFor", "HPI"),
			createObjectNode("id", 2, "name", "Tommy", "worksFor", "Arvid Inc."),
			createObjectNode("id", 2, "name", "Foobar", "worksFor", "Arvid Inc."),
			createObjectNode("id", 3, "name", "Fabian", "worksFor", "SAP"));
		this.companies = this.testServer.getOutputFile("companies.json");
	}

	@Test
	public void testTakeFirstValue() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor,\n" +
			"		   employe: $originalPersons.name\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("companies.json",
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employe", "Tommy"),
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employe", "Foobar"),
			createObjectNode("id", "HPI", "name", "HPI", "employe", "Arvid"),
			createObjectNode("id", "SAP", "name", "SAP", "employe", "Fabian"));
	}
	
	@Test
	public void testTakeAllValues() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor,\n" +
			"		   employe: [$originalPersons.name]\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("companies.json",
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employe",  JsonUtil.createArrayNode("Tommy", "Foobar")),
			createObjectNode("id", "HPI", "name", "HPI", "employe", JsonUtil.createArrayNode("Arvid")),
			createObjectNode("id", "SAP", "name", "SAP", "employe", JsonUtil.createArrayNode("Fabian")));
	}
}
