package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * This test shall test mapping from one source to two entities.
 * 
 * @author fabian
 */

public class TransformRecordsEmbeddedObjects extends MeteorIT {
	private File originalPersons, persons;

	@Before
	public void createFiles() throws IOException {
		this.originalPersons = this.testServer.createFile("originalPersons.json",
			createObjectNode("id", 1, "person", createObjectNode("name", "Arvid"), "worksFor", "HPI"),
			createObjectNode("id", 2, "person", createObjectNode("name", "Fabian"), "worksFor", "HPI"));
		this.persons = this.testServer.getOutputFile("persons.json");
	}

	@Test
	public void testEmbeddedObjects() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$persons  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $persons identified by $persons.id with {\n" +
			"	       id: $originalPersons.person.name\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $persons to '" + this.persons.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		SopremoUtil.trace();
		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("persons.json",
			createObjectNode("id", "Arvid"),
			createObjectNode("id", "Fabian"));
	}
}
