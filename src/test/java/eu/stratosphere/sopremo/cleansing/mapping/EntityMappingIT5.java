package eu.stratosphere.sopremo.cleansing.mapping;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This test shall introduce new design concepts for the EntityMapping operator.
 * @author fabian
 *
 */

public class EntityMappingIT5 extends MeteorIT {

	@Test
	public void testGroupingForMapping1() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT5a.script"));

		this.client.submit(plan, null, true);
		IJsonNode[] personsArray = getContentsToCheckFrom("src/test/resources/MappingIT5TestOutputPersons.json");

		IJsonNode[] leArray = getContentsToCheckFrom("src/test/resources/MappingIT5aTestOutputCompanies.json");

		this.testServer.checkContentsOf("MappingIT5TestOutputPersons.json", personsArray);

		this.testServer.checkContentsOf("MappingIT5aTestOutputCompanies.json", leArray);
	}
	
	@Test
	public void testGroupingForMapping2() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT5b.script"));

		this.client.submit(plan, null, true);
		IJsonNode[] personsArray = getContentsToCheckFrom("src/test/resources/MappingIT5TestOutputPersons.json");

		IJsonNode[] leArray = getContentsToCheckFrom("src/test/resources/MappingIT5bTestOutputCompanies.json");

		this.testServer.checkContentsOf("MappingIT5TestOutputPersons.json", personsArray);

		this.testServer.checkContentsOf("MappingIT5bTestOutputCompanies.json", leArray);
	}
}
