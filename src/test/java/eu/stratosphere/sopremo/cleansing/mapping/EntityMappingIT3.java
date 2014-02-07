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

public class EntityMappingIT3 extends MeteorIT {

	@Test
	public void testMappingWithFunction() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT3a.script"));
		this.client.submit(plan, null, true);

		IJsonNode[] personsArray = getContentsToCheckFrom("src/test/resources/MappingIT3aTestOutputPersons.json");

		this.testServer.checkContentsOf("MappingIT3aTestOutputPersons.json", personsArray);
	}
	
	@Test
	public void testMappingWithArrayAccess() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT3b.script"));
		this.client.submit(plan, null, true);

		IJsonNode[] personsArray = getContentsToCheckFrom("src/test/resources/MappingIT3bTestOutputPersons.json");

		this.testServer.checkContentsOf("MappingIT3bTestOutputPersons.json", personsArray);
	}
	
	@Test
	public void testMappingWithTernaryExpression() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT3c.script"));
		this.client.submit(plan, null, true);

		IJsonNode[] personsArray = getContentsToCheckFrom("src/test/resources/MappingIT3cTestOutputPersons.json");

		this.testServer.checkContentsOf("MappingIT3cTestOutputPersons.json", personsArray);
	}
}
