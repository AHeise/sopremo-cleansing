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
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT3.script"));
		this.client.submit(plan, null, true);

		IJsonNode[] personsArray = getContentsToCheckFrom("src/test/resources/MappingIT3TestOutputPersons.json");

		this.testServer.checkContentsOf("MappingIT3TestOutputPersons.json", personsArray);
	}
}
