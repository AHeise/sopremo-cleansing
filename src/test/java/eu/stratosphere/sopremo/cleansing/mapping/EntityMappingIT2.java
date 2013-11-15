package eu.stratosphere.sopremo.cleansing.mapping;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This test shall introduce new design concepts for the EntityMapping operator.
 * @author fabian
 *
 */

public class EntityMappingIT2 extends MeteorIT {

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT2.script"));

		this.client.submit(plan, null, true);
		final JsonParser persons = new JsonParser(new FileReader("src/test/resources/MappingITTestOutputPersons2.json"));
		persons.setWrappingArraySkipping(false);
		IJsonNode expectedPersons = persons.readValueAsTree();
		final JsonParser le = new JsonParser(new FileReader("src/test/resources/MappingITTestOutputLegalEntities2.json"));
		le.setWrappingArraySkipping(false);
		IJsonNode expectedLE = le.readValueAsTree();

		IJsonNode[] personsArray = new IJsonNode[((IArrayNode) expectedPersons).size()];
		for (int i = 0; i < personsArray.length; i++) {
			personsArray[i] = ((IArrayNode) expectedPersons).get(i);
		}

		IJsonNode[] leArray = new IJsonNode[((IArrayNode) expectedLE).size()];
		for (int i = 0; i < leArray.length; i++) {
			leArray[i] = ((IArrayNode) expectedLE).get(i);
		}

		this.testServer.checkContentsOf("MappingITTestOutputPersons2.json", personsArray);

		this.testServer.checkContentsOf("MappingITTestOutputLegalEntities2.json", leArray);
	}
}
