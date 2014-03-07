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
 * This IT simply does the same as {@link EntityMappingIT}, but it takes the
 * script as well as the input and expected outputs from files.
 * 
 * @author fabian
 * 
 */

public class EntityMappingITfromFileInput extends MeteorIT {

	@Test
	public void testSimpleMappingFromMeteorFile() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/MappingIT.script"));

		this.client.submit(plan, null, true);
		final JsonParser persons = new JsonParser(new FileReader("src/test/resources/MappingITTestOutputPersons.json"));
		persons.setWrappingArraySkipping(false);
		IJsonNode expectedPersons = persons.readValueAsTree();
		final JsonParser le = new JsonParser(new FileReader("src/test/resources/MappingITTestOutputLegalEntities.json"));
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

		this.testServer.checkContentsOf("MappingITTestOutputPersons.json", personsArray);

		this.testServer.checkContentsOf("MappingITTestOutputLegalEntities.json", leArray);
	}
}
