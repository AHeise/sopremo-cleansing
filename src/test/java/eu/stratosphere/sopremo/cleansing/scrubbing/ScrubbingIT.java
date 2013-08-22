package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class ScrubbingIT extends MeteorIT {

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/ScrubbingIT.script"));

		this.client.submit(plan, null, true);
		final JsonParser parser = new JsonParser(new FileReader("src/test/resources/TestOutput.json"));
		parser.setWrappingArraySkipping(true);
		this.testServer.checkContentsOf("TestOutput.json", parser.readValueAsTree());
	}
}
