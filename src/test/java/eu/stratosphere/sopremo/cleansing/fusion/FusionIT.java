package eu.stratosphere.sopremo.cleansing.fusion;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class FusionIT extends MeteorIT {

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = getPlan();

		this.client.submit(plan, null, true);
		final JsonParser parser = new JsonParser(new FileReader("src/test/resources/FusionITTestOutput.json"));
		parser.setWrappingArraySkipping(true);
		this.testServer.checkContentsOf("FusionITTestOutput.json", parser.readValueAsTree());
	}

	protected SopremoPlan getPlan() {
		File scriptFile = new File("src/test/resources/FusionIT.script");
		final SopremoPlan plan = parseScript(scriptFile);
		return plan;
	}
}
