package eu.stratosphere.sopremo.cleansing.fusion;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

public class FusionIT extends MeteorIT {

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = getPlan();

		this.client.submit(plan, null, true);
		final JsonParser parser = new JsonParser(new FileReader(
				"src/test/resources/FusionITTestOutput.json"));
		parser.setWrappingArraySkipping(true);
		List<IJsonNode> expectedValues = new LinkedList<IJsonNode>();
		while (!parser.checkEnd())
			expectedValues.add(parser.readValueAsTree());
		this.testServer.checkContentsOf("FusionITTestOutput.json",
				expectedValues.toArray(new IJsonNode[expectedValues.size()]));
	}

	protected SopremoPlan getPlan() {
		File scriptFile = new File("src/test/resources/FusionIT.script");
		final SopremoPlan plan = parseScript(scriptFile);
		return plan;
	}
}
