package eu.stratosphere.sopremo.cleansing.scrubbing;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IObjectNode;

public class RangeConstraintIT extends MeteorIT {

	private final static IObjectNode
			r0 = createObjectNode("number", 1000, "name", "NOT filtered"),
			r1 = createObjectNode("number", 499, "name", "filtered (number too small)"),
			r2 = createObjectNode("number", 1501, "name", "filtered (number too big");

	private File records;

	private File output;

	@Before
	public void createInput() throws IOException {
		this.records = this.testServer.createFile("records.json", r0, r1, r2);
		this.output = this.testServer.getOutputFile("output.json");
	}

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = this.getPlan();

		this.client.submit(plan, null, true);
		final JsonParser parser = new JsonParser(new FileReader("src/test/resources/TestOutputRange.json"));
		parser.setWrappingArraySkipping(true);

		this.testServer.checkContentsOf("output.json",
			createObjectNode("number", 1000, "name", "NOT filtered"),
			createObjectNode("number", 500, "name", "filtered (number too small)"),
			createObjectNode("number", 1500, "name", "filtered (number too big"));
	}

	protected SopremoPlan getPlan() {
		return this.parseScript("using cleansing;" +
			"$data = read from '" + this.records.toURI() + "';" +
			"$data_scrubbed = scrub $data with rules {" +
			"number: range(500, 1500)," + "};" +
			"write $data_scrubbed to '" + this.output.toURI() + "';");
	}
}
