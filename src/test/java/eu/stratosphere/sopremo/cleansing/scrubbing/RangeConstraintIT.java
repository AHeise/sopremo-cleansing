package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class RangeConstraintIT extends MeteorIT {

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = this.getPlan();

		this.client.submit(plan, null, true);
		final JsonParser parser = new JsonParser(new FileReader("src/test/resources/TestOutputRange.json"));
		parser.setWrappingArraySkipping(true);

		this.testServer.checkContentsOf("TestOutputRange.json", parser.readValueAsTree());
	}

	protected SopremoPlan getPlan() {
		return this.parseScript("using cleansing;" +
			"$data = read from 'src/test/resources/TestDataRange.json';" +
			"$data_scrubbed = scrub $data with rules {" +
			"number: range(500, 1500)," + "};" +
			"write $data_scrubbed to 'file:///tmp/TestOutputRange.json';");
	}
}
