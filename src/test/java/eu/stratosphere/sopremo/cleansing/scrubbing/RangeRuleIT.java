package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class RangeRuleIT extends MeteorIT {

	@Test @org.junit.Ignore
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = getPlan();

		this.client.submit(plan, null, true);
		final JsonParser parser = new JsonParser(new FileReader("src/test/resources/TestOutputRange.json"));
		parser.setWrappingArraySkipping(true);
		File outputFile = new File("/tmp/TestOutputRange.json");
		this.testServer.checkContentsOf(outputFile.getName(), parser.readValueAsTree());
		//TODO improve this
		outputFile.delete();
	}

	@Override
	protected SopremoPlan getPlan() throws IOException {
		final SopremoPlan plan = parseScript("using cleansing;" +
		"$data = read from 'src/test/resources/TestDataRange.json';" +
		"$data_scrubbed = scrub $data with rules {" +
		"number: range(500, 1500)," + "};" +
		"write $data_scrubbed to 'file:///tmp/TestOutputRange.json';");
		return plan;
	}
}
