package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IJsonNode;

public class ScrubbingIT extends MeteorIT {

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = parseScript(new File("src/test/resources/ScrubbingIT.script"));

		this.client.submit(plan, null, true);
		
		IJsonNode[] nodeArray = getContentsToCheckFrom("src/test/resources/TestOutput.json");
		
		this.testServer.checkContentsOf("TestOutput.json", nodeArray);
	}
}
