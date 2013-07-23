package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;

public class ScrubbingIT extends MeteorIT {

	@Override
	@Before
	public void setupSpecific() throws Exception {
		this.testServer.createFile("input/input.json",
				JsonUtil.createObjectNode("name", null, "income", 30000, "mgr", false),
				JsonUtil.createObjectNode("income", 20000, "mgr", false),
				JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false));
	}

	@Test
	public void testSuccessfulExecution() throws IOException {
		final SopremoPlan plan = getPlan();

		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("output.json",
				JsonUtil.createObjectNode("name", "Vince Wayne", "income", 32500, "mgr", false));
	}

	@Override
	protected SopremoPlan getPlan() throws IOException {
		final SopremoPlan plan = this.parseScript("using cleansing;\n using govwild;" +

		"$persons = read from 'input/input.json';" +

		"$persons_scrubbed = scrub $persons_sample with rules {name: required};" +

		"write $persons_scrubbed to 'output.json';");

		return plan;
	}

}
