/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.JsonUtil;

public class GlobalMatchingIT extends MeteorIT {
	private File pairs, output;

	private final static IArrayNode<?>
			r0 = JsonUtil.createArrayNode(createObjectNode("id", 1, "name", "Johnson"), createObjectNode("id", 2, "name", "John")),
			r1 = JsonUtil.createArrayNode(createObjectNode("id", 4, "name", "John"), createObjectNode("id", 2, "name", "John")),
			r2 = JsonUtil.createArrayNode(createObjectNode("id", 4, "name", "John"), createObjectNode("id", 3, "name", "J.")),
			r3 = JsonUtil.createArrayNode(createObjectNode("id", 5, "name", "Meyer"), createObjectNode("id", 6, "name", "Mayer"));

	@Before
	public void createInput() throws IOException {
		this.pairs = this.testServer.createFile("pairs.json", r0, r1, r2, r3);
		this.output = this.testServer.getOutputFile("output.json");
	}

	@Test
	public void testNaive() throws IOException {
		final SopremoPlan plan = parseScript("using cleansing;" +
			"$pairs = read from '" + this.pairs.toURI() + "';" +
			"$cluster = global match $pair in $pairs " +
			"  by jaro($pair[0].name, $pair[1].name);" +
			"write $cluster to '" + this.output.toURI() + "';");
		SopremoUtil.trace();
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("output.json",
				JsonUtil.createArrayNode(createObjectNode("id", 4, "name", "John"), createObjectNode("id", 2, "name", "John")),
				JsonUtil.createArrayNode(createObjectNode("id", 1, "name", "Johnson"), createObjectNode("id", 3, "name", "J.")),
				JsonUtil.createArrayNode(createObjectNode("id", 5, "name", "Meyer"), createObjectNode("id", 6, "name", "Mayer")));
	}
	
	@Test
	public void testWithThreshold() throws IOException {
		final SopremoPlan plan = parseScript("using cleansing;" +
			"$pairs = read from '" + this.pairs.toURI() + "';" +
			"$cluster = global match $pair in $pairs " +
			"  by jaro($pair[0].name, $pair[1].name) > 0.7;" +
			"write $cluster to '" + this.output.toURI() + "';");
		SopremoUtil.trace();
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("output.json",
				JsonUtil.createArrayNode(createObjectNode("id", 4, "name", "John"), createObjectNode("id", 2, "name", "John")),
				JsonUtil.createArrayNode(createObjectNode("id", 5, "name", "Meyer"), createObjectNode("id", 6, "name", "Mayer")));
	}
}
