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
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.JsonUtil;

public class GlobalMatchingIT extends MeteorIT {
	private File pairs, output;

	private final static IArrayNode<?>
			r0 = JsonUtil.createArrayNode(createObjectNode("id", 1, "value", 1), createObjectNode("id", 2, "value", 3)),
			r1 = JsonUtil.createArrayNode(createObjectNode("id", 3, "value", 2), createObjectNode("id", 4, "value", 3));

	@Before
	public void createInput() throws IOException {
		this.pairs = this.testServer.createFile("pairs.json", r0, r1);
		this.output = this.testServer.getOutputFile("output.json");
	}

	@Test
	public void testNaive() throws IOException {
		final SopremoPlan plan = parseScript("using cleansing;" +
			"$pairs = read from '" + this.pairs.toURI() + "';" +
			"$cluster = global match $pair in $pairs " +
			"  with $pair[0].value-$pair[1].value;" +
			"write $cluster to '" + this.output.toURI() + "';");

		Assert.assertNotNull(this.client.submit(plan, null, true));

//		this.testServer.checkContentsOf("output.json",
//			JsonUtil.createArrayNode(r1, s1),
//			JsonUtil.createArrayNode(r0, s4),
//			JsonUtil.createArrayNode(r1, s1),
//			JsonUtil.createArrayNode(r2, s2),
//			JsonUtil.createArrayNode(r3, s3),
//			JsonUtil.createArrayNode(r4, s4),
//			JsonUtil.createArrayNode(r4, s0));
	}
}
