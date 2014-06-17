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
package eu.stratosphere.sopremo.cleansing.scrubbing;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class ScrubbingIT2 extends MeteorIT {
	private File usCongressMembers, congressScrubbed;

	@Before
	public void createInput() throws IOException {
		this.usCongressMembers = this.testServer.createFile("usCongressMembers.json",
			createObjectNode("field1", "foo", "field2", null),
			createObjectNode("field1", null, "field2", 42));
		this.congressScrubbed = this.testServer.getOutputFile("congressScrubbed.json");
	}

	@Test
	public void testScrubbing() throws IOException {

		String query = "using cleansing;\n" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$congressScrubbed = scrub $usCongressMembers with rules {\n"+
				"field1: required?:default(''),\n"+
				"field2: required?:default(0)\n"+
			"};\n"+
			"write $congressScrubbed to '" + this.congressScrubbed.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf(
			"congressScrubbed.json",
			createObjectNode("field1", "foo", "field2", 0),
			createObjectNode("field1", "", "field2", 42));
	}
}