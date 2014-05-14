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
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.JsonUtil;

public class ScrubbingComplexTest extends MeteorIT {
	private File usCongressMembers, person;

	@Before
	public void createInput() throws IOException {
		this.usCongressMembers = this.testServer.createFile(
				"usCongressMembers.json",
				createObjectNode(
						"id",
						"1990-1994",
						"name",
						createObjectNode("firstName", "Adams", "lastName",
								createObjectNode("value", "Adams")), "biography", 1, "party", "PBC"),
				createObjectNode(
										"id",
										"1990-1994",
										"name",
										createObjectNode("firstName", "Adams", "lastName",
												createObjectNode("value", "Adams", "notin" , 1)), "biography", 1, "party", "PBC"));
		this.person = this.testServer.getOutputFile("person.json");
	}

	@Test
	public void testComplexScrubbing() throws IOException {
		SopremoUtil.trace();
		String query = "using cleansing;" + "$usCongressMembers = read from '"
				+ this.usCongressMembers.toURI() + "';\n"
				+ "$persons_scrubbed = scrub $usCongressMembers with rules{"
				+ "id: required,"
				+ "name: {"
				+ "	firstName: required,"
				+ "	lastName: {value : required, notin : required}"
				+ "	}"
				+ "};"
				+ "write $persons_scrubbed to '"+ this.person.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf(
				"person.json",
				createObjectNode(
						"id",
						"1990-1994",
						"name",
						createObjectNode("firstName", "Adams", "lastName",
								createObjectNode("value", "Adams", "notin" , 1)), "biography", 1, "party", "PBC"));
	}
}