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
package eu.stratosphere.sopremo.cleansing.record_linkage;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.sopremo.type.ObjectNode;

/**
 * @author arv
 */
public class DuplicateDetectionIT extends MeteorIT {
	private File input, output;

	private final static ObjectNode
			r0 = createObjectNode("id", 0, "firstName", "albert", "lastName", "perfect duplicate", "age", 80),
			r1 = createObjectNode("id", 1, "firstName", "berta", "lastName", "typo", "age", 70),
			r2 = createObjectNode("id", 2, "firstName", "charles", "lastName", "age inaccurate", "age", 70),
			r3 = createObjectNode("id", 3, "firstName", "dagmar", "lastName", "unmatched", "age", 75),
			r4 = createObjectNode("id", 4, "firstName", "elma", "lastName", "firstNameDiffers", "age", 60),
			r10 = createObjectNode("id", 10, "firstName", "albert", "lastName", "perfect duplicate", "age", 80),
			r11 = createObjectNode("id", 11, "firstName", "betra", "lastName", "typo", "age", 70),
			r12 = createObjectNode("id", 12, "firstName", "charles", "lastName", "age inaccurate", "age", 69),
			r14 = createObjectNode("id", 14, "firstName", "alma", "lastName", "firstNameDiffers", "age", 60);

	@Before
	public void createInput() throws IOException {
		this.input = this.testServer.createFile("input.json", r0, r1, r2, r3, r4, r10, r11, r12, r14);
		this.output = this.testServer.getOutputFile("output.json");
	}

	@Test
	public void testNaive() throws IOException {

		final SopremoPlan plan = parseScript("using cleansing;" +
			"$persons = read from '" + this.input.toURI() + "';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7;" +
			"write $duplicates to '" + this.output.toURI() + "';");

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("output.json",
			JsonUtil.createArrayNode(r0, r10),
			JsonUtil.createArrayNode(r2, r12),
			JsonUtil.createArrayNode(r4, r14));
	}

	@Test
	public void testBlocking() throws IOException {

		final SopremoPlan plan = parseScript("using cleansing;" +
			"$persons = read from '" + this.input.toURI() + "';" +
			"$duplicates = detect duplicates $persons " +
			"  where (levenshtein($persons.firstName) + 3*jaro($person.lastName))/2 > 0.8" +
			"  partition on $persons.age;" +
			"write $duplicates to '" + this.output.toURI() + "';");

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("output.json",
			JsonUtil.createArrayNode(r0, r10),
			JsonUtil.createArrayNode(r4, r14));
	}

	@Test
	public void testSorting() throws IOException {

		final SopremoPlan plan = parseScript("using cleansing;" +
			"$persons = read from '" + this.input.toURI() + "';" +
			"$duplicates = detect duplicates $persons " +
			"  where levenshtein($persons.firstName) >= 0.7" +
			"  sort on $persons.age" +
			"  with window 20;" +
			"write $duplicates to '" + this.output.toURI() + "';");

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("output.json",
			JsonUtil.createArrayNode(r0, r10),
			JsonUtil.createArrayNode(r4, r14));
	}
}
