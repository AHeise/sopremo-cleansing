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
public class RecordLinkageIT extends MeteorIT {
	private File inputR, inputS, output;

	private final static ObjectNode
			r0 = createObjectNode("id", 0, "firstName", "albert", "lastName", "A"),
			r1 = createObjectNode("id", 1, "firstName", "gustav", "lastName", "B"),
			r2 = createObjectNode("id", 2, "firstName", "frank", "lastName", "C"),
			r3 = createObjectNode("id", 3, "firstName", "julius", "lastName", "D"),
			r4 = createObjectNode("id", 4, "firstName", "albert", "lastName", "E"),
			s0 = createObjectNode("id", 0, "fName", "albert", "lName", "A"),
			s1 = createObjectNode("id", 1, "fName", "gustav", "lName", "B"),
			s2 = createObjectNode("id", 2, "fName", "frank", "lName", "C"),
			s3 = createObjectNode("id", 3, "fName", "julius", "lName", "D"),
			s4 = createObjectNode("id", 4, "fName", "albert", "lName", "E");

	@Before
	public void createInput() throws IOException {
		this.inputR = this.testServer.createFile("inputR.json", r0, r1, r2, r3, r4);
		this.inputS = this.testServer.createFile("inputS.json", s0, s1, s2, s3, s4);
		this.output = this.testServer.getOutputFile("output.json");
	}

	@Test
	public void testNaive() throws IOException {
		final SopremoPlan plan = parseScript("using cleansing;" +
			"$personsR = read from '" + this.inputR.toURI() + "';" +
			"$personsS = read from '" + this.inputS.toURI() + "';" +
			"$duplicates = link records $personsR, $personsS " +
			"  where (jaro($personsR.firstName, $personsS.fName)*3 + jaro($personsR.lastName, $personsS.lName))/4 >= 0.7;" +
			"write $duplicates to '" + this.output.toURI() + "';");

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("output.json",
			JsonUtil.createArrayNode(r0, s0),
			JsonUtil.createArrayNode(r0, s4),
			JsonUtil.createArrayNode(r1, s1),
			JsonUtil.createArrayNode(r2, s2),
			JsonUtil.createArrayNode(r3, s3),
			JsonUtil.createArrayNode(r4, s4),
			JsonUtil.createArrayNode(r4, s0));
	}

	@Test
	public void testBlocking1() throws IOException {

		final SopremoPlan plan = parseScript("using cleansing;" +
				"$personsR = read from '" + this.inputR.toURI() + "';" +
				"$personsS = read from '" + this.inputS.toURI() + "';" +
				"$duplicates = link records $personsR, $personsS " +
				"  partition on {substring($personsR.lastName, 0, 3) : substring($personsS.lName, 0, 3)}" +
				"  where (jaro($personsR.firstName, $personsS.fName)*3 + jaro($personsR.lastName, $personsS.lName))/4 >= 0.7;" +
				"write $duplicates to '" + this.output.toURI() + "';");

			Assert.assertNotNull(this.client.submit(plan, null, true));

			this.testServer.checkContentsOf("output.json",
				JsonUtil.createArrayNode(r0, s0),
				JsonUtil.createArrayNode(r1, s1),
				JsonUtil.createArrayNode(r2, s2),
				JsonUtil.createArrayNode(r3, s3),
				JsonUtil.createArrayNode(r4, s4));
	}
	
	@Test
	public void testBlocking2() throws IOException {

		final SopremoPlan plan = parseScript("using cleansing;" +
				"$personsR = read from '" + this.inputR.toURI() + "';" +
				"$personsS = read from '" + this.inputS.toURI() + "';" +
				"$duplicates = link records $personsR, $personsS " +
				"  partition on {$personsR.firstName : $personsS.fName}" +
				"  where (jaro($personsR.firstName, $personsS.fName)*3 + jaro($personsR.lastName, $personsS.lName))/4 >= 0.7;" +
				"write $duplicates to '" + this.output.toURI() + "';");

			Assert.assertNotNull(this.client.submit(plan, null, true));

			this.testServer.checkContentsOf("output.json",
					JsonUtil.createArrayNode(r0, s0),
					JsonUtil.createArrayNode(r0, s4),
					JsonUtil.createArrayNode(r1, s1),
					JsonUtil.createArrayNode(r2, s2),
					JsonUtil.createArrayNode(r3, s3),
					JsonUtil.createArrayNode(r4, s4),
					JsonUtil.createArrayNode(r4, s0));
	}

	@Test
	public void testSorting1() throws IOException {

		final SopremoPlan plan = parseScript("using cleansing;" +
				"$personsR = read from '" + this.inputR.toURI() + "';" +
				"$personsS = read from '" + this.inputS.toURI() + "';" +
				"$duplicates = link records $personsR, $personsS " +
				"  sort on {$personsR.lastName : $personsS.lName}" +
				"  where (jaro($personsR.firstName, $personsS.fName)*3 + jaro($personsR.lastName, $personsS.lName))/4 >= 0.7;" +
				"write $duplicates to '" + this.output.toURI() + "';");

			Assert.assertNotNull(this.client.submit(plan, null, true));

			this.testServer.checkContentsOf("output.json",
				JsonUtil.createArrayNode(r0, s0),
				JsonUtil.createArrayNode(r1, s1),
				JsonUtil.createArrayNode(r2, s2),
				JsonUtil.createArrayNode(r3, s3),
				JsonUtil.createArrayNode(r4, s4));
	}
	
	@Test
	public void testSorting2() throws IOException {

		final SopremoPlan plan = parseScript("using cleansing;" +
				"$personsR = read from '" + this.inputR.toURI() + "';" +
				"$personsS = read from '" + this.inputS.toURI() + "';" +
				"$duplicates = link records $personsR, $personsS " +
				"  sort on {$personsR.lastName : $personsS.lName} with window 6" +
				"  where (jaro($personsR.firstName, $personsS.fName)*3 + jaro($personsR.lastName, $personsS.lName))/4 >= 0.7;" +
				"write $duplicates to '" + this.output.toURI() + "';");

			Assert.assertNotNull(this.client.submit(plan, null, true));

			this.testServer.checkContentsOf("output.json",
					JsonUtil.createArrayNode(r0, s0),
					JsonUtil.createArrayNode(r0, s4),
					JsonUtil.createArrayNode(r1, s1),
					JsonUtil.createArrayNode(r2, s2),
					JsonUtil.createArrayNode(r3, s3),
					JsonUtil.createArrayNode(r4, s4),
					JsonUtil.createArrayNode(r4, s0));
	}
}
