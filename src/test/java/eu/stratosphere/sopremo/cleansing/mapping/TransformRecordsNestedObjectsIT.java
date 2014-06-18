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
package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class TransformRecordsNestedObjectsIT extends MeteorIT {
	private File usCongressMembers, usCongressBiographies, person, legalEntity;

	@Before
	public void createInput() throws IOException {
		this.usCongressMembers = this.testServer.createFile("usCongressMembers.json",
			createObjectNode("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029"));
		this.usCongressBiographies = this.testServer.createFile("usCongressBiographies.json",
			createObjectNode("biographyId", "A000029", "worksFor", "CompanyXYZ"));
		this.person = this.testServer.getOutputFile("person.json");
		this.legalEntity = this.testServer.getOutputFile("legalEntity.json");
	}
	
	@Test
	public void testDoubleForeignKey() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n" +
			"where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n" +
			"into [\n" +
			"  entity $person identified by $person.name with {" +
			"    name: $usCongressMembers.name,\n" +
			"    worksFor: {foo: {bar: $legalEntity.id}, bar: $legalEntity.id}\n" +
			"  },\n" +
			"  entity $legalEntity with {\n" +
			"    lname: $usCongressBiographies.worksFor\n" +
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n" +
			"write $legalEntity to '" + this.legalEntity.toURI() + "';";

		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "name", "Andrew Adams", "worksFor",
				createObjectNode("foo", createObjectNode("bar", "Andrew Adams"), "bar", "Andrew Adams")));

		this.testServer.checkContentsOf("legalEntity.json",
			createObjectNode("id", "Andrew Adams", "lname", "CompanyXYZ"));
	}
}