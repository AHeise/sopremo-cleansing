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

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class TransformRecordsForeignKeysIT extends MeteorIT {
	private File usCongressMembers, usCongressBiographies, person, legalEntity;

	@Before
	public void createInput() throws IOException {
		this.usCongressMembers = this.testServer.createFile("usCongressMembers.json",
			createObjectNode("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029"));
		this.usCongressBiographies = this.testServer.createFile("usCongressBiographies.json",
			createObjectNode("biographyId", "A000029", "worksFor", "CompanyXYZ"),
			createObjectNode("biographyId", "A000029", "worksFor", "CompanyABC"),
			createObjectNode("biographyId", "A000011", "worksFor", "CompanyUVW"));
		this.person = this.testServer.getOutputFile("person.json");
		this.legalEntity = this.testServer.getOutputFile("legalEntity.json");
	}

	@Test
	public void testNonEmbeddedForeignKeyReferences() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n" +
			"where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n" +
			"into [\n" +
			"  entity $person with {" +
			"    name: $usCongressMembers.name,\n" +
			"    emp: $legalEntity.id" +
			"  }," +
			"  entity $legalEntity with {" +
			"    name: $usCongressBiographies.worksFor" +	
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n" +
			"write $legalEntity to '" + this.legalEntity.toURI() + "';";

		final SopremoPlan plan = parseScript(query);
		SopremoUtil.trace();
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "name", "Andrew Adams", "emp", "Andrew Adams|CompanyABC"),
			createObjectNode("id", "Andrew Adams", "name", "Andrew Adams", "emp", "Andrew Adams|CompanyXYZ"));

		this.testServer.checkContentsOf("legalEntity.json",
			createObjectNode("id", "Andrew Adams|CompanyXYZ", "name", "CompanyXYZ"),
			createObjectNode("id", "Andrew Adams|CompanyABC", "name", "CompanyABC"),
			createObjectNode("id", "CompanyUVW", "name", "CompanyUVW"));
	}

	@Test
	public void testNonEmbeddedForeignKeyArrayReferences() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n" +
			"where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n" +
			"into [\n" +
			"  entity $person identified by $person.name with {" +
			"    name: $usCongressMembers.name,\n" +
			"    emp: [$legalEntity.id]" +
			"  }," +
			"  entity $legalEntity identified by $legalEntity.name with {" +
			"    name: $usCongressBiographies.worksFor" +	
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n" +
			"write $legalEntity to '" + this.legalEntity.toURI() + "';";

		final SopremoPlan plan = parseScript(query);
		SopremoUtil.trace();
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "name", "Andrew Adams", "emp", createArrayNode("CompanyABC", "CompanyXYZ")));

		this.testServer.checkContentsOf("legalEntity.json",
			createObjectNode("id", "CompanyXYZ", "name", "CompanyXYZ"),
			createObjectNode("id", "CompanyABC", "name", "CompanyABC"),
			createObjectNode("id", "CompanyUVW", "name", "CompanyUVW"));
	}

	@Before
	public void trace() {
		SopremoUtil.trace();
	}
	
	@Test
	public void testEmbeddedReferences() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n" +
			"where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n" +
			"into [\n" +
			"  entity $person identified by $person.name with {" +
			"    name: {fullName: $usCongressMembers.name},\n" +
			"    worksFor: $legalEntity.id" +
			"  }," +
			"  entity $legalEntity with {\n" +
			"    name: $usCongressBiographies.worksFor" +			
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n" +
			"write $legalEntity to '" + this.legalEntity.toURI() + "';";

		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		// Although unintuitive, this is actually the result of spicy.
		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "name", createObjectNode("fullName", "Andrew Adams"),
				"worksFor", null));
//
//		this.testServer.checkContentsOf("person.json",
//			createObjectNode("id", "Andrew Adams|CompanyABC", "name", "Andrew Adams", "emp", "Andrew Adams|CompanyABC"),
//			createObjectNode("id", "Andrew Adams|CompanyXYZ", "name", "Andrew Adams", "emp", "Andrew Adams|CompanyXYZ"));
		
		this.testServer.checkContentsOf("legalEntity.json",
			createObjectNode("id", "CompanyXYZ", "name", "CompanyXYZ"),
			createObjectNode("id", "CompanyABC", "name", "CompanyABC"),
			createObjectNode("id", "CompanyUVW", "name", "CompanyUVW"));
	}

	@Test
	public void testEmbeddedForeignKeyReferences() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n" +
			"where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n" +
			"into [\n" +
			"  entity $person with {" +
			"    name: $usCongressMembers.name,\n" +
			"    employers: {legalEntity: $legalEntity.id}" +
			"  }," +
			"  entity $legalEntity with {" +
			"    name: $usCongressBiographies.worksFor" +	
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n" +
			"write $legalEntity to '" + this.legalEntity.toURI() + "';";
		SopremoUtil.trace();
		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "employer", createObjectNode("legalEntity", "CompanyXYZ")));

		this.testServer.checkContentsOf("legalEntity.json",
			createObjectNode("id", "CompanyXYZ", "name", "CompanyXYZ"),
			createObjectNode("id", "CompanyABC", "name", "CompanyABC"),
			createObjectNode("id", "CompanyUVW", "name", "CompanyUVW"));
	}

	@Test
	public void testEmbeddedForeignArrayKeyReferences() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n" +
			"where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n" +
			"into [\n" +
			"  entity $person with {" +
			"    name: $usCongressMembers.name,\n" +
			"    employers: [{legalEntity: $legalEntity.id}]" +
			"  }," +
			"  entity $legalEntity with {" +
			"    name: $usCongressBiographies.worksFor" +	
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n" +
			"write $legalEntity to '" + this.legalEntity.toURI() + "';";
		SopremoUtil.trace();
		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "employers", createObjectNode("legalEntity", "CompanyXYZ")));

		this.testServer.checkContentsOf("legalEntity.json",
			createObjectNode("id", "CompanyXYZ", "name", "CompanyXYZ"),
			createObjectNode("id", "CompanyABC", "name", "CompanyABC"),
			createObjectNode("id", "CompanyUVW", "name", "CompanyUVW"));
	}

	@Test
	public void testEmbeddedObjectArray() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person = transform records $usCongressMembers\n" +
			"into [\n" +
			"  entity $person identified by $person.name with {" +
			"    name: $usCongressMembers.name,\n" +
			"    employers: [{legalEntity: $usCongressMembers.biography}]" +
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n";
		SopremoUtil.trace();
		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "name", "Andrew Adams", "employers", createArrayNode(createObjectNode("legalEntity", "A000029"))));
	}

	@Test
	public void testEmbeddedObject() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person = transform records $usCongressMembers\n" +
			"into [\n" +
			"  entity $person with {" +
			"    employers: {legalEntity: $usCongressMembers.biography}" +
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n";
		SopremoUtil.trace();
		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "A000029", "employers", createObjectNode("legalEntity", "A000029")));
	}

	@Test
	public void testEmbeddedArray() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
			"$person = transform records $usCongressMembers\n" +
			"into [\n" +
			"  entity $person identified by $person.employers with {" +
			"    name: $usCongressMembers.name,\n" +
			"    employers: [$usCongressMembers.biography]" +
			"  }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n";
		SopremoUtil.trace();
		final SopremoPlan plan = parseScript(query);

		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("person.json",
			createObjectNode("id", "Andrew Adams", "name", "Andrew Adams", "employers", createArrayNode("A000029")));
	}
}