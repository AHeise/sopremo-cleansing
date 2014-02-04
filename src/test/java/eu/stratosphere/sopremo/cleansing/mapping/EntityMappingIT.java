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
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class EntityMappingIT extends MeteorIT {
	private File usCongressMembers, usCongressBiographies, person, legalEntity;

	@Before
	public void createInput() throws IOException {
		this.usCongressMembers = this.testServer.createFile("usCongressMembers.json", 
				createObjectNode("id", "usCongress1", "name", "Andrew Adams", "biography", "A000029"),
				createObjectNode("id", "usCongress2", "name", "John Adams", "biography", "A000039"), 
				createObjectNode("id", "usCongress3", "name", "John Doe", "biography", "A000059"));
		this.usCongressBiographies = this.testServer.createFile("usCongressBiographies.json", 
				createObjectNode("biographyId", "A000029", "worksFor", "CompanyXYZ"),
				createObjectNode("biographyId", "A000059", "worksFor", "CompanyUVW"),
				createObjectNode("biographyId", "A000049", "worksFor", "CompanyABC"));
		this.person = this.testServer.getOutputFile("person.json");
		this.legalEntity = this.testServer.getOutputFile("legalEntity.json");
	}
	
	@Test
	public void testSimpleMapping() throws IOException {

		String query = "using cleansing;"+
				"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
				"$usCongressBiographies = read from '" + this.usCongressBiographies.toURI() + "';\n" +
				"$person, $legalEntity = map entities of $usCongressMembers, $usCongressBiographies\n" +
				"where ($usCongressMembers.biography[1:1] == $usCongressBiographies.biographyId[1:1])\n" + 
				"into [\n" +  
				"  entity $usCongressMembers identified by $usCongressMembers.id with {" + 
				"    name: $usCongressMembers.name,\n" +
				"    worksFor: $legalEntity.id" + 
				"  }," + 
				"  entity $usCongressBiographies identified by $usCongressBiographies.worksFor with {" + 
				"    name: $usCongressBiographies.worksFor" + 
				"  }" + 
				"];\n" + 
				"write $person to '" + this.person.toURI() + "';\n" +
				"write $legalEntity to '" + this.legalEntity.toURI() + "';";
		
		final SopremoPlan plan = parseScript(query);
		
		Assert.assertNotNull(this.client.submit(plan, null, true));
		
		this.testServer.checkContentsOf("person.json",
				createObjectNode("id", "usCongress1", "name", "Andrew Adams", "worksFor", "CompanyXYZ"),
				createObjectNode("id", null, "name", null, "worksFor", "CompanyABC"),
				createObjectNode("id", "usCongress3", "name", "John Doe", "worksFor", "CompanyUVW"));
		
		this.testServer.checkContentsOf("legalEntity.json",
				createObjectNode("id", "CompanyXYZ", "name", "CompanyXYZ"),
				createObjectNode("id", "CompanyUVW", "name", "CompanyUVW"),
				createObjectNode("id", "CompanyABC", "name", "CompanyABC"));
	}
}