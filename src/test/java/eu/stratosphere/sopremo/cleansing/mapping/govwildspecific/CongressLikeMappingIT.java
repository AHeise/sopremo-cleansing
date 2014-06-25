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
package eu.stratosphere.sopremo.cleansing.mapping.govwildspecific;

import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;
import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class CongressLikeMappingIT extends MeteorIT {
	private File usCongress, politician, party;

	@Before
	public void createInput() throws IOException {
		this.usCongress = this.testServer.createFile("usCongress.json",
			createObjectNode("party", "Democrats", "person", createObjectNode("name", "Andrew Adams")),
			createObjectNode("party", "Republican", "person", createObjectNode("name", "Andrew Adams")));
		
		this.politician = this.testServer.getOutputFile("politician.json");
		this.party = this.testServer.getOutputFile("party.json");
	}

	@Test
	public void testEmbeddedForeignKeys() throws IOException {

		String query = "using cleansing;\n" +
			"$usCongress = read from '" + this.usCongress.toURI() + "';\n" +
			"$person, $party = transform records $usCongress\n" +
			"into [\n" +
			"  entity $person identified by $person.id with{\n" +
			"    id: $usCongress.person.name,\n" +
			"    worksForParty: [{party: $party.id, dummy: 'field'}],\n" +
			"  }\n," +
			"  entity $party identified by $party.name with {\n" +
			"    name: $usCongress.party,\n" +
			"  }\n" +
			"];\n" +
			"write $person to '" + this.politician.toURI() + "';\n" +
			"write $party to '" + this.party.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("politician.json",
			createObjectNode("id", "Andrew Adams", "worksForParty", createArrayNode(
					createObjectNode("party", "Democrats", "dummy", "field"),
					createObjectNode("party", "Republican", "dummy", "field"))));

		this.testServer.checkContentsOf("party.json",
			createObjectNode("id", "Republican", "name", "Republican"),
			createObjectNode("id", "Democrats", "name", "Democrats"));
	}
}