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

public class FreebaseLikeMappingIT extends MeteorIT {
	private File freebase, politician;

	@Before
	public void createInput() throws IOException {
		this.freebase = this.testServer.createFile("freebase.json",
			createObjectNode("id", 1, "name", createArrayNode("Andrew", "Jr.", "Adams"), 
					"positions", createArrayNode(
							createObjectNode("title", "Senator", "sessions", createArrayNode(1,2,3)),
							createObjectNode("title", "Representative", "sessions", createArrayNode(4))
							)
			)
		);
		
		this.politician = this.testServer.getOutputFile("politician.json");
	}

	@Test
	public void testArrayAccess() throws IOException {

		String query = "using cleansing;\n" +
			"$freebase = read from '" + this.freebase.toURI() + "';\n" +
			"$person = transform records $freebase\n" +
			"into [\n" +
			"  entity $person identified by $person.id with{\n" +
			"    id: $freebase.id,\n" +
			"    firstName: $freebase.name[0]\n" +
			"  }\n" +
			"];\n" +
			"write $person to '" + this.politician.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("politician.json",
			createObjectNode("id", 1, "firstName", "Andrew"));
	}
	
	@Test
	public void testTakeAllObjects() throws IOException {

		String query = "using cleansing;\n" +
			"$freebase = read from '" + this.freebase.toURI() + "';\n" +
			"$person = transform records $freebase\n" +
			"into [\n" +
			"  entity $person identified by $person.id with{\n" +
			"    id: $freebase.id,\n" +
			"    firstName: $freebase.name[0],\n" +
			"    worksFor: [{title: $freebase.positions.title,"
			+ "				sessions: $freebase.positions.session}]\n" +
			"  }\n" +
			"];\n" +
			"write $person to '" + this.politician.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("politician.json",
			createObjectNode("id", 1, "firstName", "Andrew"));
	}
}