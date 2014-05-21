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
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.JsonUtil;

public class TransformRecordsNestedArrayIT extends MeteorIT {
	private File usCongressMembers, person;

	@Before
	public void createInput() throws IOException {
		this.usCongressMembers = this.testServer.createFile("usCongressMembers.json",
			createObjectNode("id", "1990-1994", "name", "Andrew Adams", "biography", 1, "party", "PBC"),
			createObjectNode("id", "1994-1998", "name", "Andrew Adams", "biography", 1, "party", "CDU"));
		this.person = this.testServer.getOutputFile("person.json");
	}

	@Test
	public void testNestedRelationsSupport() throws IOException {

		String query = "using cleansing;" +
			"$usCongressMembers = read from '" + this.usCongressMembers.toURI() + "';\n" +
			"$person = transform records $usCongressMembers\n" +
			"into [\n" +
			"  entity $usCongressMembers identified by $usCongressMembers.biography with {" +
			"    name: $usCongressMembers.name,\n" +
			"    worksFor: [{" +
			"	   LE: $usCongressMembers.party, startYear : split($usCongressMembers.id, '-')[0] as int," +
			"      endYear : split($usCongressMembers.id, '-')[1] as int" +
			"    }]" +
			" }" +
			"];\n" +
			"write $person to '" + this.person.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		SopremoUtil.trace();
		Assert.assertNotNull(this.client.submit(plan, null, true));

		ObjectCreation canonicalizer = new ObjectCreation();
		canonicalizer.addMapping(new ObjectCreation.CopyFields(EvaluationExpression.VALUE));
		canonicalizer.addMapping("worksFor",
			FunctionUtil.createFunctionCall(CoreFunctions.SORT, new ObjectAccess("worksFor")));
		this.testServer.checkContentsOf("person.json", canonicalizer,
			createObjectNode("id", 1, "name", "Andrew Adams", "worksFor", JsonUtil.createArrayNode(
				createObjectNode("LE", "PBC", "endYear", 1994, "startYear", 1990),
				createObjectNode("LE", "CDU", "endYear", 1998, "startYear", 1994))));
	}
}