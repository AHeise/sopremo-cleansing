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

import static eu.stratosphere.sopremo.type.JsonUtil.createObjectNode;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class SpendingLikeMappingIT extends MeteorIT {
	private File spending, funds, legalEntity;

	@Before
	public void createInput() throws IOException {
		this.spending = this.testServer.createFile("spending.json",
			createObjectNode("spendingId", 1, "vendorname", "CompanyA"),
			createObjectNode("spendingId", 2, "vendorname", "CompanyA"));
		
		this.funds = this.testServer.getOutputFile("funds.json");
		this.legalEntity = this.testServer.getOutputFile("legalEntity.json");
	}

	@Test
	public void testSimpleForeignKeys() throws IOException {

		String query = "using cleansing;\n" +
			"$spendings = read from '" + this.spending.toURI() + "';\n" +
			"$funds, $legalEntity = transform records $spendings\n" +
			"into [\n" +
			"  entity $funds with{\n" +
			"    originalId: $spendings.spendingId,\n" +
			"    recipient: $legalEntity.id,\n" +
			"  }\n," +
			"  entity $legalEntity identified by $legalEntity.name with {\n" +
			"    name: $spendings.vendorname,\n" +
			"  }\n" +
			"];\n" +
			"write $funds to '" + this.funds.toURI() + "';\n" +
			"write $legalEntity to '" + this.legalEntity.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);
		Assert.assertNotNull(this.client.submit(plan, null, true));

		this.testServer.checkContentsOf("funds.json",
			createObjectNode("id", 1, "originalId", 1, "recipient","CompanyA"),
			createObjectNode("id", 2, "originalId", 2, "recipient","CompanyA"));

		this.testServer.checkContentsOf("legalEntity.json",
			createObjectNode("id", "CompanyA", "name", "CompanyA"));
	}
}