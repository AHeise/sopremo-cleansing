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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.SopremoPlan;

public class ScrubbingComplexTest extends MeteorIT {

	@Test
	public void shouldScrubComplexObject() {
		SopremoPlan plan = this.getPlan();
		this.client.submit(plan, null, true);
		JsonParser parser;
		try {
			parser = new JsonParser(new FileReader(
					"src/test/resources/ScrubbingComplexExpected.json"));
			parser.setWrappingArraySkipping(true);
			this.testServer.checkContentsOf("ScrubbingComplexTest.json",
					parser.readValueAsTree());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (JsonParseException e) {
			e.printStackTrace();
			Assert.fail();
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	protected SopremoPlan getPlan() {
		return parseScript("using cleansing;"
				+ "$data = read from 'src/test/resources/ScrubbingComplexData.json';"

				+ "$data_scrubbed = scrub $data with rules {"

				// + "firstName: required"

				+ "	worksFor: {"
				+ "		name: notContainedIn([\"name_B\"]),"
				+ "		ceos: required"
				+ " }"
				+ "};"

				+ "write $data_scrubbed to 'file:///tmp/ScrubbingComplexTest.json';");
	}

}
