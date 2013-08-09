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
package eu.stratosphere.sopremo.cleansing;

import static eu.stratosphere.sopremo.testing.FunctionTest.assertReturn;
import static eu.stratosphere.sopremo.type.JsonUtil.createArrayNode;

import org.junit.Test;

import eu.stratosphere.sopremo.type.TextNode;

/**
 * Tests for StringUDFs
 */
public class StringUDFsTest {

	@Test
	public void shouldRemoveAllStringOccurences() {
		assertReturn(TextNode.valueOf("Foobar "),
				StringUDFs.REMOVE_ALL_STRINGS, "Foobar Corporation",
				createArrayNode("Corporation", "Corp", "Co"));
	}

}
