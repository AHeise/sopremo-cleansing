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
package eu.stratosphere.sopremo.cleansing.similarity.text;

import eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public abstract class TextSimilarity extends AbstractSimilarity<TextNode> {
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.AbstractSimilarity#getExpectedType()
	 */
	@Override
	public final Class<TextNode> getExpectedType() {
		return TextNode.class;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.similarity.Similarity#getSimilarity(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public double getSimilarity(TextNode node1, TextNode node2) {
		if (node1.equals(node2))
			return 1;
		if (node1.length() == 0 || node2.length() == 0)
			return 0;
		return this.getSimilarity((CharSequence) node1, node2);
	}

	public abstract double getSimilarity(CharSequence text1, CharSequence text2);
}
