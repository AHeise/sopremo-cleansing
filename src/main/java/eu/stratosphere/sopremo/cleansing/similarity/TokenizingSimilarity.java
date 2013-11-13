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
package eu.stratosphere.sopremo.cleansing.similarity;

import java.io.IOException;
import java.io.ObjectInputStream;

import eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity;
import eu.stratosphere.sopremo.tokenizer.DelimiterTokenizer;
import eu.stratosphere.sopremo.tokenizer.Tokenizer;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class TokenizingSimilarity extends TextSimilarity {
	private Similarity<IArrayNode<TextNode>> innerSimilarity;

	private Tokenizer tokenizer = DelimiterTokenizer.WHITESPACES;

	private transient CachingArrayNode<TextNode> tokens1 = new CachingArrayNode<TextNode>(), tokens2 = new CachingArrayNode<TextNode>();

	public TokenizingSimilarity(Similarity<IArrayNode<TextNode>> innerSimilarity) {
		this.innerSimilarity = innerSimilarity;
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		this.tokens1 = new CachingArrayNode<TextNode>();
		this.tokens2 = new CachingArrayNode<TextNode>();
	}

	/**
	 * Returns the tokenizer.
	 * 
	 * @return the tokenizer
	 */
	public Tokenizer getTokenizer() {
		return this.tokenizer;
	}

	/**
	 * Sets the tokenizer to the specified value.
	 * 
	 * @param tokenizer
	 *        the tokenizer to set
	 */
	public void setTokenizer(Tokenizer tokenizer) {
		if (tokenizer == null)
			throw new NullPointerException("tokenizer must not be null");

		this.tokenizer = tokenizer;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.similarity.text.TextSimilarity#getSimilarity(java.lang.String,
	 * java.lang.String, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public float getSimilarity(CharSequence text1, CharSequence text2) {
		this.tokenizer.tokenizeInto(text1, this.tokens1);
		this.tokenizer.tokenizeInto(text2, this.tokens2);
		return this.innerSimilarity.getSimilarity(this.tokens1, this.tokens2);
	}
}
