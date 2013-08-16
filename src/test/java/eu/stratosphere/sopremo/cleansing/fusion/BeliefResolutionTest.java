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
package eu.stratosphere.sopremo.cleansing.fusion;

import it.unimi.dsi.fastutil.objects.Object2DoubleMap;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.sopremo.cleansing.fusion.BeliefResolution.BeliefMassFunction;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.CharSequenceUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Arvid Heise
 */
public class BeliefResolutionTest {
	public class AbbrExpression extends EvaluationExpression {

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
		 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public IJsonNode evaluate(IJsonNode node) {
			CharSequence value1 = ((TextNode) ((IArrayNode<?>) node).get(0));
			CharSequence value2 = ((TextNode) ((IArrayNode<?>) node).get(1));
			if(value2.length() < value1.length())
				return BooleanNode.FALSE;
			final int lastPos = value1.length() - 1;
			return BooleanNode.valueOf( value1.charAt(lastPos) == '.' &&
				CharSequenceUtil.regionMatches(value1, 0, value2, 0, lastPos, false));
		}
	}

	@Test
	public void testMassCombination() {
		BeliefResolution beliefResolution = new BeliefResolution(new AbbrExpression());
		ObjectNode johnNode = new ObjectNode(), jNode = new ObjectNode(), billNode = new ObjectNode();
		johnNode.put("_source", TextNode.valueOf("1"));
		jNode.put("_source", TextNode.valueOf("2"));
		billNode.put("_source", TextNode.valueOf("3"));
		TextNode john = new TextNode("John"), j = new TextNode("J."), bill = new TextNode("Bill");
		johnNode.put("_value", john);
		jNode.put("_value", j);
		billNode.put("_value", bill);
		Map<String, CompositeEvidence> weights = new HashMap<String, CompositeEvidence>();
		weights.put("1", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.8))));
		weights.put("2", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.7))));
		weights.put("3", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.9))));
		BeliefMassFunction massFunction = beliefResolution.getFinalMassFunction(
			new ArrayNode<IJsonNode>(johnNode, jNode, billNode),
			weights);

		Object2DoubleMap<IJsonNode> valueMasses = massFunction.getValueMasses();
		Assert.assertEquals(0.52, valueMasses.getDouble(john), 0.01);
		Assert.assertEquals(0.09, valueMasses.getDouble(j), 0.01);
		Assert.assertEquals(0.35, valueMasses.getDouble(bill), 0.01);
	}

	@Test
	public void testBeliefResolution() {
		BeliefResolution beliefResolution = new BeliefResolution(new AbbrExpression());

		ObjectNode johnNode = new ObjectNode(), jNode = new ObjectNode(), billNode = new ObjectNode();
		johnNode.put("_source", TextNode.valueOf("1"));
		jNode.put("_source", TextNode.valueOf("2"));
		billNode.put("_source", TextNode.valueOf("3"));
		TextNode john = new TextNode("John"), j = new TextNode("J."), bill = new TextNode("Bill");
		johnNode.put("_value", john);
		jNode.put("_value", j);
		billNode.put("_value", bill);
		ArrayNode<IJsonNode> choices = new ArrayNode<IJsonNode>(johnNode, jNode, billNode);
		Map<String, CompositeEvidence> weights = new HashMap<String, CompositeEvidence>();
		weights.put("1", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.8))));
		weights.put("2", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.7))));
		weights.put("3", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.9))));
		beliefResolution.fuse(choices, weights);

		Assert.assertEquals(new ArrayNode<TextNode>(john), choices);
	}
}
