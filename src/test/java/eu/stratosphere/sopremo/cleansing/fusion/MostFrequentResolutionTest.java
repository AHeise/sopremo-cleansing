package eu.stratosphere.sopremo.cleansing.fusion;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

@RunWith(Parameterized.class)
public class MostFrequentResolutionTest {
	private final IArrayNode<IJsonNode> nodes;
	private final Map<String, CompositeEvidence> weights;
	private final TextNode expected;
	private final MostFrequentResolution mostFrequentResolution = new MostFrequentResolution();

	public MostFrequentResolutionTest(IArrayNode<IJsonNode> nodes, Map<String, CompositeEvidence> weights, TextNode expected) {
		this.nodes = nodes;
		this.weights = weights;
		this.expected = expected;
	}

	// @Test
	// public void testMostFrequentResolutionWithoutWeights() {
	// MostFrequentResolution mostFrequentResolution = new
	// MostFrequentResolution();
	//
	// ObjectNode johnNode = new ObjectNode(), jNode = new ObjectNode(),
	// billNode = new ObjectNode();
	// johnNode.put("_source", TextNode.valueOf("1"));
	// jNode.put("_source", TextNode.valueOf("2"));
	// billNode.put("_source", TextNode.valueOf("3"));
	// TextNode john = new TextNode("John"), j = new TextNode("J."), bill = new
	// TextNode("Bill");
	// johnNode.put("_value", john);
	// jNode.put("_value", j);
	// billNode.put("_value", bill);
	//
	// Map<String, CompositeEvidence> weights = new HashMap<String,
	// CompositeEvidence>();
	// // weights.put("1", new
	// // CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.8))));
	// // weights.put("2", new
	// // CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.7))));
	// // weights.put("3", new
	// // CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.9))));
	//
	// ArrayNode<IJsonNode> choices = new ArrayNode<IJsonNode>(johnNode, jNode,
	// billNode);
	//
	// mostFrequentResolution.fuse(choices, weights);
	//
	// Assert.assertEquals(new ArrayNode<TextNode>(john), choices);
	// }

	@Test
	public void testMostFrequentResolution() {
		mostFrequentResolution.fuse(nodes, weights);
		assertEquals(new ArrayNode<TextNode>(expected), nodes);
	}

	@Parameterized.Parameters
	public static Collection<Object[]> primeNumbers() {
		return Arrays.asList(withoutWeights(), withWeights());
	}

	private static Object[] withWeights() {
		ObjectNode johnNode = new ObjectNode(), jNode = new ObjectNode(), billNode = new ObjectNode();
		
		johnNode.put("_source", TextNode.valueOf("1"));
		jNode.put("_source", TextNode.valueOf("2"));
		billNode.put("_source", TextNode.valueOf("3"));
		
		TextNode john = new TextNode("John"), j = new TextNode("John"), bill = new TextNode("Bill");
		
		johnNode.put("_value", john);
		jNode.put("_value", j);
		billNode.put("_value", bill);

		Map<String, CompositeEvidence> weights = new HashMap<String, CompositeEvidence>();
		weights.put("1", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.1))));
		weights.put("2", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.7))));
		weights.put("3", new CompositeEvidence(DecimalNode.valueOf(BigDecimal.valueOf(0.9))));

		ArrayNode<IJsonNode> choices = new ArrayNode<IJsonNode>(johnNode, jNode, billNode);
		
		return new Object[]{ choices, weights, bill };
	}
	
	private static Object[] withoutWeights() {
		ObjectNode johnNode = new ObjectNode(), jNode = new ObjectNode(), billNode = new ObjectNode();
		
		johnNode.put("_source", TextNode.valueOf("1"));
		jNode.put("_source", TextNode.valueOf("2"));
		billNode.put("_source", TextNode.valueOf("3"));
		
		TextNode john = new TextNode("John"), j = new TextNode("John"), bill = new TextNode("Bill");
		
		johnNode.put("_value", john);
		jNode.put("_value", j);
		billNode.put("_value", bill);

		Map<String, CompositeEvidence> weights = new HashMap<String, CompositeEvidence>();

		ArrayNode<IJsonNode> choices = new ArrayNode<IJsonNode>(johnNode, jNode, billNode);
		
		return new Object[]{ choices, weights, john };
	}
}
