package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public abstract class ConflictResolution extends CleansingRule<FusionContext> {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule#evaluateRule
	 * (eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public IJsonNode evaluate(IJsonNode values) {
		this.fuse((IArrayNode<IJsonNode>) values, this.getWeights(values));
		return values;
	}
	
	private Map<String, CompositeEvidence> getWeights(IJsonNode values) {
		EvaluationContext context = this.getContext();
		@SuppressWarnings("unchecked")
		Map<String, CompositeEvidence> weights = context.getParameter("weights", Map.class);
		if (weights == null) {
			weights = new HashMap<String, CompositeEvidence>();
			//TODO do we really have to initialize, since no given weight means weight of 1.0
//			for (int i = 0; i < weights.length; i++) {
//				weights[i] = 1;
//			}
		}
		return weights;
	}
	
	protected double getWeightForValue(IJsonNode value, Map<String, CompositeEvidence> weights) {
		String sourceKey = ((TextNode)((ObjectNode)value).get("_source")).toString();
		double evidence = weights.get(sourceKey).getBaseEvidence().getDoubleValue();
		return evidence;
	}
	
	protected IJsonNode getValueFromSourceTaggedObject(IJsonNode value) {
		return ((ObjectNode)value).get("_value");
	}

	public abstract void fuse(IArrayNode<IJsonNode> values, Map<String, CompositeEvidence> weights);
}
