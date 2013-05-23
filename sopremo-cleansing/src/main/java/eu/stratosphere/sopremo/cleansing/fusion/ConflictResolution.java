package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class ConflictResolution extends CleansingRule<FusionContext> {

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule#evaluateRule
	 * (eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public IJsonNode evaluate(IJsonNode values) {
		this.fuse((IArrayNode<IJsonNode>) values, this.getContext().getWeights());
		return values;
	}

	public abstract void fuse(IArrayNode<IJsonNode> values, double[] weights);
}
