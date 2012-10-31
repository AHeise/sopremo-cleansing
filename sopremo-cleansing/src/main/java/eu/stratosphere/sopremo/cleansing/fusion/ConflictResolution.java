package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class ConflictResolution extends CleansingRule<FusionContext> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5841402171573265477L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule#evaluateRule
	 * (eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public IJsonNode evaluateRule(IJsonNode values, IJsonNode target, FusionContext context) {
		this.fuse((IArrayNode) values, context.getWeights(), context);
		return values;
	}

	public abstract void fuse(IArrayNode values, double[] weights, FusionContext context);
}
