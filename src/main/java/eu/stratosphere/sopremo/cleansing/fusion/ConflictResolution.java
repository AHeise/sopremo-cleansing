package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.cleansing.scrubbing.CleansingRule;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

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
		this.fuse((IArrayNode<IJsonNode>) values, this.getFusionContext(values).getWeights());
		return values;
	}
	
	private FusionContext getFusionContext(IJsonNode values) {
		// TODO: FIXME -> Generic getContext() should return FusionContext BUT returns normal EvaluationContext
		
		//return this.getContext();
		FusionContext fContext = new FusionContext(this.getContext());
		double[] weights = new double[((IArrayNode<IJsonNode>) values).size()];
		for (int i = 0; i < weights.length; i++) {
			weights[i] = 1;
		}
		fContext.setWeights(weights);
		return fContext;
	}

	public abstract void fuse(IArrayNode<IJsonNode> values, double[] weights);
}
