package eu.stratosphere.sopremo.cleansing.fusion;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IArrayNode;

public class FusionContext extends EvaluationContext {
	private IArrayNode<?> contextNodes;

	private int[] sourceIndexes;

	private double[] weights;
	
	public FusionContext(){
		
	}

	public FusionContext(final EvaluationContext context) {
		super(context);
	}

	public IArrayNode<?> getContextNodes() {
		return this.contextNodes;
	}

	public int[] getSourceIndexes() {
		return this.sourceIndexes;
	}

	public double[] getWeights() {
		return this.weights;
	}

	public void setContextNodes(final IArrayNode<?> contextNodes) {
		if (contextNodes == null)
			throw new NullPointerException("contextNode must not be null");

		this.contextNodes = contextNodes;
	}

	public void setSourceIndexes(final int[] sourceIndexes) {
		if (sourceIndexes == null)
			throw new NullPointerException("sourceIndexes must not be null");

		this.sourceIndexes = sourceIndexes;
	}

	public void setWeights(final double[] weights) {
		if (weights == null)
			throw new NullPointerException("weights must not be null");

		this.weights = weights;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.AbstractSopremoType#clone()
	 */
	@Override
	public FusionContext clone() {
		return (FusionContext) super.clone();
	}

}
