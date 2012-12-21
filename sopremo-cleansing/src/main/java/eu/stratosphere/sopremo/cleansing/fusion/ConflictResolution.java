package eu.stratosphere.sopremo.cleansing.fusion;

import java.io.IOException;

import eu.stratosphere.sopremo.SopremoRuntime;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public abstract class ConflictResolution<T extends IJsonNode> extends PathSegmentExpression {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5841402171573265477L;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode evaluate(IJsonNode node) {
		final IJsonNode result = this.getInputExpression().evaluate(node);
		@SuppressWarnings("unchecked")
		final IArrayNode<T> values = (IArrayNode<T>) result;
		if (values.size() <= 1)
			return values;
		return evaluateSegment(result);
	}

	protected double[] getWeights() {
		return ((FusionContext) SopremoRuntime.getInstance().getCurrentEvaluationContext()).getWeights();
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.PathSegmentExpression#evaluateSegment(eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	protected IArrayNode<T> evaluateSegment(IJsonNode node) {
		@SuppressWarnings("unchecked")
		final IArrayNode<T> values = (IArrayNode<T>) node;
		this.fuse(values);
		return values;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append(this.getClass().getSimpleName());
	}

	public abstract void fuse(IArrayNode<T> values);
}
