package eu.stratosphere.sopremo.cleansing;

import java.util.Collection;

import eu.stratosphere.sopremo.expressions.ChainedSegmentExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class FusionSpecificChainedSegmentExpression extends
		ChainedSegmentExpression {
	public FusionSpecificChainedSegmentExpression() {
		super();
	}

	public FusionSpecificChainedSegmentExpression(
			Collection<EvaluationExpression> values) {
		super(values);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected IJsonNode evaluateSegment(final IJsonNode node) {
		IJsonNode result = node;
		for (EvaluationExpression expression : this.getExpressions()) {
			if (((IArrayNode<IJsonNode>) result).size() <= 1)
				break;
			result = expression.evaluate(result);
		}
		return result;
	}
}
