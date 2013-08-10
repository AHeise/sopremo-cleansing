package eu.stratosphere.sopremo.cleansing;

import java.util.Collection;

import eu.stratosphere.sopremo.cleansing.scrubbing.ValidationRule;
import eu.stratosphere.sopremo.expressions.ChainedSegmentExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.type.IJsonNode;

public class ScrubbingSpecificChainedSegmentExpression extends
		ChainedSegmentExpression {
	public ScrubbingSpecificChainedSegmentExpression() {
		super();
	}

	public ScrubbingSpecificChainedSegmentExpression(
			Collection<EvaluationExpression> values) {
		super(values);
	}

	@Override
	protected IJsonNode evaluateSegment(final IJsonNode node) {
		IJsonNode result = node;
		for (EvaluationExpression expression : this.getExpressions()) {
			if (expression instanceof ValidationRule) {
				ValidationRule rule = (ValidationRule) expression;
				if (!rule.validate(result)) {
					return rule.fix(result);
				}
			} else if (expression instanceof FunctionCall) {
				result = expression.evaluate(result);
			}
		}
		return result;
	}
}
