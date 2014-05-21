package eu.stratosphere.sopremo.cleansing.mapping;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.*;

@InputCardinality(min = 1, max = 1)
@OutputCardinality(1)
@Name(verb = "entity")
public class IdentifyOperator extends ElementaryOperator<IdentifyOperator> {
	{
		setRemoveTrivialInputSelections(false);
	}
	
	@Property(preferred = true)
	@Name(preposition = "identified by")
	public void setGroupingKey(final EvaluationExpression keyExpression) {
		this.setKeyExpressions(0, keyExpression);
	}
	
	public IdentifyOperator withGroupingKey(final EvaluationExpression keyExpression) {
		this.setGroupingKey(keyExpression);
		return this;
	}

	@Override
	@Property(preferred = true)
	@Name(preposition = "with")
	public void setResultProjection(final EvaluationExpression resultProjection) {
		super.setResultProjection(resultProjection);
	}

	public EvaluationExpression getGroupingKey() {
		return this.getKeyExpressions(0).isEmpty() ? EvaluationExpression.VALUE : this.getKeyExpressions(0).get(0);
	}
}
