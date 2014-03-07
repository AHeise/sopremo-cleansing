package eu.stratosphere.sopremo.cleansing.mapping;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;

@InputCardinality(min = 1, max = 1)
@OutputCardinality(1)
@Name(verb = "entity")
public class IdentifyOperator extends ElementaryOperator<IdentifyOperator>{

	private EvaluationExpression primaryKeyExpression = null;
	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;
	
	@Property(preferred = true)
	@Name(preposition = "identified by")
	public void setGroupingKey(final EvaluationExpression keyExpression) {
		this.primaryKeyExpression = keyExpression;
	}
	
	@Property(preferred = true)
	@Name(preposition = "with")
	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}
	
	public EvaluationExpression getKeyExpression() {
		return primaryKeyExpression;
	}

	public EvaluationExpression getResultProjection() {
		return resultProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((primaryKeyExpression == null) ? 0 : primaryKeyExpression.hashCode());
		result = prime * result + ((resultProjection == null) ? 0 : resultProjection.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		IdentifyOperator other = (IdentifyOperator) obj;
		if (primaryKeyExpression == null) {
			if (other.primaryKeyExpression != null)
				return false;
		} else if (!primaryKeyExpression.equals(other.primaryKeyExpression))
			return false;
		if (resultProjection == null) {
			if (other.resultProjection != null)
				return false;
		} else if (!resultProjection.equals(other.resultProjection))
			return false;
		return true;
	}

	
}
