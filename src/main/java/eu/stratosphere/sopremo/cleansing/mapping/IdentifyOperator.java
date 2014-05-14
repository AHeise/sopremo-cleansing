package eu.stratosphere.sopremo.cleansing.mapping;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.*;

@InputCardinality(min = 1, max = 1)
@OutputCardinality(1)
@Name(verb = "entity")
public class IdentifyOperator extends ElementaryOperator<IdentifyOperator> {

	private EvaluationExpression primaryKeyExpression = null;

	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;

	@Property(preferred = true)
	@Name(preposition = "identified by")
	public void setGroupingKey(final EvaluationExpression keyExpression) {
		this.primaryKeyExpression = keyExpression;
	}

	@Override
	@Property(preferred = true)
	@Name(preposition = "with")
	public void setResultProjection(final EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	public EvaluationExpression getKeyExpression() {
		return this.primaryKeyExpression;
	}

	@Override
	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (this.primaryKeyExpression == null ? 0 : this.primaryKeyExpression.hashCode());
		result = prime * result + (this.resultProjection == null ? 0 : this.resultProjection.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final IdentifyOperator other = (IdentifyOperator) obj;
		if (this.primaryKeyExpression == null) {
			if (other.primaryKeyExpression != null)
				return false;
		} else if (!this.primaryKeyExpression.equals(other.primaryKeyExpression))
			return false;
		if (this.resultProjection == null) {
			if (other.resultProjection != null)
				return false;
		} else if (!this.resultProjection.equals(other.resultProjection))
			return false;
		return true;
	}

}
