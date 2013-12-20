package eu.stratosphere.sopremo.cleansing.mapping;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.util.CollectionUtil;

@InputCardinality(min = 1, max = 1)
@OutputCardinality(1)
@Name(verb = "identified")
public class IdentifyOperator extends ElementaryOperator<IdentifyOperator>{

	private final List<EvaluationExpression> keyExpressions = new ArrayList<EvaluationExpression>(1);
	private EvaluationExpression resultProjection = EvaluationExpression.VALUE;
	
	@Property(preferred = true, input = true)
	@Name(preposition = "by")
	public void setGroupingKey(final int inputIndex, final EvaluationExpression keyExpression) {
		CollectionUtil.ensureSize(this.keyExpressions, inputIndex + 1);
		this.keyExpressions.set(inputIndex, keyExpression);
	}
	
	@Property(preferred = true)
	@Name(preposition = "into")
	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}
	
	public EvaluationExpression getKeyExpression(final int index) {
		return keyExpressions.get(index);
	}

	public EvaluationExpression getResultProjection() {
		return resultProjection;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((keyExpressions == null) ? 0 : keyExpressions.hashCode());
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
		if (keyExpressions == null) {
			if (other.keyExpressions != null)
				return false;
		} else if (!keyExpressions.equals(other.keyExpressions))
			return false;
		if (resultProjection == null) {
			if (other.resultProjection != null)
				return false;
		} else if (!resultProjection.equals(other.resultProjection))
			return false;
		return true;
	}

	
}
