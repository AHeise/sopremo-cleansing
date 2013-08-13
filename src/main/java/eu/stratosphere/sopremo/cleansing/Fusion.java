package eu.stratosphere.sopremo.cleansing;

import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.fusion.RuleBasedFusion;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ExpressionUtil;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * Input elements are either
 * <ul>
 * <li>Array of records resulting from record linkage without transitive
 * closure. [r1, r2, r3] with r<sub>i</sub>
 * <li>Array of record clusters resulting from record linkage with transitive
 * closure
 * </ul>
 * 
 * @author Arvid Heise
 */
@Name(verb = "fuse")
@InputCardinality(1)
@OutputCardinality(1)
public class Fusion extends CompositeOperator<Fusion> {
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((ruleBasedFusion == null) ? 0 : ruleBasedFusion.hashCode());
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
		Fusion other = (Fusion) obj;
		if (ruleBasedFusion == null) {
			if (other.ruleBasedFusion != null)
				return false;
		} else if (!ruleBasedFusion.equals(other.ruleBasedFusion))
			return false;
		return true;
	}

	private RuleBasedFusion ruleBasedFusion = new RuleBasedFusion();

	@Property
	@Name(preposition = "with resolutions")
	public void setResolutionExpression(ObjectCreation ruleExpression) {
		this.ruleBasedFusion.clear();
		this.parseRuleExpression(ruleExpression, EvaluationExpression.VALUE);
	}

	private void parseRuleExpression(ObjectCreation ruleExpression, PathSegmentExpression value) {
		final List<Mapping<?>> mappings = ruleExpression.getMappings();
		for (Mapping<?> mapping : mappings) {
			final EvaluationExpression expression = mapping.getExpression();
			final PathSegmentExpression path = ExpressionUtil.makePath(value, mapping.getTargetExpression());

			if (expression instanceof ArrayCreation) {
				for (EvaluationExpression nestedExpression : expression) {
					this.ruleBasedFusion.addResolution(nestedExpression, path);
				}
			} else {
				this.ruleBasedFusion.addResolution(expression, path);
			}
		}
	}

	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		this.ruleBasedFusion.addImplementation(module, context);
	}

}
