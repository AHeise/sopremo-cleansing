package eu.stratosphere.sopremo.cleansing;

import java.io.IOException;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.scrubbing.RuleBasedScrubbing;
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

@Name(verb = "scrub")
@InputCardinality(1)
@OutputCardinality(1)
public class Scrubbing extends CompositeOperator<Scrubbing> {
	private RuleBasedScrubbing ruleBasedScrubbing = new RuleBasedScrubbing();

	@Property
	@Name(preposition = "with rules")
	public void setRuleExpression(ObjectCreation ruleExpression) {
		this.ruleBasedScrubbing.clear();
		this.parseRuleExpression(ruleExpression, EvaluationExpression.VALUE);
	}

	private void parseRuleExpression(ObjectCreation ruleExpression, PathSegmentExpression value) {
		final List<Mapping<?>> mappings = ruleExpression.getMappings();
		for (Mapping<?> mapping : mappings) {
			final EvaluationExpression expression = mapping.getExpression();
			final PathSegmentExpression path = ExpressionUtil.makePath(value, mapping.getTargetExpression());
			if (expression instanceof ObjectCreation)
				this.parseRuleExpression((ObjectCreation) expression, path);
			else if (expression instanceof ArrayCreation){
				for (EvaluationExpression partial : expression)
					this.ruleBasedScrubbing.addRule(partial, path);
			} else{
				this.ruleBasedScrubbing.addRule(expression, path);
			}
		}
	}

	public void addRule(EvaluationExpression ruleExpression, PathSegmentExpression target) {
		this.ruleBasedScrubbing.addRule(ruleExpression, target);
	}

	public void removeRule(EvaluationExpression ruleExpression, PathSegmentExpression target) {
		this.ruleBasedScrubbing.removeRule(ruleExpression, target);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(eu.stratosphere.sopremo.operator.SopremoModule
	 * , eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
		this.ruleBasedScrubbing.addImplementation(module, context);
	}

	public Scrubbing withRuleExpression(ObjectCreation ruleExpression) {
		this.setRuleExpression(ruleExpression);
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.ruleBasedScrubbing.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		Scrubbing other = (Scrubbing) obj;
		return this.ruleBasedScrubbing.equals(other.ruleBasedScrubbing);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.ElementaryOperator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		this.ruleBasedScrubbing.appendAsString(appendable);
	}
}
