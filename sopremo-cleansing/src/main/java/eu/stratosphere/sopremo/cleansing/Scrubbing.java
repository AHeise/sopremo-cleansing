package eu.stratosphere.sopremo.cleansing;

import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.fusion.UnresolvableEvaluationException;
import eu.stratosphere.sopremo.cleansing.scrubbing.DefaultRuleFactory;
import eu.stratosphere.sopremo.cleansing.scrubbing.ExpressionRewriter;
import eu.stratosphere.sopremo.cleansing.scrubbing.RuleManager;
import eu.stratosphere.sopremo.cleansing.scrubbing.ValidationContext;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.SingletonExpression;
import eu.stratosphere.sopremo.function.SimpleMacro;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;

@Name(verb = "scrub")
public class Scrubbing extends ElementaryOperator<Scrubbing> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3979039050900230817L;

	private RuleManager ruleManager = new RuleManager();

	public static final EvaluationExpression CONTEXT_NODE = new SingletonExpression("<context>") {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3340948936846733311L;

		@Override
		public IJsonNode evaluate(final IJsonNode node, final EvaluationContext context) {
			return ((ValidationContext) context).getContextNode();
		}

		@Override
		protected Object readResolve() {
			return CONTEXT_NODE;
		}
	};

	private final static DefaultRuleFactory RuleFactory = new DefaultRuleFactory();

	{
		RuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(MethodPointerExpression.class),
			new SimpleMacro<MethodPointerExpression>() {
				private static final long serialVersionUID = -8260133422163585840L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(MethodPointerExpression inputExpr, EvaluationContext context) {
					return new FunctionCall(inputExpr.getFunctionName(), context, new InputSelection(0));
				}
			});
//		RuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(InputSelection.class),
//			new SimpleMacro<InputSelection>() {
//				private static final long serialVersionUID = 111389216483477521L;
//
//				/*
//				 * (non-Javadoc)
//				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
//				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
//				 */
//				@Override
//				public EvaluationExpression call(InputSelection inputExpr, EvaluationContext context) {
//					if (inputExpr.hasTag(JsonStreamExpression.THIS_CONTEXT))
//						return EvaluationExpression.VALUE;
//					return CONTEXT_NODE;
//				}
//			});
	}

	public void addRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.ruleManager.addRule(rule, target);
	}

	public void addRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.ruleManager.addRule(rule, target);
	}

	public void removeRule(EvaluationExpression rule, List<EvaluationExpression> target) {
		this.ruleManager.removeRule(rule, target);
	}

	public void removeRule(EvaluationExpression rule, EvaluationExpression... target) {
		this.ruleManager.removeRule(rule, target);
	}

	@Override
	public PactModule asPactModule(final EvaluationContext context) {
		if (this.ruleManager.isEmpty()) {
			final PactModule pactModule = new PactModule(this.getName(), 1, 1);
			pactModule.getOutput(0).setInput(pactModule.getInput(0));
			return pactModule;
		}
		return super.asPactModule(context);
	}

	@Property
	@Name(preposition = "with")
	public void setRuleExpression(ObjectCreation ruleExpression) {
		this.ruleManager.parse(ruleExpression, this, RuleFactory);
		System.out.println(this.ruleManager);
	}

	public Scrubbing withRuleExpression(ObjectCreation ruleExpression) {
		setRuleExpression(ruleExpression);
		return this;
	}

	public ObjectCreation getRuleExpression() {
		return (ObjectCreation) this.ruleManager.getLastParsedExpression();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ruleManager.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		Scrubbing other = (Scrubbing) obj;
		return ruleManager.equals(other.ruleManager);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.Operator#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		super.toString(builder);
		builder.append(" with rules: ").append(this.ruleManager);
	}

	public static class Implementation extends
			SopremoMap<IJsonNode, IJsonNode, IJsonNode, IJsonNode> {
		private RuleManager ruleManager = new RuleManager();

		private transient ValidationContext context;

		@Override
		public void configure(final Configuration parameters) {
			super.configure(parameters);

			this.context = new ValidationContext(this.getContext());
		}

		@Override
		protected void map(final IJsonNode key, IJsonNode value, final JsonCollector out) {
			try {
				this.context.setContextNode(value);

				for (final Entry<PathExpression, EvaluationExpression> rulePath : this.ruleManager.getRules()) {
					final List<EvaluationExpression> targetPath = rulePath.getKey().getFragments();
					final EvaluationExpression rule = rulePath.getValue();

					if (targetPath.isEmpty())
						value = rule.evaluate(value, this.context);
					else {
						IJsonNode parent = value;
						final int lastIndex = targetPath.size() - 1;
						for (int index = 0; index < lastIndex; index++)
							parent = targetPath.get(index).evaluate(parent, this.context);

						final EvaluationExpression lastSegment = targetPath.get(lastIndex);

						final IJsonNode validationValue = lastSegment.evaluate(value, this.context);
						IJsonNode newValue = rule.evaluate(validationValue, this.context);
						if (validationValue != newValue)
							lastSegment.set(parent, newValue, this.context);
					}
				}
				out.collect(key, value);
			} catch (final UnresolvableEvaluationException e) {
				// do not emit invalid record
				if (SopremoUtil.LOG.isDebugEnabled())
					SopremoUtil.LOG.debug(String.format("Cannot fix validation rule for tuple %s: %s", value,
						e.getMessage()));
			}
		}
	}
}
