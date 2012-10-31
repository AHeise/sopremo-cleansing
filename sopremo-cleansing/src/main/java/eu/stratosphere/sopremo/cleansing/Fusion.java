package eu.stratosphere.sopremo.cleansing;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMaps;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import javassist.expr.MethodCall;

import org.mockito.internal.matchers.Equals;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.testing.Equaler;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.fusion.ConflictResolution;
import eu.stratosphere.sopremo.cleansing.fusion.FusionContext;
import eu.stratosphere.sopremo.cleansing.fusion.MergeRule;
import eu.stratosphere.sopremo.cleansing.fusion.UnresolvableEvaluationException;
import eu.stratosphere.sopremo.cleansing.fusion.UnresolvedNodes;
import eu.stratosphere.sopremo.cleansing.scrubbing.DefaultRuleFactory;
import eu.stratosphere.sopremo.cleansing.scrubbing.ExpressionRewriter;
import eu.stratosphere.sopremo.cleansing.scrubbing.RewriteContext;
import eu.stratosphere.sopremo.cleansing.scrubbing.RuleManager;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.SingletonExpression;
import eu.stratosphere.sopremo.function.SimpleMacro;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMap;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.util.CollectionUtil;

/**
 * Input elements are either
 * <ul>
 * <li>Array of records resulting from record linkage without transitive closure. [r1, r2, r3] with r<sub>i</sub>
 * <li>Array of record clusters resulting from record linkage with transitive closure
 * </ul>
 * 
 * @author Arvid Heise
 */
@Name(verb = "fuse")
public class Fusion extends CompositeOperator<Fusion> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8429199636646276642L;

	public static final EvaluationExpression CONTEXT_NODES = new SingletonExpression("<context>") {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3340948936846733311L;

		@Override
		public IJsonNode evaluate(final IJsonNode node, final EvaluationContext context) {
			return ((FusionContext) context).getContextNodes();
		}

		@Override
		protected Object readResolve() {
			return CONTEXT_NODES;
		}
	};

	private static final DefaultRuleFactory FusionRuleFactory = new DefaultRuleFactory(),
			UpdateRuleFactory = new DefaultRuleFactory(),
			WeightRuleFactory = new DefaultRuleFactory();

	{
		FusionRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(MethodPointerExpression.class),
			new SimpleMacro<MethodPointerExpression>() {
				private static final long serialVersionUID = -8260133422163585840L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(final MethodPointerExpression inputExpr,
						final EvaluationContext context) {
					return new MethodCall(inputExpr.getFunctionName(), EvaluationExpression.VALUE);
				}
			});
		FusionRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(InputSelection.class),
			new SimpleMacro<InputSelection>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(final InputSelection inputExpr, final EvaluationContext context) {
					if (inputExpr.hasTag(JsonStreamExpression.THIS_CONTEXT))
						return EvaluationExpression.VALUE;
					return CONTEXT_NODES;
				}
			});
		WeightRuleFactory.addRewriteRule(new ExpressionRewriter.ExpressionType(ArithmeticExpression.class),
			new SimpleMacro<ArithmeticExpression>() {
				private static final long serialVersionUID = 111389216483477521L;

				/*
				 * (non-Javadoc)
				 * @see eu.stratosphere.sopremo.function.SimpleMacro#call(eu.stratosphere.sopremo.expressions.
				 * EvaluationExpression, eu.stratosphere.sopremo.EvaluationContext)
				 */
				@Override
				public EvaluationExpression call(final ArithmeticExpression arithExpr, final EvaluationContext context) {
					if (arithExpr.getOperator() != ArithmeticOperator.MULTIPLICATION)
						throw new IllegalArgumentException("unsupported arithmetic expression");

					((RewriteContext) context).parse(arithExpr.getSecondOperand());
					return arithExpr.getFirstOperand();
				}
			});
	}

	private final List<Object2DoubleMap<PathExpression>> weights = new ArrayList<Object2DoubleMap<PathExpression>>();

	private final RuleManager fusionRules = new RuleManager(), updateRules = new RuleManager(),
			weightRules = new RuleManager();

	private boolean multipleRecordsPerSource = false;

	private EvaluationExpression defaultValueRule = MergeRule.INSTANCE;

	public EvaluationExpression getDefaultValueRule() {
		return this.defaultValueRule;
	}

	private Object2DoubleMap<PathExpression> getWeightMap(final int inputIndex) {
		CollectionUtil.ensureSize(this.weights, inputIndex + 1);
		Object2DoubleMap<PathExpression> weightMap = this.weights.get(inputIndex);
		if (weightMap == null) {
			while (this.weights.size() <= inputIndex)
				this.weights.add(null);
			this.weights.set(inputIndex, weightMap = new Object2DoubleOpenHashMap<PathExpression>());
			weightMap.defaultReturnValue(1);
		}
		return weightMap;
	}

	public double getWeights(final int inputIndex, final PathExpression path) {
		return this.getWeightMap(inputIndex).getDouble(path);
	}

	public boolean isMultipleRecordsPerSource() {
		return this.multipleRecordsPerSource;
	}

	public void addFusionRule(final EvaluationExpression rule, final List<EvaluationExpression> target) {
		this.fusionRules.addRule(rule, target);
	}

	public void addFusionRule(final EvaluationExpression rule, final EvaluationExpression... target) {
		this.fusionRules.addRule(rule, target);
	}

	public void removeFusionRule(final EvaluationExpression rule, final List<EvaluationExpression> target) {
		this.fusionRules.removeRule(rule, target);
	}

	public void removeFusionRule(final EvaluationExpression rule, final EvaluationExpression... target) {
		this.fusionRules.removeRule(rule, target);
	}

	public void addUpdateRule(final EvaluationExpression rule, final List<EvaluationExpression> target) {
		this.updateRules.addRule(rule, target);
	}

	public void addUpdateRule(final EvaluationExpression rule, final EvaluationExpression... target) {
		this.updateRules.addRule(rule, target);
	}

	public void removeUpdateRule(final EvaluationExpression rule, final List<EvaluationExpression> target) {
		this.updateRules.removeRule(rule, target);
	}

	public void removeUpdateRule(final EvaluationExpression rule, final EvaluationExpression... target) {
		this.updateRules.removeRule(rule, target);
	}

	@Property
	@Name(noun = "default")
	public void setDefaultValueRule(final EvaluationExpression defaultValueRule) {
		if (defaultValueRule == null)
			throw new NullPointerException("defaultValueRule must not be null");

		this.defaultValueRule = defaultValueRule;
	}

	public void setMultipleRecordsPerSource(final boolean multipleRecordsPerSource) {
		this.multipleRecordsPerSource = multipleRecordsPerSource;
	}

	public void setWeight(final double weight, final int inputIndex, final PathExpression path) {
		this.getWeightMap(inputIndex).put(path, weight);
	}

	@Property
	@Name(preposition = "into")
	public void setFusionExpression(final ObjectCreation ruleExpression) {
		this.fusionRules.parse(ruleExpression, this, FusionRuleFactory);
	}

	public ObjectCreation getFusionExpression() {
		return (ObjectCreation) this.fusionRules.getLastParsedExpression();
	}

	@Property
	@Name(preposition = "with weights")
	public void setWeightExpression(final ObjectCreation ruleExpression) {
		this.weightRules.parse(ruleExpression, this, WeightRuleFactory);

		for (final Entry<PathExpression, EvaluationExpression> rule : this.weightRules.getRules()) {
			final PathExpression path = rule.getKey();
			this.parseWeightRule(rule.getValue(), ((InputSelection) path.getFragment(0)).getIndex(),
				path.subPath(1, -1));
		}
	}

	private void parseWeightRule(final EvaluationExpression value, final int index, final PathExpression path) {
		if (value instanceof ArithmeticExpression) {
			this.setWeight(((NumericNode) ((ConstantExpression) ((ArithmeticExpression) value).getFirstOperand())
				.getConstant()).getDoubleValue(), index, path);
			this.parseWeightRule(((ArithmeticExpression) value).getSecondOperand(), index, new PathExpression(path));
		} else
			this.setWeight(((NumericNode) ((ConstantExpression) value).getConstant()).getDoubleValue(), index, path);
	}

	public ObjectCreation getWeightExpression() {
		return (ObjectCreation) this.weightRules.getLastParsedExpression();
	}

	@Property
	@Name(verb = "update")
	public void setUpdateExpression(final ObjectCreation ruleExpression) {
		this.updateRules.parse(ruleExpression, this, UpdateRuleFactory);
	}

	public ObjectCreation getUpdateExpression() {
		return (ObjectCreation) this.updateRules.getLastParsedExpression();
	}

	@Override
	public SopremoModule asElementaryOperators() {
		return SopremoModule.valueOf("fusion", new ValueFusion().withFusionRules(this.fusionRules));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.defaultValueRule.hashCode();
		result = prime * result + this.fusionRules.hashCode();
		result = prime * result + (this.multipleRecordsPerSource ? 1231 : 1237);
		result = prime * result + this.updateRules.hashCode();
		result = prime * result + this.weights.hashCode();
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
		final Fusion other = (Fusion) obj;
		return this.defaultValueRule.equals(other.defaultValueRule)
			&& this.fusionRules.equals(other.fusionRules)
			&& this.multipleRecordsPerSource == other.multipleRecordsPerSource
			// && this.updateRules.equals(other.updateRules)
			&& this.weights.equals(other.weights)
			&& this.updateRules.customEquals(other.updateRules, new Equaler<PathExpression>() {
				@Override
				public boolean isEqual(final PathExpression value1, final PathExpression value2) {
					final Operator<?>.Output thisSource = ((JsonStreamExpression) value1.getFragment(0)).getStream()
						.getSource();
					final Operator<?>.Output otherSource = ((JsonStreamExpression) value2.getFragment(0)).getStream()
						.getSource();
					return thisSource.getIndex() == otherSource.getIndex()
						&& Equals.nonRecursiveEquals(thisSource.getOperator(), otherSource.getOperator(), Fusion.this, other);
				}
			}, Equals.SafeEquals);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.Operator#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(final StringBuilder builder) {
		super.toString(builder);
		builder.append(" ").append(this.fusionRules).
			append(" default ").append(this.defaultValueRule).
			append(" with weights ").append(this.weights).
			append(" update ").append(this.updateRules);
	}
}
