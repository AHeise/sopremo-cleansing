package eu.stratosphere.sopremo.cleansing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.cleansing.fusion.CompositeEvidence;
import eu.stratosphere.sopremo.cleansing.fusion.ResolutionBasedFusion;
import eu.stratosphere.sopremo.cleansing.fusion.SingleOutputResolution;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ExpressionUtil;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.NullNode;

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
@InputCardinality(1)
@OutputCardinality(1)
public class Fusion extends CompositeOperator<Fusion> {

	private static String SINGLE_OUTPUT_WARNING =
		"Last resolution of field '%s' (%s) can yield to multiple outputs.\nEither you've chosen this behavior explicitly or you should choose an additional resolution that enforces a single output value.";

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.resolutionBasedFusion.hashCode();
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
		return this.resolutionBasedFusion.equals(other.resolutionBasedFusion);
	}

	private ResolutionBasedFusion resolutionBasedFusion = new ResolutionBasedFusion();

	@Property
	@Name(preposition = "with weights")
	public void setWeightsExpression(ObjectCreation weightsExpression) {
		this.parseWeightsExpression(weightsExpression,
			EvaluationExpression.VALUE);
	}

	@Property
	@Name(preposition = "with resolutions")
	public void setResolutionExpression(ObjectCreation ruleExpression) {
		this.resolutionBasedFusion.clear();
		this.parseResolutionExpression(ruleExpression,
			EvaluationExpression.VALUE);
	}

	private void parseWeightsExpression(ObjectCreation weightsExpression,
			PathSegmentExpression value) {
		Map<String, CompositeEvidence> weights = new HashMap<String, CompositeEvidence>();
		this.traverseWeightExpressions(weights, weightsExpression);
		this.resolutionBasedFusion.setWeights(weights);
	}

	private void traverseWeightExpressions(
			Map<String, CompositeEvidence> weights,
			ObjectCreation weightsExpression) {
		for (Mapping<?> mapping : weightsExpression.getMappings()) {
			weights.put(this.getFieldName(mapping),
				this.createEvidence(mapping.getExpression()));
		}
	}

	public CompositeEvidence createEvidence(EvaluationExpression expr) {
		if (expr instanceof ArithmeticExpression) {
			CompositeEvidence evidence;
			ArithmeticExpression arithmeticExpr = (ArithmeticExpression) expr;
			ObjectCreation objectCreation;
			EvaluationExpression evaluationExpression;
			if (arithmeticExpr.getFirstOperand() instanceof ObjectCreation) {
				objectCreation = (ObjectCreation) arithmeticExpr.getFirstOperand();
				evaluationExpression = arithmeticExpr.getSecondOperand();
			} else {
				objectCreation = (ObjectCreation) arithmeticExpr.getSecondOperand();
				evaluationExpression = arithmeticExpr.getFirstOperand();
			}
			DecimalNode baseEvidence = (DecimalNode) evaluationExpression.evaluate(NullNode.getInstance());
			evidence = new CompositeEvidence(baseEvidence);
			for (Mapping<?> mapping : objectCreation.getMappings()) {
				evidence.putEvidence(this.getFieldName(mapping),
					this.createEvidence(mapping.getExpression()));
			}
			return evidence;
		}
		return new CompositeEvidence((DecimalNode) expr.evaluate(NullNode.getInstance()));
	}

	private String getFieldName(Mapping<?> mapping) {
		return ((ObjectAccess) mapping.getTargetExpression().getLast())
			.getField();
	}

	private void parseResolutionExpression(ObjectCreation ruleExpression,
			PathSegmentExpression value) {
		final List<Mapping<?>> mappings = ruleExpression.getMappings();
		for (Mapping<?> mapping : mappings) {
			final EvaluationExpression expression = mapping.getExpression();
			final PathSegmentExpression path = ExpressionUtil.makePath(value,
				mapping.getTargetExpression());

			if (expression instanceof ArrayCreation) {
				int expressionSize = ((ArrayCreation) expression).size();
				int currentExpression = 0;
				for (EvaluationExpression nestedExpression : expression) {
					if (expressionSize == ++currentExpression)
						this.checkAnnotationAndShowWarning(path,
							nestedExpression);
					this.resolutionBasedFusion.addResolution(nestedExpression,
						path);
				}
			} else {
				this.checkAnnotationAndShowWarning(path, expression);
				this.resolutionBasedFusion.addResolution(expression, path);
			}
		}
	}

	private void checkAnnotationAndShowWarning(PathSegmentExpression path,
			EvaluationExpression expr) {
		SingleOutputResolution annotation = expr.getClass().getAnnotation(SingleOutputResolution.class);
		if (annotation == null) {
			this.showWarning(path, expr);
		}
	}

	private void showWarning(PathSegmentExpression path, EvaluationExpression expr) {
		String scriptName =
			SopremoEnvironment.getInstance().getEvaluationContext().getConstantRegistry().getName(
				expr.getClass().getAnnotation(Name.class));

		SopremoUtil.LOG.warn(String.format(Fusion.SINGLE_OUTPUT_WARNING,
			path.toString(), scriptName));
	}

	@Override
	public void addImplementation(SopremoModule module,
			EvaluationContext context) {
		this.resolutionBasedFusion.addImplementation(module, context);
	}

}
