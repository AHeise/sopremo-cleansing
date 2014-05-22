package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.cleansing.FilterRecord;
import eu.stratosphere.sopremo.cleansing.fusion.ValueTreeContains;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression.ValueExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.SymbolicAssignment;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Internal;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.tree.NodeHandler;
import eu.stratosphere.sopremo.tree.TreeHandler;

@InputCardinality(1)
@OutputCardinality(1)

@Internal
public class RuleBasedScrubbing extends CompositeOperator<RuleBasedScrubbing> {

	private List<SymbolicAssignment> rules = new ArrayList<SymbolicAssignment>();

	public void addRule(EvaluationExpression ruleExpression,
			PathSegmentExpression target) {
		this.rules.add(new SymbolicAssignment(target, ruleExpression));
	}

	public void removeRule(EvaluationExpression ruleExpression,
			PathSegmentExpression target) {
		this.rules.remove(new SymbolicAssignment(ruleExpression, target));
	}

	@Override
	public Operator<RuleBasedScrubbing> clone() {
		return super.clone();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator.SopremoModule ,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module) {
		if (this.rules.isEmpty()) {
			// short circuit
			module.getOutput(0).setInput(0, module.getInput(0));
			return;
		}

		for (SymbolicAssignment corr : this.rules) {
			ArrayList<EvaluationExpression> ruleList = new ArrayList<EvaluationExpression>();
			ruleList.add(corr.getExpression());
			PathSegmentExpression cse = new ScrubbingSpecificChainedSegmentExpression(ruleList).withTail(corr.getTarget());
			RuleBasedScrubbing.this.targetSchemaHandler.addToSchema(corr.getTarget(), cse);
		}
		
		Projection normalization = new Projection().withResultProjection(
				this.targetSchemaHandler.targetSchema).withInputs(module.getInput(0));
		Selection filterInvalid = new Selection().withCondition(
				new UnaryExpression(
						new ValueTreeContains(FilterRecord.Instance), true))
				.withInputs(normalization);
		module.getOutput(0).setInput(0, filterInvalid);
	}

//	private EvaluationExpression createResultProjection() {
//		// no nested rule
//		if (this.rules.size() == 1
//				&& this.rules.containsKey(EvaluationExpression.VALUE))
//			return new ScrubbingSpecificChainedSegmentExpression(
//					this.rules.values());
//
//		Queue<PathSegmentExpression> uncoveredPaths = new LinkedList<PathSegmentExpression>(
//				this.rules.keySet());
//
//		final ObjectCreation objectCreation = new ObjectCreation();
//		objectCreation.addMapping(new ObjectCreation.CopyFields(
//				EvaluationExpression.VALUE));
//		while (!uncoveredPaths.isEmpty()) {
//			final PathSegmentExpression path = uncoveredPaths.remove();
//			this.addToObjectCreation(
//					objectCreation,
//					path,
//					path,
//					new ScrubbingSpecificChainedSegmentExpression(this.rules
//							.get(path)).withTail(path));
//		}
//		return objectCreation;*/
//		return null;
//	}
//
//	/**
//	 * @param objectCreation
//	 * @param path
//	 * @param chainedSegmentExpression
//	 */
//	private void addToObjectCreation(ObjectCreation objectCreation,
//			PathSegmentExpression remainingPath,
//			PathSegmentExpression completePath,
//			PathSegmentExpression chainedSegmentExpression) {
//
//		final String field = ((ObjectAccess) remainingPath).getField();
//
//		for (int index = 0, size = objectCreation.getMappingSize(); index < size; index++) {
//			final Mapping<?> mapping = objectCreation.getMapping(index);
//			final PathSegmentExpression targetExpression = mapping
//					.getTargetExpression();
//			// if (targetExpression.equalsThisSeqment(targetExpression)) {
//			
//			// TODO add case for ObjectAccess to allow complex objects
//			// Testcase: ScrubbingComplexTest.java
//			if (remainingPath.getInputExpression() == EvaluationExpression.VALUE){
//				objectCreation.addMapping(new ObjectCreation.FieldAssignment(
//						field, chainedSegmentExpression));
//			}
//			else{
//				this.addToObjectCreation(objectCreation,
//						(PathSegmentExpression) remainingPath
//								.getInputExpression(), completePath,
//						chainedSegmentExpression);
//			}
//		}
//
//		if (remainingPath.getInputExpression() == EvaluationExpression.VALUE)
//			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field,
//					chainedSegmentExpression));
//		else {
//			final ObjectCreation subObject = new ObjectCreation();
//			PathSegmentExpression processedPath = EvaluationExpression.VALUE;
//			for (PathSegmentExpression segment = completePath; remainingPath != segment; segment = (PathSegmentExpression) segment
//					.getInputExpression())
//				processedPath = segment.cloneSegment().withTail(processedPath);
//			subObject.addMapping(new ObjectCreation.CopyFields(processedPath));
//			objectCreation.addMapping(new ObjectCreation.FieldAssignment(field,
//					subObject));
//			this.addToObjectCreation(subObject,
//					(PathSegmentExpression) remainingPath.getInputExpression(),
//					completePath, chainedSegmentExpression);
//		}
//
//	}
	
	private transient SchemaHandler targetSchemaHandler = new SchemaHandler();

	private class SchemaHandler extends TreeHandler<EvaluationExpression, EvaluationExpression, EvaluationExpression> {
		/**
		 * Initializes EntityMapping.SourceHandler.
		 */
		public SchemaHandler() {
			put(ObjectAccess.class, new NodeHandler<ObjectAccess, EvaluationExpression, EvaluationExpression>() {
				@Override
				public EvaluationExpression handle(ObjectAccess value, EvaluationExpression expected,
						TreeHandler<Object, EvaluationExpression, EvaluationExpression> treeHandler) {
					ObjectCreation oc = new ObjectCreation();
					oc.addMapping(new ObjectCreation.CopyFields(value.getInputExpression()));
					
					oc = (ObjectCreation)
						treeHandler.handle(value.getInputExpression(), oc);
					
					EvaluationExpression actualSourceSchema = oc.getExpression(value.getField());
					if (!conforms(actualSourceSchema, expected)){
						oc.addMapping(value.getField(), actualSourceSchema = expected);
					}else{
						if(oc.getExpression(value.getField()) instanceof ScrubbingSpecificChainedSegmentExpression){
							ScrubbingSpecificChainedSegmentExpression cse = (ScrubbingSpecificChainedSegmentExpression) oc.getExpression(value.getField());
							
							for(EvaluationExpression ee :((ScrubbingSpecificChainedSegmentExpression)expected).getExpressions()){
								cse.addExpression(ee);
							}
						}
					}
					return actualSourceSchema;
				}
			});
			put(ValueExpression.class, new NodeHandler<ValueExpression, EvaluationExpression, EvaluationExpression>() {
				@Override
				public EvaluationExpression handle(ValueExpression value, EvaluationExpression expected,
						TreeHandler<Object, EvaluationExpression, EvaluationExpression> treeHandler) {
					EvaluationExpression actualSourceSchema = RuleBasedScrubbing.this.targetSchemaHandler.targetSchema;
					if (!conforms(actualSourceSchema, expected)){
						actualSourceSchema = expected;
						RuleBasedScrubbing.this.targetSchemaHandler.targetSchema = actualSourceSchema;
					}
						
					return actualSourceSchema;
				}
			});
		}

		protected boolean conforms(EvaluationExpression actualSourceSchema, EvaluationExpression expectedType) {
			if (expectedType == null)
				return true;
			if (actualSourceSchema == null || actualSourceSchema == EvaluationExpression.VALUE)
				return false;
			if (actualSourceSchema.getClass() == expectedType.getClass())
				return true;
			throw new IllegalArgumentException("Incompatible source paths found");
		}

		private EvaluationExpression targetSchema;

		public void addToSchema(EvaluationExpression sourcePath, EvaluationExpression ruleExpression) {
			handle(sourcePath, ruleExpression);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((rules == null) ? 0 : rules.hashCode());
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
		RuleBasedScrubbing other = (RuleBasedScrubbing) obj;
		if (rules == null) {
			if (other.rules != null)
				return false;
		} else if (!rules.equals(other.rules))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.ElementaryOperator#appendAsString(java
	 * .lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		SopremoUtil.append(appendable, " with rules: ", this.rules);
	}

	/**
	 * 
	 */
	public void clear() {
		this.rules.clear();
	}
}
