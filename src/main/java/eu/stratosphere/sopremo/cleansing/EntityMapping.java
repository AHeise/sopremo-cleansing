package eu.stratosphere.sopremo.cleansing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.sopremo.cleansing.mapping.IdentifyOperator;
import eu.stratosphere.sopremo.cleansing.mapping.NodeHandler;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyMappingTransformation;
import eu.stratosphere.sopremo.cleansing.mapping.TreeHandler;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.expressions.NestedOperatorExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.FieldAssignment;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.ObjectCreation.SymbolicAssignment;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.util.CollectionUtil;
import eu.stratosphere.util.reflect.ReflectUtil;

@Name(noun = "map entities of")
@InputCardinality(min = 1)
@OutputCardinality(min = 1)
public class EntityMapping extends CompositeOperator<EntityMapping> {
	private Set<ObjectCreation.SymbolicAssignment> sourceFKs = new HashSet<ObjectCreation.SymbolicAssignment>(),
			targetFKs = new HashSet<ObjectCreation.SymbolicAssignment>();

	private transient ArrayCreation mappingExpression = new ArrayCreation();

	private transient BooleanExpression foreignKeyExpression = new AndExpression();

	private Set<ObjectCreation.SymbolicAssignment> sourceToValueCorrespondences = new HashSet<ObjectCreation.SymbolicAssignment>();

	private List<EvaluationExpression> sourceSchema = new ArrayList<EvaluationExpression>(),
			targetSchema = new ArrayList<EvaluationExpression>();

	/**
	 * Returns the mappingExpression.
	 * 
	 * @return the mappingExpression
	 */
	public ArrayCreation getMappingExpression() {
		return this.mappingExpression;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.sourceFKs.hashCode();
		result = prime * result + this.targetFKs.hashCode();
		result = prime * result + this.sourceToValueCorrespondences.hashCode();
		result = prime * result + this.sourceSchema.hashCode();
		result = prime * result + this.targetSchema.hashCode();
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
		EntityMapping other = (EntityMapping) obj;
		return this.sourceFKs.equals(other.sourceFKs) &&
			this.sourceFKs.equals(other.sourceFKs) &&
			this.sourceFKs.equals(other.sourceFKs) &&
			this.sourceFKs.equals(other.sourceFKs) &&
			this.mappingExpression.equals(other.mappingExpression);
	}

	@Property
	@Name(preposition = "where")
	public void setSourceFKs(final BooleanExpression assignment) {
		this.foreignKeyExpression = assignment;
		this.sourceFKs = SourceFKParser.convert(assignment);
	}

	private static SourceFKParser SourceFKParser = new SourceFKParser();

	private static class SourceFKParser extends TreeHandler<BooleanExpression> {
		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 */
		public SourceFKParser() {
			put(AndExpression.class, new NodeHandler<AndExpression>() {
				@Override
				public void handle(AndExpression value, TreeHandler<Object> treeHandler) {
					for (EvaluationExpression subExpr : value)
						treeHandler.handle(subExpr);
				}
			});
			put(ComparativeExpression.class, new NodeHandler<ComparativeExpression>() {
				@Override
				public void handle(ComparativeExpression value, TreeHandler<Object> treeHandler) {
					if (value.getBinaryOperator() != BinaryOperator.EQUAL)
						throw new IllegalArgumentException("Only == supported");
					SourceFKParser.this.foreignKeyExpressions.add(new ObjectCreation.SymbolicAssignment(value.getExpr1(), value.getExpr2()));
				}
			});
		}

		private Set<ObjectCreation.SymbolicAssignment> foreignKeyExpressions = new HashSet<ObjectCreation.SymbolicAssignment>();

		@SuppressWarnings("unchecked")
		public Set<ObjectCreation.SymbolicAssignment> convert(BooleanExpression value) {
			this.foreignKeyExpressions.clear();
			handle(value);
			return new HashSet<ObjectCreation.SymbolicAssignment>(this.foreignKeyExpressions);
		}
	}

	/**
	 * Mapping Task: value correspondences: mappings and grouping keys source
	 * and target schema: attributes in mappings and grouping keys target keys:
	 * grouping keys join conditions with foreign keys: where-clause and foreign
	 * keys
	 */
	@Property
	@Name(adjective = "into")
	public void setMappingExpression(final ArrayCreation mappingExpression) {
		this.mappingExpression = mappingExpression;

		this.sourceSchema.clear();
		CollectionUtil.ensureSize(this.sourceSchema, getNumInputs(), EvaluationExpression.VALUE);
		this.targetSchema.clear();
		CollectionUtil.ensureSize(this.targetSchema, getNumOutputs(), EvaluationExpression.VALUE);
		this.sourceToValueCorrespondences.clear();
		List<EvaluationExpression> elements = mappingExpression.getElements();
		for (EvaluationExpression targetAssignment : elements) {
			NestedOperatorExpression noe =
				cast(targetAssignment, NestedOperatorExpression.class, "Please specify suboperators with " + IdentifyOperator.class);
			final IdentifyOperator nestedOperator =
				cast(noe.getOperator(), IdentifyOperator.class, "Please specify suboperators with " + IdentifyOperator.class);
			JsonStream outVar = nestedOperator.getInput(0);
			int targetIndex = outVar.getSource().getIndex();
			this.targetHandler.process(nestedOperator.getResultProjection(), targetIndex);
		}

		System.out.println(this);
	}

	private transient TargetHandler targetHandler = new TargetHandler();

	private class TargetHandler extends TreeHandler<EvaluationExpression> {
		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 */
		public TargetHandler() {
			put(ObjectCreation.class, new NodeHandler<ObjectCreation>() {
				@Override
				public void handle(ObjectCreation value, TreeHandler<Object> treeHandler) {
					ObjectCreation expected = new ObjectCreation();
					for (Mapping<?> subExpr : value.getMappings()) {
						FieldAssignment assignment = cast(subExpr, ObjectCreation.FieldAssignment.class, "");
						TargetHandler.this.targetPath =
							new ObjectAccess(assignment.getTarget()).withInputExpression(TargetHandler.this.targetPath);						
						treeHandler.handle(assignment.getExpression());
						expected.addMapping(assignment.getTarget(), getExpectedTargetSchema());
						TargetHandler.this.targetPath = (PathSegmentExpression) TargetHandler.this.targetPath.getInputExpression();
					}
					TargetHandler.this.expectedTargetSchema = expected;
				}
			});
			put(ArrayCreation.class, new NodeHandler<ArrayCreation>() {
				@Override
				public void handle(ArrayCreation value, TreeHandler<Object> treeHandler) {
					ArrayCreation expected = new ArrayCreation();
					for (EvaluationExpression subExpr : value.getElements()) {
						treeHandler.handle(subExpr);
						expected.add(getExpectedTargetSchema());
					}
					TargetHandler.this.expectedTargetSchema = expected;
				}
			});
			put(ObjectAccess.class, new NodeHandler<ObjectAccess>() {
				@Override
				public void handle(ObjectAccess value, TreeHandler<Object> treeHandler) {
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE) {
						TargetHandler.this.sourcePath = value;
						TargetHandler.this.expectedSourceSchema.push(EvaluationExpression.VALUE);
					}
//					TargetHandler.this.expectedSourceType.add(ObjectCreation.class);
					TargetHandler.this.expectedSourceSchema.push(new ObjectCreation());
					treeHandler.handle(value.getInputExpression());
					ObjectCreation actualObject = cast(TargetHandler.this.actualSourceSchema, ObjectCreation.class, "");
					TargetHandler.this.actualSourceSchema = actualObject.getExpression(value.getField());
					EvaluationExpression expectedType = TargetHandler.this.expectedSourceSchema.pop();
					if(!conforms(TargetHandler.this.actualSourceSchema, expectedType))
						actualObject.addMapping(value.getField(), TargetHandler.this.actualSourceSchema = expectedType);
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE)
						TargetHandler.this.sourcePath = value;
				}
			});
			put(InputSelection.class, new NodeHandler<InputSelection>() {
				@Override
				public void handle(InputSelection value, TreeHandler<Object> treeHandler) {
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE) 
						TargetHandler.this.sourcePath  = value;
//					} else
//						EntityMapping.this.sourceHandler.addToSchema(TargetHandler.this.sourcePath, value.getIndex());
					EntityMapping.this.sourceToValueCorrespondences.add(new SymbolicAssignment(TargetHandler.this.targetPath.clone(), TargetHandler.this.sourcePath));
					TargetHandler.this.sourcePath = EvaluationExpression.VALUE;
					TargetHandler.this.actualSourceSchema = EntityMapping.this.sourceSchema.get(value.getIndex());
					EvaluationExpression expectedType = TargetHandler.this.expectedSourceSchema.pop();
					if(!conforms(TargetHandler.this.actualSourceSchema, expectedType))
						EntityMapping.this.sourceSchema.set(value.getIndex(), TargetHandler.this.actualSourceSchema = expectedType);
				}
			});
			put(JsonStreamExpression.class, new NodeHandler<JsonStreamExpression>() {
				@Override
				public void handle(JsonStreamExpression value, TreeHandler<Object> treeHandler) {
					InputSelection output = new InputSelection(value.getStream().getSource().getIndex());
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE)
						EntityMapping.this.targetFKs.add(new SymbolicAssignment(TargetHandler.this.targetPath.clone(), output));
					else
						EntityMapping.this.targetFKs.add(new SymbolicAssignment(TargetHandler.this.targetPath.clone(),
							TargetHandler.this.sourcePath.clone().replace(value, output)));
					TargetHandler.this.sourcePath = EvaluationExpression.VALUE;
				}
			});
			put(ArrayAccess.class, new NodeHandler<ArrayAccess>() {
				@Override
				public void handle(ArrayAccess value, TreeHandler<Object> treeHandler) {
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE) {
						TargetHandler.this.sourcePath = value;
					}
//					TargetHandler.this.expectedSourceType.add(ArrayCreation.class);
					treeHandler.handle(value.getInputExpression());
				}
			});
			put(FunctionCall.class, new NodeHandler<FunctionCall>() {
				public void handle(FunctionCall value, eu.stratosphere.sopremo.cleansing.mapping.TreeHandler<Object> treeHandler) {
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE) {
						// TargetHandler.this.sourcePath = value;
					}
					for (EvaluationExpression param : value.getParameters())
						treeHandler.handle(param);
				}
			});
		}

		protected boolean conforms(EvaluationExpression actualSourceSchema, EvaluationExpression expectedType) {
			if(actualSourceSchema == null || actualSourceSchema == EvaluationExpression.VALUE)
				return false;
			if(actualSourceSchema.getClass() == expectedType.getClass())
				return true;
			throw new IllegalArgumentException("Incompatible source paths found");
		}

		private PathSegmentExpression targetPath, sourcePath;
		
		private EvaluationExpression expectedTargetSchema, actualSourceSchema;
		
		private Deque<EvaluationExpression> expectedSourceSchema = new LinkedList<EvaluationExpression>();
		
		private EvaluationExpression getExpectedTargetSchema() {
			EvaluationExpression expected = TargetHandler.this.expectedTargetSchema;
			TargetHandler.this.expectedTargetSchema = EvaluationExpression.VALUE;
			return expected;
		}
		
		public void process(EvaluationExpression expression, int targetIndex) {
			this.targetPath = new InputSelection(targetIndex);
			this.sourcePath = EvaluationExpression.VALUE;
			this.expectedTargetSchema = EvaluationExpression.VALUE;
			handle(expression);
			EntityMapping.this.targetSchema.set(targetIndex, this.expectedTargetSchema);
		}
	}

	private transient SourceHandler sourceHandler = new SourceHandler();

	private class SourceHandler extends TreeHandler<EvaluationExpression> {
		/**
		 * Initializes EntityMapping.SourceHandler.
		 */
		public SourceHandler() {
			put(ObjectAccess.class, new NodeHandler<ObjectAccess>() {
				@Override
				public void handle(ObjectAccess value, TreeHandler<Object> treeHandler) {
					treeHandler.handle(value.getInputExpression());
				}
			});
			put(ArrayAccess.class, new NodeHandler<ArrayAccess>() {
				@Override
				public void handle(ArrayAccess value, TreeHandler<Object> treeHandler) {
					treeHandler.handle(value.getInputExpression());
				}
			});
		}

		public void addToSchema(EvaluationExpression sourcePath, int index) {
			handle(sourcePath);
		}
	}

	@SuppressWarnings("unchecked")
	private <V> V cast(Object object, Class<V> expectedClass, String error) {
		if (expectedClass.isInstance(object))
			return (V) object;
		throw new IllegalArgumentException(error);
	}

	/**
	 * Returns the foreignKeys.
	 * 
	 * @return the foreignKeys
	 */
	public BooleanExpression getSourceFKs() {
		return this.foreignKeyExpression;
	}

	public EntityMapping withForeignKeys(final BooleanExpression assignment) {
		setSourceFKs(assignment);
		return this;
	}

	public EntityMapping withMappingExpression(final ArrayCreation assignment) {
		setMappingExpression(assignment);
		return this;
	}

	// public static final String type = "XML";
	//
	// public static final String targetStr = "target";
	//
	// public static final String sourceStr = "source";
	//
	// public static final String idStr = "id";
	//
	// public static final String separator = ".";
	//
	// public static final INode dummy = new LeafNode("string");
	// /**
	// * Mapping Task: value correspondences: mappings and grouping keys source
	// * and target schema: attributes in mappings and grouping keys target keys:
	// * grouping keys join conditions with foreign keys: where-clause and foreign
	// * keys
	// */
	// @Property
	// @Name(adjective = "into")
	// public void setMappingExpression(final ArrayCreation assignment) {
	// this.createDefaultSourceSchema(this.getInputs().size());
	// HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys =
	// new HashMap<SpicyPathExpression, SpicyPathExpression>();
	//
	// this.createDefaultTargetSchema(this.getNumOutputs());
	//
	// // iterate over schema mapping groupings
	// final MappingInformation mappingInformation = this.spicyMappingTransformation.getMappingInformation();
	// for (int index = 0; index < assignment.size(); index++) { // operator
	// // level
	//
	// final NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment.get(index);
	// final IdentifyOperator operator = (IdentifyOperator) nestedOperator.getOperator();
	// EvaluationExpression groupingKey = operator.getKeyExpression();
	// final Integer targetInputIndex = index;
	// final InputSelection sourceInput = groupingKey.findFirst(InputSelection.class);
	// String sourceNesting = this.createNesting(EntityMapping.sourceStr, sourceInput.getIndex()); // e.g.
	// // source.entities_in0.entity_in0
	// final String targetEntity = this.createNesting(EntityMapping.targetStr, targetInputIndex); // e.g.
	// // target.entities_in0.entity_in0
	//
	// // add target entity id to target schema
	// this.extendTargetSchemaBy(EntityMapping.idStr, targetInputIndex, AttributeNode.class);
	//
	// // create primary key: target entity id
	// MappingKeyConstraint targetKey = new MappingKeyConstraint(targetEntity, EntityMapping.idStr);
	// mappingInformation.getTarget().addKeyConstraint(targetKey);
	//
	// if (groupingKey instanceof ObjectAccess) {
	// final String keyStr = ((ObjectAccess) groupingKey).getField();
	// // add source grouping key to source schema
	// this.extendSourceSchemaBy(keyStr, sourceInput.getIndex(), AttributeNode.class);
	// // create value correspondence: source grouping key -> target entity
	// // id
	// MappingValueCorrespondence corr =
	// this.createValueCorrespondence(sourceNesting, keyStr, targetEntity, EntityMapping.idStr);
	// mappingInformation.getValueCorrespondences().add(corr);
	// } else if (groupingKey instanceof FunctionCall) {
	// FunctionCall expr = (FunctionCall) groupingKey;
	// List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();
	// for (ObjectAccess oa : expr.findAll(ObjectAccess.class)) {
	// this.extendSourceSchemaBy(oa.getField(), sourceInput.getIndex(), AttributeNode.class);
	// String sourceNesting2;
	// final EvaluationExpression sourceInputExpr = oa.getInputExpression();
	// final String sourceExpr = oa.getField();
	//
	// sourceNesting2 = this.createNesting(EntityMapping.sourceStr,
	// sourceInputExpr.findFirst(InputSelection.class).getIndex());
	// sourcePaths.add(new SpicyPathExpression(sourceNesting2, sourceExpr));
	// }
	// MappingValueCorrespondence corr = this.createValueCorrespondence(sourcePaths, targetEntity,
	// EntityMapping.idStr, expr);
	// mappingInformation.getValueCorrespondences().add(corr);
	// }
	//
	// ObjectCreation resultProjection = (ObjectCreation) operator.getResultProjection();
	// final List<Mapping<?>> mappings = resultProjection.getMappings();
	// for (final Mapping<?> mapping : mappings) { // mapping level
	// handleMapping(((FieldAssignment) mapping).getTarget(), mapping.getExpression(), foreignKeys,
	// targetInputIndex,
	// targetEntity, "");
	// }
	// }
	//
	// // create transitive value correspondences from foreign keys
	// List<MappingValueCorrespondence> transitiveValueCorrespondences =
	// createTransitiveValueCorrespondences(this.spicyMappingTransformation
	// .getMappingInformation().getValueCorrespondences(), foreignKeys);
	//
	// for (MappingValueCorrespondence cond : transitiveValueCorrespondences) {
	// mappingInformation.getValueCorrespondences().add(cond);
	// }
	// }
	//
	//
	// private void handleSourceJoinExpression(ComparativeExpression condidition) {
	// final ObjectAccess inputExpr1 = condidition.getExpr1().findFirst(ObjectAccess.class);
	// final ObjectAccess inputExpr2 = condidition.getExpr2().findFirst(ObjectAccess.class);
	// final InputSelection input1 = inputExpr1.findFirst(InputSelection.class);
	// final InputSelection input2 = inputExpr2.findFirst(InputSelection.class);
	// final String attr1 = inputExpr1.getField();
	// final String attr2 = inputExpr2.getField();
	//
	// final String sourceNesting = this.createNesting(EntityMapping.sourceStr, input1.getIndex());
	// final String targetNesting = this.createNesting(EntityMapping.sourceStr, input2.getIndex());
	//
	// this.spicyMappingTransformation.getMappingInformation().getSourceJoinConditions()
	// .add(this.createJoinCondition(sourceNesting, attr1, targetNesting, attr2));
	// extendSourceSchemaBy(attr1, input1.getIndex(), AttributeNode.class);
	// extendSourceSchemaBy(attr2, input2.getIndex(), AttributeNode.class);
	// }
	//
	// private MappingValueCorrespondence handleMapping(String target, EvaluationExpression expr,
	// HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys,
	// final Integer targetInputIndex, final String targetEntity, final String targetNesting) {
	//
	// MappingValueCorrespondence corr = null;
	//
	// if (expr instanceof FunctionCall || expr instanceof ArrayAccess || expr instanceof TernaryExpression
	// || expr instanceof ConstantExpression) {
	// corr = handleSpecialExpression(target, expr, foreignKeys, targetInputIndex, targetNesting);
	// } else if (expr instanceof ObjectAccess) {
	// corr =
	// handleObjectAccess(target, (ObjectAccess) expr, foreignKeys, targetInputIndex, targetEntity,
	// targetNesting, false);
	// } else if (expr instanceof ObjectCreation) {
	// ObjectCreation oc = (ObjectCreation) expr;
	// // String targetPathStepWithDummyNode = target + EntityMapping.separator + EntityMapping.dummy.getLabel();
	// // String extendedTargetNesting =
	// // targetNesting.equals("") ? targetPathStepWithDummyNode : targetNesting + EntityMapping.separator +
	// // targetPathStepWithDummyNode;
	//
	// final String attr = joinPath(targetEntity, targetNesting);
	// this.extendTargetSchemaBy(attr, targetInputIndex, SequenceNode.class);
	// for (Mapping<?> innerMapping : oc.getMappings()) {
	// corr =
	// handleMapping((String) innerMapping.getTarget(), innerMapping.getExpression(), foreignKeys, targetInputIndex,
	// targetEntity,
	// attr);
	// }
	//
	// } else if (expr instanceof AggregationExpression) {
	// final AggregationExpression agg = (AggregationExpression) expr;
	// expr = ((ArrayProjection) agg.getInputExpression()).getInputExpression();
	// // add target attribute to target schema
	// this.extendTargetSchemaBy(joinPath(targetEntity, targetNesting), targetInputIndex, SetNode.class);
	// corr = handleMapping("", expr, foreignKeys, targetInputIndex, targetEntity, targetNesting);
	// // if (agg.getAggregation() != CoreFunctions.FIRST)
	// // corr.setTakeAllValuesOfGrouping(true);
	// } else if (expr instanceof ArrayCreation) {
	// final String attr = joinPath(targetEntity, targetNesting);
	// this.extendTargetSchemaBy(attr, targetInputIndex, SetNode.class);
	// corr = handleMapping("", ((ArrayCreation) expr).getElements().get(0), foreignKeys,
	// targetInputIndex, targetEntity, attr);
	// } else {
	// throw new IllegalArgumentException("No valid value correspondence was given: " + expr);
	// }
	// return corr;
	// }
	//
	// private String joinPath(String path, String lastSegment) {
	// if (path.isEmpty())
	// return lastSegment;
	// if (lastSegment.isEmpty())
	// return path;
	// return path + EntityMapping.separator + lastSegment;
	// }
	//
	// private MappingValueCorrespondence handleObjectAccess(String target, final ObjectAccess expr,
	// HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys,
	// final Integer targetInputIndex, final String targetEntity, final String targetNesting,
	// boolean skipValueCorrespondence) {
	// MappingValueCorrespondence corr = null;
	//
	// final EvaluationExpression sourceInputExpr = expr.getInputExpression();
	// final String sourceExpr = expr.getField();
	//
	// // distinguish between s-t and t dependencies
	// final int fkSource;
	// if (sourceInputExpr.findFirst(InputSelection.class) != null)
	// fkSource = sourceInputExpr.findFirst(InputSelection.class).getIndex();
	// else
	// fkSource = sourceInputExpr.findFirst(JsonStreamExpression.class).getStream().getSource().getIndex();
	//
	// if (sourceInputExpr instanceof JsonStreamExpression &&
	// ((JsonStreamExpression) sourceInputExpr).getStream().getSource().getOperator() == this) {
	//
	// String sourceNesting = this.createNesting(EntityMapping.targetStr, fkSource);
	//
	// // create join condition for foreign keys, but no value
	// // correspondence
	// MappingJoinCondition targetJoinCondition =
	// this.createJoinCondition(joinPath(targetEntity, targetNesting), target, sourceNesting, sourceExpr);
	// this.spicyMappingTransformation.getMappingInformation().getTargetJoinConditions().add(targetJoinCondition);
	//
	// // store foreign keys to add missing (transitive) value
	// // correspondences later
	// foreignKeys.put(targetJoinCondition.getFromPaths().get(0), targetJoinCondition.getToPaths().get(0));
	// } else { // no foreign key
	//
	// String sourceNesting = this.createNesting(EntityMapping.sourceStr, fkSource);
	//
	// // add source attribute to source schema
	// this.extendSourceSchemaBy(sourceExpr, sourceInputExpr.findFirst(InputSelection.class).getIndex(),
	// AttributeNode.class);
	//
	// // create value correspondence: source attribute ->
	// // target
	// // attribute
	// if (!skipValueCorrespondence) {
	// corr =
	// this.createValueCorrespondence(sourceNesting, sourceExpr, joinPath(targetEntity, targetNesting), target);
	// this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences().add(corr);
	// }
	// }
	//
	// // add target attribute to target schema
	// if (target.isEmpty())
	// this.extendTargetSchemaBy(targetNesting, targetInputIndex, LeafNode.class);
	// else
	// this.extendTargetSchemaBy(joinPath(targetNesting, target), targetInputIndex, AttributeNode.class);
	//
	// // add join attributes to source schema
	// for (MappingJoinCondition cond :
	// this.spicyMappingTransformation.getMappingInformation().getSourceJoinConditions()) {
	//
	// String joinAttr = cond.getFromPaths().get(0).getLastStep();
	// String joinSource = cond.getFromPaths().get(0).getFirstStep();
	//
	// if (joinSource.contains(Integer.toString(fkSource)))
	// this.extendSourceSchemaBy(joinAttr, fkSource, AttributeNode.class);
	//
	// joinAttr = cond.getToPaths().get(0).getLastStep();
	// joinSource = cond.getToPaths().get(0).getFirstStep();
	//
	// if (joinSource.contains(Integer.toString(fkSource)))
	// this.extendSourceSchemaBy(joinAttr, fkSource, AttributeNode.class);
	// }
	// return corr;
	// }
	//
	// private MappingValueCorrespondence handleSpecialExpression(String target, EvaluationExpression expr,
	// HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys,
	// final Integer targetInputIndex, final String targetNesting) {
	// List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();
	// for (ObjectAccess oa : expr.findAll(ObjectAccess.class)) {
	// handleObjectAccessInSpecialExpression(target, oa, foreignKeys, targetInputIndex, targetNesting, sourcePaths);
	// }
	// this.extendTargetSchemaBy(target, targetInputIndex, AttributeNode.class);
	// MappingValueCorrespondence corr =
	// this.createValueCorrespondence(sourcePaths, targetNesting, target, expr);
	// this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences().add(corr);
	// return corr;
	// }
	//
	// private void handleObjectAccessInSpecialExpression(String target, ObjectAccess oa,
	// HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys,
	// Integer targetInputIndex, String targetNesting, List<SpicyPathExpression> sourcePaths) {
	// String sourceNesting;
	// handleObjectAccess(target, oa, foreignKeys, targetInputIndex, targetNesting, "", true);
	//
	// final EvaluationExpression sourceInputExpr = oa.getInputExpression();
	// final String sourceExpr = oa.getField();
	//
	// // TODO this does not work as expected
	// if (sourceInputExpr.toString().contains(this.getClass().getSimpleName())) {
	// Integer fkSource = Integer.parseInt(sourceInputExpr.toString().replaceAll("[^0-9]", ""));
	// sourceNesting = this.createNesting(EntityMapping.targetStr, fkSource);
	// sourcePaths.add(new SpicyPathExpression(sourceNesting, sourceExpr));
	// } else {
	// sourceNesting =
	// this.createNesting(EntityMapping.sourceStr, sourceInputExpr.findFirst(InputSelection.class).getIndex());
	// sourcePaths.add(new SpicyPathExpression(sourceNesting, sourceExpr));
	// }
	// }
	//
	// private void createDefaultSourceSchema(final int size) {
	// this.spicyMappingTransformation.getMappingInformation().setSourceSchema(
	// new MappingSchema(size, EntityMapping.sourceStr));
	// }
	//
	// private void createDefaultTargetSchema(final int size) {
	// this.spicyMappingTransformation.getMappingInformation().getTarget().setTargetSchema(
	// new MappingSchema(size, EntityMapping.targetStr));
	//
	// }
	//
	// private void extendSourceSchemaBy(final String attr, final Integer inputIndex, Class<? extends INode> node) {
	// this.spicyMappingTransformation.getMappingInformation().getSourceSchema().addKeyToInput(inputIndex, attr, node);
	// }
	//
	// private void extendTargetSchemaBy(final String attr, final Integer inputIndex, Class<? extends INode> node) {
	// final MappingSchema targetSchema =
	// this.spicyMappingTransformation.getMappingInformation().getTarget().getTargetSchema();
	// targetSchema.addKeyToInput(inputIndex, attr, node);
	// }
	//
	// private String createNesting(final String type, final int inputIndex) {
	//
	// final StringBuilder builder =
	// new StringBuilder().append(type).append(separator).append(inputIndex).
	// append(separator).append("object");
	//
	// return builder.toString();
	// }
	//
	// private MappingValueCorrespondence createValueCorrespondence(final String sourceNesting, final String sourceAttr,
	// final String targetNesting,
	// final String targetAttr) {
	// final SpicyPathExpression sourceSteps = new SpicyPathExpression(sourceNesting, sourceAttr);
	// final SpicyPathExpression targetSteps = new SpicyPathExpression(targetNesting, targetAttr);
	//
	// return new MappingValueCorrespondence(sourceSteps, targetSteps);
	// }
	//
	// private MappingValueCorrespondence createValueCorrespondence(final List<SpicyPathExpression> sourcePaths,
	// final String targetNesting,
	// final String targetAttr, EvaluationExpression expr) {
	// final SpicyPathExpression targetSteps = new SpicyPathExpression(targetNesting, targetAttr);
	//
	// return new MappingValueCorrespondence(sourcePaths, targetSteps, expr);
	// }
	//
	// private MappingJoinCondition createJoinCondition(final String sourceNesting, final String sourceAttr,
	// final String targetNesting, final String targetAttr) {
	// MappingJoinCondition joinCondition =
	// new MappingJoinCondition(Collections.singletonList(new SpicyPathExpression(sourceNesting, sourceAttr)),
	// Collections.singletonList(new SpicyPathExpression(targetNesting, targetAttr)), true, true);
	//
	// return joinCondition;
	// }
	//
	// private List<MappingValueCorrespondence> createTransitiveValueCorrespondences(
	// Set<MappingValueCorrespondence> valueCorrespondences,
	// HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys) {
	//
	// List<MappingValueCorrespondence> transitiveValueCorrespondences = new ArrayList<MappingValueCorrespondence>();
	// for (SpicyPathExpression fk : foreignKeys.keySet()) {
	// SpicyPathExpression value = foreignKeys.get(fk);
	//
	// for (MappingValueCorrespondence mvc : valueCorrespondences) {
	// if (mvc.getTargetPath().equals(value)) {
	// MappingValueCorrespondence correspondence =
	// new MappingValueCorrespondence(mvc.getSourcePaths().get(0), fk);
	// transitiveValueCorrespondences.add(correspondence);
	// }
	// }
	// }
	//
	// return transitiveValueCorrespondences;
	// }

	public SpicyMappingTransformation getSpicyMappingTransformation() {
		SpicyMappingTransformation spicyMappingTransformation = new SpicyMappingTransformation();

		// spicyMappingTransformation.setSourceSchema(this.sourceSchema, this.sourceFKs);
		// spicyMappingTransformation.setTargetSchema(this.targetSchema, this.targetFKs);
		// spicyMappingTransformation.setSourceToTargetDependencies(this.sourceToValueCorrespondences);
		// spicyMappingTransformation.setTargetDependencies();
		// spicyMappingTransformation.setTargetSchema();

		// spicyMappingTransformation.getM
		// this.createDefaultSourceSchema(this.getInputs().size());
		// if (assignment instanceof ComparativeExpression) {
		// handleSourceJoinExpression((ComparativeExpression) assignment);
		// } else {
		//
		// for (final ChildIterator it = assignment.iterator(); it.hasNext();) {
		// final EvaluationExpression expr = it.next();
		// final ComparativeExpression condidition = (ComparativeExpression) expr;
		// handleSourceJoinExpression(condidition);
		// }
		// }

		return spicyMappingTransformation;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.Operator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		SopremoUtil.append(appendable, "EntityMapping [sourceFKs=", this.sourceFKs, ", targetFKs=", this.targetFKs,
			", sourceToValueCorrespondences=", this.sourceToValueCorrespondences, ", sourceSchema=", this.sourceSchema, ", targetSchema=",
			this.targetSchema, "]");
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator. SopremoModule,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(final SopremoModule module) {
		module.embed(getSpicyMappingTransformation());
		//
		// Map<String, Integer> inputIndex = new HashMap<String, Integer>();
		// for (int i = 0; i < this.getNumInputs(); i++) {
		// inputIndex.put(String.valueOf(i), i);
		// }
		// Map<String, Integer> outputIndex = new HashMap<String, Integer>();
		// // FIXME hack to solve #output problem
		// this.getOutputs();
		//
		// for (int i = 0; i < this.getNumOutputs(); i++) {
		// outputIndex.put(String.valueOf(i), i);
		// }
		// this.spicyMappingTransformation.setInputIndex(inputIndex);
		// this.spicyMappingTransformation.setOutputIndex(outputIndex);
		// this.spicyMappingTransformation.addImplementation(module);
	}
}