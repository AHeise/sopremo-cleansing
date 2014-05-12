package eu.stratosphere.sopremo.cleansing;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.sopremo.cleansing.mapping.DataTransformationBase;
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
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.util.CollectionUtil;
import static eu.stratosphere.sopremo.pact.SopremoUtil.*;

@Name(noun = "map entities of")
public class EntityMapping extends DataTransformationBase<EntityMapping> {
	private transient ArrayCreation mappingExpression = new ArrayCreation();

	private transient BooleanExpression sourceForeignKeyExpression = new AndExpression();

	private transient List<EvaluationExpression> sourceSchemaFromMapping = new ArrayList<EvaluationExpression>();

	/**
	 * Returns the mappingExpression.
	 * 
	 * @return the mappingExpression
	 */
	public ArrayCreation getMappingExpression() {
		return this.mappingExpression;
	}


	@Property
	@Name(preposition = "where")
	public void setSourceForeignKeyExpression(final BooleanExpression assignment) {
		this.sourceForeignKeyExpression = assignment;
		this.sourceFKs = SourceFKParser.convert(assignment);
		mergeSourceSchema();
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
					SourceFKParser.this.foreignKeyExpressions.add(new ObjectCreation.SymbolicAssignment(
						value.getExpr1(), value.getExpr2()));
				}
			});
		}

		private Set<ObjectCreation.SymbolicAssignment> foreignKeyExpressions =
			new HashSet<ObjectCreation.SymbolicAssignment>();

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

		this.sourceSchemaFromMapping.clear();
		CollectionUtil.ensureSize(this.sourceSchemaFromMapping, getNumInputs(), EvaluationExpression.VALUE);
		this.targetSchema.clear();
		CollectionUtil.ensureSize(this.targetSchema, getNumOutputs(), EvaluationExpression.VALUE);
		this.sourceToValueCorrespondences.clear();
		List<EvaluationExpression> elements = mappingExpression.getElements();
		for (EvaluationExpression targetAssignment : elements) {
			NestedOperatorExpression noe =
				cast(targetAssignment, NestedOperatorExpression.class, "Please specify suboperators with " +
					IdentifyOperator.class);
			final IdentifyOperator nestedOperator =
				cast(noe.getOperator(), IdentifyOperator.class, "Please specify suboperators with " +
					IdentifyOperator.class);
			JsonStream outVar = nestedOperator.getInput(0);
			int targetIndex = outVar.getSource().getIndex();
			this.targetHandler.process(nestedOperator.getResultProjection(), targetIndex);
			EntityMapping.this.sourceHandler.addToSchema(nestedOperator.getKeyExpression(), EntityMapping.this.sourceSchemaFromMapping);
			this.sourceToValueCorrespondences.add(new SymbolicAssignment(
				new ObjectAccess("id").withInputExpression(new InputSelection(targetIndex)), nestedOperator.getKeyExpression()));
		}

		mergeSourceSchema();

		// add target as late as possible to avoid overwriting it in targetHandler.process
		for (SymbolicAssignment corr : this.targetFKs) 
			EntityMapping.this.sourceHandler.addToSchema(corr.getExpression(), EntityMapping.this.targetSchema);
		System.out.println(this);
	}

	/**
	 * 
	 */
	private void mergeSourceSchema() {
		this.sourceSchema.clear();
		CollectionUtil.ensureSize(this.sourceSchema, getNumInputs(), EvaluationExpression.VALUE);
		for (int index = 0; index < this.sourceSchemaFromMapping.size(); index++) 
			this.sourceSchema.set(index, this.sourceSchemaFromMapping.get(index).clone());
		for (SymbolicAssignment corr : this.sourceFKs) {
			EntityMapping.this.sourceHandler.addToSchema(corr.getExpression(), EntityMapping.this.sourceSchema);
			EntityMapping.this.sourceHandler.addToSchema(corr.getTargetTagExpression(), EntityMapping.this.sourceSchema);
		}
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
						treeHandler.handle(TargetHandler.this.sourcePath = assignment.getExpression());
						expected.addMapping(assignment.getTarget(), getExpectedTargetSchema());
						TargetHandler.this.targetPath =
							(PathSegmentExpression) TargetHandler.this.targetPath.getInputExpression();
					}
					TargetHandler.this.expectedTargetSchema = expected;
				}
			});
			put(ArrayCreation.class, new NodeHandler<ArrayCreation>() {
				@Override
				public void handle(ArrayCreation value, TreeHandler<Object> treeHandler) {
					ArrayCreation expected = new ArrayCreation();
					for (EvaluationExpression subExpr : value.getElements()) {
						treeHandler.handle(TargetHandler.this.sourcePath = subExpr);
						expected.add(getExpectedTargetSchema());
					}
					TargetHandler.this.expectedTargetSchema = expected;
				}
			});
			put(ObjectAccess.class, new NodeHandler<ObjectAccess>() {
				@Override
				public void handle(ObjectAccess value, TreeHandler<Object> treeHandler) {
					treeHandler.handle(value.getInputExpression());
				}
			});
			put(InputSelection.class, new NodeHandler<InputSelection>() {
				@Override
				public void handle(InputSelection value, TreeHandler<Object> treeHandler) {
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE)
						TargetHandler.this.sourcePath = value;
					else
						EntityMapping.this.sourceHandler.addToSchema(TargetHandler.this.sourcePath, EntityMapping.this.sourceSchemaFromMapping);
						
					EntityMapping.this.sourceToValueCorrespondences.add(new SymbolicAssignment(
						TargetHandler.this.targetPath.clone(), TargetHandler.this.sourcePath));
					TargetHandler.this.sourcePath = EvaluationExpression.VALUE;
				}
			});
			put(JsonStreamExpression.class, new NodeHandler<JsonStreamExpression>() {
				@Override
				public void handle(JsonStreamExpression value, TreeHandler<Object> treeHandler) {
					InputSelection output = new InputSelection(value.getStream().getSource().getIndex());
					if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE)
						EntityMapping.this.targetFKs.add(new SymbolicAssignment(TargetHandler.this.targetPath.clone(),
							output));
					else {
						final EvaluationExpression exprWithInputSel =
							TargetHandler.this.sourcePath.clone().replace(value, output);
						EntityMapping.this.targetFKs.add(new SymbolicAssignment(TargetHandler.this.targetPath.clone(),
							exprWithInputSel));
					}
					TargetHandler.this.sourcePath = EvaluationExpression.VALUE;
				}
			});
			put(ArrayAccess.class, new NodeHandler<ArrayAccess>() {
				@Override
				public void handle(ArrayAccess value, TreeHandler<Object> treeHandler) {
					treeHandler.handle(value.getInputExpression());
				}
			});
			put(FunctionCall.class, new NodeHandler<FunctionCall>() {
				@Override
				public void handle(FunctionCall value, TreeHandler<Object> treeHandler) {
					throw new UnsupportedOperationException();
					// if (TargetHandler.this.sourcePath == EvaluationExpression.VALUE) {
					// // TargetHandler.this.sourcePath = value;
					// }
					// for (EvaluationExpression param : value.getParameters())
					// treeHandler.handle(param);
				}
			});
		}

		private PathSegmentExpression targetPath;

		private EvaluationExpression sourcePath;

		private EvaluationExpression expectedTargetSchema;

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
					SourceHandler.this.expectedSourceSchema.push(new ObjectCreation());
					treeHandler.handle(value.getInputExpression());
					ObjectCreation oc = (ObjectCreation) SourceHandler.this.actualSourceSchema;
					EvaluationExpression expectedType = SourceHandler.this.expectedSourceSchema.poll();
					SourceHandler.this.actualSourceSchema = oc.getExpression(value.getField());
					if (!conforms(SourceHandler.this.actualSourceSchema, expectedType))
						oc.addMapping(value.getField(), SourceHandler.this.actualSourceSchema = expectedType);
				}
			});
			put(ArrayAccess.class, new NodeHandler<ArrayAccess>() {
				@Override
				public void handle(ArrayAccess value, TreeHandler<Object> treeHandler) {
					SourceHandler.this.expectedSourceSchema.push(new ArrayCreation());
					treeHandler.handle(value.getInputExpression());
					ArrayCreation ac = (ArrayCreation) SourceHandler.this.actualSourceSchema;
					EvaluationExpression expectedType = SourceHandler.this.expectedSourceSchema.poll();
					final List<EvaluationExpression> elements = ac.getElements();
					CollectionUtil.ensureSize(elements, value.getStartIndex() + 1, EvaluationExpression.VALUE);
					SourceHandler.this.actualSourceSchema = elements.get(value.getStartIndex());
					if (!conforms(SourceHandler.this.actualSourceSchema, expectedType)) {
						elements.set(value.getStartIndex(), SourceHandler.this.actualSourceSchema = expectedType);
						ac.setElements(elements);
					}
				}
			});
			put(InputSelection.class, new NodeHandler<InputSelection>() {
				@Override
				public void handle(InputSelection value, TreeHandler<Object> treeHandler) {
					SourceHandler.this.actualSourceSchema = SourceHandler.this.sourceSchemas.get(value.getIndex());
					EvaluationExpression expectedType = SourceHandler.this.expectedSourceSchema.poll();
					if (!conforms(SourceHandler.this.actualSourceSchema, expectedType))
						SourceHandler.this.sourceSchemas.set(value.getIndex(), SourceHandler.this.actualSourceSchema = expectedType);
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

		private Deque<EvaluationExpression> expectedSourceSchema = new LinkedList<EvaluationExpression>();

		private EvaluationExpression actualSourceSchema;

		private List<EvaluationExpression> sourceSchemas;

		public void addToSchema(EvaluationExpression sourcePath, List<EvaluationExpression> sourceSchemas) {
			this.expectedSourceSchema.push(EvaluationExpression.VALUE);
			this.sourceSchemas = sourceSchemas;
			handle(sourcePath);
		}
	}

	/**
	 * Returns the foreignKeys.
	 * 
	 * @return the foreignKeys
	 */
	public BooleanExpression getSourceForeignKeyExpression() {
		return this.sourceForeignKeyExpression;
	}

	public EntityMapping withForeignKeys(final BooleanExpression assignment) {
		setSourceForeignKeyExpression(assignment);
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

	private SpicyMappingTransformation getSpicyMappingTransformation() {
		SpicyMappingTransformation spicyMappingTransformation = new SpicyMappingTransformation();

		spicyMappingTransformation.setSourceSchema(getSourceSchema());
		spicyMappingTransformation.setTargetSchema(getTargetSchema());
		spicyMappingTransformation.setSourceFKs(getSourceFKs());
		spicyMappingTransformation.setTargetFKs(getTargetFKs());
		spicyMappingTransformation.setSourceToValueCorrespondences(getSourceToValueCorrespondences());

		return spicyMappingTransformation;
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