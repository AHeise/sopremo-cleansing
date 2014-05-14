package eu.stratosphere.sopremo.cleansing;

import static eu.stratosphere.sopremo.pact.SopremoUtil.cast;

import java.util.*;

import org.apache.tools.ant.helper.ProjectHelper2.TargetHandler;

import eu.stratosphere.sopremo.cleansing.mapping.DataTransformationBase;
import eu.stratosphere.sopremo.cleansing.mapping.IdentifyOperator;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyMappingTransformation;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ObjectCreation.FieldAssignment;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.ObjectCreation.SymbolicAssignment;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.tree.NodeHandler;
import eu.stratosphere.sopremo.tree.ReturnLessNodeHandler;
import eu.stratosphere.sopremo.tree.ReturnlessTreeHandler;
import eu.stratosphere.sopremo.tree.TreeHandler;
import eu.stratosphere.util.CollectionUtil;

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

	private static class SourceFKParser extends
			ReturnlessTreeHandler<BooleanExpression, Set<ObjectCreation.SymbolicAssignment>> {
		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 */
		public SourceFKParser() {
			put(AndExpression.class,
				new ReturnLessNodeHandler<AndExpression, Set<ObjectCreation.SymbolicAssignment>>() {
					@Override
					protected void handleNoReturn(AndExpression value, Set<SymbolicAssignment> param,
							TreeHandler<Object, Object, Set<SymbolicAssignment>> treeHandler) {
						for (EvaluationExpression subExpr : value)
							treeHandler.handle(subExpr, param);
					}
				});
			put(ComparativeExpression.class,
				new ReturnLessNodeHandler<ComparativeExpression, Set<ObjectCreation.SymbolicAssignment>>() {
					/*
					 * (non-Javadoc)
					 * @see eu.stratosphere.sopremo.tree.ReturnLessNodeHandler#handleNoReturn(java.lang.Object,
					 * java.lang.Object, eu.stratosphere.sopremo.tree.TreeHandler)
					 */
					@Override
					protected void handleNoReturn(ComparativeExpression value, Set<SymbolicAssignment> param,
							TreeHandler<Object, Object, Set<SymbolicAssignment>> treeHandler) {
						if (value.getBinaryOperator() != BinaryOperator.EQUAL)
							throw new IllegalArgumentException("Only == supported");
						param.add(new ObjectCreation.SymbolicAssignment(
							value.getExpr1(), value.getExpr2()));
					}
				});
		}

		public Set<ObjectCreation.SymbolicAssignment> convert(BooleanExpression value) {
			Set<ObjectCreation.SymbolicAssignment> foreignKeyExpressions =
				new HashSet<ObjectCreation.SymbolicAssignment>();
			handle(value, foreignKeyExpressions);
			return foreignKeyExpressions;
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
			EntityMapping.this.sourceHandler.addToSchema(nestedOperator.getKeyExpression(),
				EntityMapping.this.sourceSchemaFromMapping);
			this.sourceToValueCorrespondences.add(new SymbolicAssignment(
				new ObjectAccess("id").withInputExpression(new InputSelection(targetIndex)),
				nestedOperator.getKeyExpression()));
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

	private class TargetHandler extends TreeHandler<EvaluationExpression, EvaluationExpression, PathSegmentExpression> {
		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 */
		public TargetHandler() {
			put(ObjectCreation.class, new NodeHandler<ObjectCreation, EvaluationExpression, PathSegmentExpression>() {
				@Override
				public EvaluationExpression handle(ObjectCreation value, PathSegmentExpression targetPath,
						TreeHandler<Object, EvaluationExpression, PathSegmentExpression> treeHandler) {
					ObjectCreation oc = new ObjectCreation();
					for (Mapping<?> subExpr : value.getMappings()) {
						FieldAssignment assignment = cast(subExpr, ObjectCreation.FieldAssignment.class, "");
						oc.addMapping(assignment.getTarget(),
							treeHandler.handle(assignment.getExpression(),
								new ObjectAccess(assignment.getTarget()).withInputExpression(targetPath)));
					}
					return oc;
				}
			});
			put(ArrayCreation.class, new NodeHandler<ArrayCreation, EvaluationExpression, PathSegmentExpression>() {
				@Override
				public EvaluationExpression handle(ArrayCreation value, PathSegmentExpression targetPath,
						TreeHandler<Object, EvaluationExpression, PathSegmentExpression> treeHandler) {
					ArrayCreation ac = new ArrayCreation();
					List<EvaluationExpression> elements = value.getElements();
					for (int index = 0; index < elements.size(); index++)
						ac.add(treeHandler.handle(elements.get(index),
							new ArrayAccess(index).withInputExpression(targetPath)));
					return ac;
				}
			});
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.tree.TreeHandler#unknownValueType(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected EvaluationExpression unknownValueType(EvaluationExpression value, PathSegmentExpression targetPath) {
			// probably the source path; let the source handler try to parse it and possibly fail
			JsonStreamExpression streamExpr = value.findFirst(JsonStreamExpression.class);
			// target FK
			if (streamExpr != null) {
				InputSelection output = new InputSelection(streamExpr.getStream().getSource().getIndex());
				final EvaluationExpression exprWithInputSel = value.clone().replace(streamExpr, output);
				EntityMapping.this.sourceHandler.addToSchema(exprWithInputSel, EntityMapping.this.targetSchema);
				EntityMapping.this.targetFKs.add(new SymbolicAssignment(targetPath.clone(),
					exprWithInputSel));
			} else {
				EntityMapping.this.sourceHandler.addToSchema(value, EntityMapping.this.sourceSchemaFromMapping);
				EntityMapping.this.sourceToValueCorrespondences.add(new SymbolicAssignment(
					targetPath.clone(), value));
			}
			return EvaluationExpression.VALUE;
		}

		public void process(EvaluationExpression expression, int targetIndex) {
			EntityMapping.this.targetSchema.set(targetIndex, handle(expression, new InputSelection(targetIndex)));
		}
	}

	private transient SourceHandler sourceHandler = new SourceHandler();

	private class SourceHandler extends TreeHandler<EvaluationExpression, EvaluationExpression, EvaluationExpression> {
		/**
		 * Initializes EntityMapping.SourceHandler.
		 */
		public SourceHandler() {
			put(ObjectAccess.class, new NodeHandler<ObjectAccess, EvaluationExpression, EvaluationExpression>() {
				@Override
				public EvaluationExpression handle(ObjectAccess value, EvaluationExpression expected,
						TreeHandler<Object, EvaluationExpression, EvaluationExpression> treeHandler) {
					ObjectCreation oc = (ObjectCreation)
						treeHandler.handle(value.getInputExpression(), new ObjectCreation());
					EvaluationExpression actualSourceSchema = oc.getExpression(value.getField());
					if (!conforms(actualSourceSchema, expected))
						oc.addMapping(value.getField(), actualSourceSchema = expected);
					return actualSourceSchema;
				}
			});
			put(ArrayAccess.class, new NodeHandler<ArrayAccess, EvaluationExpression, EvaluationExpression>() {
				@Override
				public EvaluationExpression handle(ArrayAccess value, EvaluationExpression expected,
						TreeHandler<Object, EvaluationExpression, EvaluationExpression> treeHandler) {
					ArrayCreation ac = (ArrayCreation)
						treeHandler.handle(value.getInputExpression(), new ArrayCreation());
					final List<EvaluationExpression> elements = ac.getElements();
					CollectionUtil.ensureSize(elements, value.getStartIndex() + 1, EvaluationExpression.VALUE);
					EvaluationExpression actualSourceSchema = elements.get(value.getStartIndex());
					if (!conforms(actualSourceSchema, expected)) {
						elements.set(value.getStartIndex(), actualSourceSchema = expected);
						ac.setElements(elements);
					}
					return actualSourceSchema;
				}
			});
			put(InputSelection.class, new NodeHandler<InputSelection, EvaluationExpression, EvaluationExpression>() {
				@Override
				public EvaluationExpression handle(InputSelection value, EvaluationExpression expected,
						TreeHandler<Object, EvaluationExpression, EvaluationExpression> treeHandler) {
					EvaluationExpression actualSourceSchema = SourceHandler.this.sourceSchemas.get(value.getIndex());
					if (!conforms(actualSourceSchema, expected))
						SourceHandler.this.sourceSchemas.set(value.getIndex(), actualSourceSchema = expected);
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

		private List<EvaluationExpression> sourceSchemas;

		public void addToSchema(EvaluationExpression sourcePath, List<EvaluationExpression> sourceSchemas) {
			this.sourceSchemas = sourceSchemas;
			handle(sourcePath, EvaluationExpression.VALUE);
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
		SpicyMappingTransformation spicyMappingTransformation = getSpicyMappingTransformation();
		spicyMappingTransformation.setInputs(module.getInputs());
		for (int index = 0; index < getNumOutputs(); index++)
			module.getOutput(index).setInput(0, spicyMappingTransformation.getOutput(index));
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