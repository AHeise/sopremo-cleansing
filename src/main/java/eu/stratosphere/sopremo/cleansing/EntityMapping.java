package eu.stratosphere.sopremo.cleansing;

import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.sopremo.cleansing.mapping.IdentifyOperator;
import eu.stratosphere.sopremo.cleansing.mapping.MappingInformation;
import eu.stratosphere.sopremo.cleansing.mapping.MappingJoinCondition;
import eu.stratosphere.sopremo.cleansing.mapping.MappingKeyConstraint;
import eu.stratosphere.sopremo.cleansing.mapping.MappingSchema;
import eu.stratosphere.sopremo.cleansing.mapping.MappingValueCorrespondence;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyMappingTransformation;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyPathExpression;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ArrayProjection;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.NestedOperatorExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.TernaryExpression;
import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * @author Claudia Lehmann, Arvid Heise, Fabian Tschirschnitz, Tommy Neubert
 */
@Name(noun = "map entities of")
@InputCardinality(min = 1)
@OutputCardinality(min = 1)
public class EntityMapping extends CompositeOperator<EntityMapping> {

	public static final String type = "XML";

	public static final String targetStr = "target";

	public static final String sourceStr = "source";

	public static final String entitiesStr = "entities_";

	public static final String entityStr = "entity_";

	public static final String idStr = "id";
	
	public static final String separator = ".";

	public static final INode dummy = new LeafNode("dummy");

	private SpicyMappingTransformation spicyMappingTransformation = new SpicyMappingTransformation();

	private BooleanExpression foreignKeys;

	private ArrayCreation mappingExpression;

	public SpicyMappingTransformation getSpicyMappingTransformation() {
		return spicyMappingTransformation;
	}

	public void setSpicyMappingTransformation(SpicyMappingTransformation spicyMappingTransformation) {
		this.spicyMappingTransformation = spicyMappingTransformation;
	}

	@Property
	@Name(preposition = "where")
	public void setForeignKeys(final BooleanExpression assignment) {
		if (assignment instanceof ComparativeExpression) {
			handleSourceJoinExpression((ComparativeExpression) assignment);
		} else {

			for (final ChildIterator it = assignment.iterator(); it.hasNext();) {
				final EvaluationExpression expr = it.next();
				final ComparativeExpression condidition = (ComparativeExpression) expr;
				handleSourceJoinExpression((ComparativeExpression) condidition);
			}
		}
	}

	private void handleSourceJoinExpression(ComparativeExpression condidition) {
		final ObjectAccess inputExpr1 = condidition.getExpr1().findFirst(ObjectAccess.class);
		final ObjectAccess inputExpr2 = condidition.getExpr2().findFirst(ObjectAccess.class);
		final InputSelection input1 = inputExpr1.findFirst(InputSelection.class);
		final InputSelection input2 = inputExpr2.findFirst(InputSelection.class);
		final String attr1 = inputExpr1.getField();
		final String attr2 = inputExpr2.getField();

		final String sourceNesting = this.createNesting(EntityMapping.sourceStr, input1.getIndex());
		final String targetNesting = this.createNesting(EntityMapping.sourceStr, input2.getIndex());

		this.spicyMappingTransformation.getMappingInformation().getSourceJoinConditions()
				.add(this.createJoinCondition(sourceNesting, attr1, targetNesting, attr2));
	}

	/**
	 * Returns the foreignKeys.
	 * 
	 * @return the foreignKeys
	 */
	public BooleanExpression getForeignKeys() {
		return this.foreignKeys;
	}

	public EntityMapping withForeignKeys(final BooleanExpression assignment) {
		setForeignKeys(assignment);
		return this;
	}

	public EntityMapping withMappingExpression(final ArrayCreation assignment) {
		setMappingExpression(assignment);
		return this;
	}

	/**
	 * Mapping Task: value correspondences: mappings and grouping keys source
	 * and target schema: attributes in mappings and grouping keys target keys:
	 * grouping keys join conditions with foreign keys: where-clause and foreign
	 * keys
	 */
	@Property
	@Name(adjective = "into")
	public void setMappingExpression(final ArrayCreation assignment) {

		HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys = new HashMap<SpicyPathExpression, SpicyPathExpression>();

		this.createDefaultSourceSchema(this.getInputs().size());
		this.createDefaultTargetSchema(this.getNumOutputs());

		// iterate over schema mapping groupings
		final MappingInformation mappingInformation = this.spicyMappingTransformation.getMappingInformation();
		for (int index = 0; index < assignment.size(); index++) { // operator
																	// level

			final NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment.get(index);
			final IdentifyOperator operator = (IdentifyOperator) nestedOperator.getOperator();
			EvaluationExpression groupingKey = operator.getKeyExpression();
			final Integer targetInputIndex = index; 
			final InputSelection sourceInput = groupingKey.findFirst(InputSelection.class); 
			String sourceNesting = this.createNesting(EntityMapping.sourceStr, sourceInput.getIndex()); // e.g.
			// source.entities_in0.entity_in0
			final String targetNesting = this.createNesting(EntityMapping.targetStr, targetInputIndex); // e.g.
			// target.entities_in0.entity_in0

			// add target entity id to target schema
			this.extendTargetSchemaBy(EntityMapping.idStr, targetInputIndex);

			// create primary key: target entity id
			MappingKeyConstraint targetKey = new MappingKeyConstraint(targetNesting, EntityMapping.idStr);
			mappingInformation.getTarget().addKeyConstraint(targetKey);
			
			if(groupingKey instanceof ObjectAccess){
				final String keyStr = ((ObjectAccess) groupingKey).getField();
				// add source grouping key to source schema
				this.extendSourceSchemaBy(keyStr, sourceInput.getIndex());
				// create value correspondence: source grouping key -> target entity
				// id
				MappingValueCorrespondence corr = this.createValueCorrespondence(sourceNesting, keyStr, targetNesting, EntityMapping.idStr);
				mappingInformation.getValueCorrespondences().add(corr);
			} else if (groupingKey instanceof FunctionCall) {
				FunctionCall expr = (FunctionCall) groupingKey;
				List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();
				for (ObjectAccess oa : expr.findAll(ObjectAccess.class)) {
					this.extendSourceSchemaBy(oa.getField(), sourceInput.getIndex());
					String sourceNesting2;
					final EvaluationExpression sourceInputExpr = oa.getInputExpression();
					final String sourceExpr = oa.getField();

					sourceNesting2 = this.createNesting(EntityMapping.sourceStr,
							sourceInputExpr.findFirst(InputSelection.class).getIndex());
					sourcePaths.add(new SpicyPathExpression(sourceNesting2, sourceExpr));
				}
				MappingValueCorrespondence corr = this.createValueCorrespondence(sourcePaths, targetNesting,
						EntityMapping.idStr, expr);
				mappingInformation.getValueCorrespondences().add(corr);
			}
			
			ObjectCreation resultProjection = (ObjectCreation) operator.getResultProjection();
			final List<Mapping<?>> mappings = resultProjection.getMappings();
			for (final Mapping<?> mapping : mappings) { // mapping level

				final EvaluationExpression expr = mapping.getExpression();

				if (expr instanceof FunctionCall || expr instanceof ArrayAccess || expr instanceof TernaryExpression
						|| expr instanceof ObjectCreation
						|| expr instanceof ConstantExpression) {
					handleSpecialExpression(foreignKeys, mappingInformation, targetInputIndex, targetNesting, mapping,
							expr);
				} else if (expr instanceof ObjectAccess) {
					handleObjectAccess(foreignKeys, mappingInformation, targetInputIndex, targetNesting, mapping,
							(ObjectAccess) expr, false);
				} else if (expr instanceof AggregationExpression) {
					handleTakeAll(foreignKeys, mappingInformation, targetInputIndex, targetNesting, mapping,
							(AggregationExpression) expr);
				} else {
					throw new IllegalArgumentException("No valid value correspondence was given: " + expr);
				}
			}
		}

		// create transitive value correspondences from foreign keys
		List<MappingValueCorrespondence> transitiveValueCorrespondences = createTransitiveValueCorrespondences(this.spicyMappingTransformation
				.getMappingInformation().getValueCorrespondences(), foreignKeys);

		for (MappingValueCorrespondence cond : transitiveValueCorrespondences) {
			mappingInformation.getValueCorrespondences().add(cond);
		}
	}

	private void handleTakeAll(HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys2, MappingInformation mappingInformation,
			Integer targetInputIndex, String targetNesting, Mapping<?> mapping, AggregationExpression aggregationExpression) {
		MappingValueCorrespondence corr;
		
		EvaluationExpression expr = ((ArrayProjection)aggregationExpression.getInputExpression()).getInputExpression();
		
		if (expr instanceof FunctionCall || expr instanceof ArrayAccess || expr instanceof TernaryExpression
				|| expr instanceof ObjectCreation || expr instanceof ArrayCreation
				|| expr instanceof ConstantExpression) {
			corr = handleSpecialExpression(foreignKeys2, mappingInformation, targetInputIndex, targetNesting, mapping,
					expr);
		} else if (expr instanceof ObjectAccess) {
			corr = handleObjectAccess(foreignKeys2, mappingInformation, targetInputIndex, targetNesting, mapping,
					(ObjectAccess) expr, false);
		}  else {
			throw new IllegalArgumentException("No valid value correspondence was given: " + expr);
		}
		corr.setTakeAllValuesOfGrouping(true);
	}

	private MappingValueCorrespondence handleObjectAccess(HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys, final MappingInformation mappingInformation,
			final Integer targetInputIndex, final String targetNesting, final Mapping<?> mapping, final ObjectAccess expr, boolean skipValueCorrespondence) {
		MappingValueCorrespondence corr = null;
		String sourceNesting;

		final EvaluationExpression sourceInputExpr = expr.getInputExpression();
		final String sourceExpr = expr.getField();

		final String targetExpr = mapping.getTarget().toString();
		
		// TODO: better way?
		// sourceInputExpr.findFirst(InputSelection.class).getIndex();
		final Integer fkSource;
		if(sourceInputExpr.findFirst(InputSelection.class)!=null){
			fkSource = sourceInputExpr.findFirst(InputSelection.class).getIndex();
		}else{
			fkSource =  Integer.parseInt(sourceInputExpr.toString().replaceAll("[^0-9]", ""));
		}
		
		sourceNesting = this.createNesting(EntityMapping.sourceStr, fkSource);

		if (sourceInputExpr.toString().contains(this.getClass().getSimpleName())) {
			
			MappingJoinCondition targetJoinCondition;
			// TODO:
			// better
			// way?
			//sourceInputExpr.findFirst(InputSelection.class).getIndex();
			sourceNesting = this.createNesting(EntityMapping.targetStr, fkSource);

			// create join condition for foreign keys, but no value
			// correspondence
			targetJoinCondition = this.createJoinCondition(targetNesting, targetExpr, sourceNesting, sourceExpr);
			mappingInformation.getTargetJoinConditions().add(targetJoinCondition);

			// store foreign keys to add missing (transitive) value
			// correspondences later
			foreignKeys.put(targetJoinCondition.getFromPaths().get(0), targetJoinCondition.getToPaths().get(0));

		} else { // no foreign key

			// add source attribute to source schema
			this.extendSourceSchemaBy(sourceExpr, sourceInputExpr.findFirst(InputSelection.class).getIndex());

			// create value correspondence: source attribute ->
			// target
			// attribute
			if (!skipValueCorrespondence) {
				corr = this.createValueCorrespondence(sourceNesting, sourceExpr, targetNesting, targetExpr);
				mappingInformation.getValueCorrespondences().add(corr);
			}
		}

		// add target attribute to target schema
		this.extendTargetSchemaBy(targetExpr, targetInputIndex);

		// add join attributes to source schema
		for(MappingJoinCondition cond : mappingInformation.getSourceJoinConditions()) {

			String joinAttr = cond.getFromPaths().get(0).getLastStep();
			String joinSource = cond.getFromPaths().get(0).getFirstStep();

			if (joinSource.contains(fkSource.toString()))
				this.extendSourceSchemaBy(joinAttr, fkSource);

			joinAttr = cond.getToPaths().get(0).getLastStep();
			joinSource = cond.getToPaths().get(0).getFirstStep();

			if (joinSource.contains(fkSource.toString()))
				this.extendSourceSchemaBy(joinAttr, fkSource);
		}
		return corr;
	}

	private MappingValueCorrespondence handleSpecialExpression(HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys, final MappingInformation mappingInformation,
			final Integer targetInputIndex, final String targetNesting, final Mapping<?> mapping, EvaluationExpression expr) {
		List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();
		for (ObjectAccess oa : expr.findAll(ObjectAccess.class)) {
			handleObjectAccessInSpecialExpression(foreignKeys, targetInputIndex, targetNesting, mappingInformation, mapping, oa, sourcePaths);
		}
		this.extendTargetSchemaBy(mapping.getTarget().toString(), targetInputIndex);
		MappingValueCorrespondence corr = this.createValueCorrespondence(sourcePaths, targetNesting, mapping.getTarget().toString(), expr);
		mappingInformation.getValueCorrespondences().add(corr);
		return corr;
	}
	
	private void handleObjectAccessInSpecialExpression(HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys, Integer targetInputIndex, String targetNesting, MappingInformation mappingInformation, Mapping<?> mapping, ObjectAccess oa, List<SpicyPathExpression> sourcePaths) {
		String sourceNesting;
		handleObjectAccess(foreignKeys, mappingInformation, targetInputIndex, targetNesting, mapping, oa, true);
		
		final EvaluationExpression sourceInputExpr = oa.getInputExpression();
		final String sourceExpr = oa.getField();

		//TODO this does not work as expected
		if (sourceInputExpr.toString().contains(this.getClass().getSimpleName())) {
			Integer fkSource =  Integer.parseInt(sourceInputExpr.toString().replaceAll("[^0-9]", ""));
			sourceNesting = this.createNesting(EntityMapping.targetStr, fkSource);
			sourcePaths.add(new SpicyPathExpression(sourceNesting, sourceExpr));
		}else{
			sourceNesting = this.createNesting(EntityMapping.sourceStr, sourceInputExpr.findFirst(InputSelection.class).getIndex());
			sourcePaths.add(new SpicyPathExpression(sourceNesting, sourceExpr));
		}
	}

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
		result = prime * result + this.spicyMappingTransformation.hashCode();
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
		return this.spicyMappingTransformation.equals(other.spicyMappingTransformation);
	}

	private void createDefaultSourceSchema(final int size) {
		this.spicyMappingTransformation.getMappingInformation().setSourceSchema(new MappingSchema(size, EntityMapping.sourceStr));
	}

	private void createDefaultTargetSchema(final int size) {
		this.spicyMappingTransformation.getMappingInformation().getTarget().setTargetSchema(new MappingSchema(size, EntityMapping.targetStr));

	}

	private void extendSourceSchemaBy(final String attr, final Integer inputIndex) {
		this.spicyMappingTransformation.getMappingInformation().getSourceSchema().addKeyToInput(inputIndex, attr);
	}

	private void extendTargetSchemaBy(final String attr, final Integer inputStr) {
		this.spicyMappingTransformation.getMappingInformation().getTarget().getTargetSchema().addKeyToInput(inputStr, attr);
	}

	private String createNesting(final String type, final Integer inputIndex) {

		final StringBuilder builder = new StringBuilder().append(type).append(separator).append(EntityMapping.entitiesStr).append(inputIndex)
				.append(separator).append(EntityMapping.entityStr).append(inputIndex);

		return builder.toString();
	}

	private MappingValueCorrespondence createValueCorrespondence(final String sourceNesting, final String sourceAttr, final String targetNesting,
			final String targetAttr) {
		final SpicyPathExpression sourceSteps = new SpicyPathExpression(sourceNesting, sourceAttr);
		final SpicyPathExpression targetSteps = new SpicyPathExpression(targetNesting, targetAttr);

		return new MappingValueCorrespondence(sourceSteps, targetSteps);
	}
	
	private MappingValueCorrespondence createValueCorrespondence(final List<SpicyPathExpression> sourcePaths, final String targetNesting,
			final String targetAttr, EvaluationExpression expr) {
		final SpicyPathExpression targetSteps = new SpicyPathExpression(targetNesting, targetAttr);

		return new MappingValueCorrespondence(sourcePaths, targetSteps, expr);
	}

	private MappingJoinCondition createJoinCondition(final String sourceNesting, final String sourceAttr, final String targetNesting, final String targetAttr) {
		MappingJoinCondition joinCondition = new MappingJoinCondition(Collections.singletonList(new SpicyPathExpression(sourceNesting, sourceAttr)),
				Collections.singletonList(new SpicyPathExpression(targetNesting, targetAttr)), true, true);

		return joinCondition;
	}

	private List<MappingValueCorrespondence> createTransitiveValueCorrespondences(Set<MappingValueCorrespondence> valueCorrespondences,
			HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys) {

		List<MappingValueCorrespondence> transitiveValueCorrespondences = new ArrayList<MappingValueCorrespondence>();
		for (SpicyPathExpression fk : foreignKeys.keySet()) {
			SpicyPathExpression value = foreignKeys.get(fk);

			for (MappingValueCorrespondence mvc : valueCorrespondences) {
				// we use a real ValueCorrespondence here, because the container
				// type MappingValueCorrespondence only stores one single
				// sourcePath
				if (mvc.getTargetPath().equals(value)) {
					MappingValueCorrespondence correspondence = new MappingValueCorrespondence(mvc.getSourcePaths().get(0), fk);
					transitiveValueCorrespondences.add(correspondence);
				}
			}
		}

		return transitiveValueCorrespondences;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.Operator#appendAsString(java.lang.Appendable
	 * )
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		this.spicyMappingTransformation.getMappingInformation().appendAsString(appendable);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator. SopremoModule,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(final SopremoModule module) {
		Map<String, Integer> inputIndex = new HashMap<String, Integer>();
		for (int i = 0; i < this.getNumInputs(); i++) {
			inputIndex.put(entitiesStr + i, i);
		}
		Map<String, Integer> outputIndex = new HashMap<String, Integer>();
		// FIXME hack to solve #output problem
		this.getOutputs();

		for (int i = 0; i < this.getNumOutputs(); i++) {
			outputIndex.put(entitiesStr + i, i);
		}
		this.spicyMappingTransformation.setInputIndex(inputIndex);
		this.spicyMappingTransformation.setOutputIndex(outputIndex);
		this.spicyMappingTransformation.addImplementation(module);
	}
}