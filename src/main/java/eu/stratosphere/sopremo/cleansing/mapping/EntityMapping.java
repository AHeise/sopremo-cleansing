package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.NestedOperatorExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Claudia Lehmann, Arvid Heise, Fabian Tschirschnitz, Tommy Neubert
 */
@Name(noun = "map entities from")
@InputCardinality(min = 1)
@OutputCardinality(min = 1)
public class EntityMapping extends CompositeOperator<EntityMapping> {

	protected static final String type = "XML";

	protected static final String targetStr = "target";

	protected static final String sourceStr = "source";

	protected static final String entitiesStr = "entities_";

	protected static final String entityStr = "entity_";

	protected static final String idStr = "id";

	protected static final String inputPrefixStr = "in";

	protected static INode dummy = new LeafNode("dummy");

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

		final List<String> inputs = new ArrayList<String>();
		final HashMap<String, String> joinParts = new HashMap<String, String>();

		for (final ChildIterator it = assignment.iterator(); it.hasNext();) {
			final EvaluationExpression expr = it.next();
			// just one key relationship
			if (expr instanceof ArrayAccess) {
				evaluateKeyRelationship(inputs, joinParts, expr);
				// multiple AND-connected relationships
			} else {
				for (final ChildIterator it2 = expr.iterator(); it2.hasNext();) {
					evaluateKeyRelationship(inputs, joinParts, it2.next());
				}
			}
		}

		final String sourceNesting = this.createNesting(
			EntityMapping.sourceStr, inputs.get(0));
		final String targetNesting = this.createNesting(
			EntityMapping.sourceStr, inputs.get(1));

		this.spicyMappingTransformation.getMappingInformation()
			.setSourceJoinCondition(
				this.createJoinCondition(sourceNesting,
					joinParts.get(inputs.get(0)), targetNesting,
					joinParts.get(inputs.get(1))));
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

	private void evaluateKeyRelationship(final List<String> inputs,
			final HashMap<String, String> joinParts,
			final EvaluationExpression expr) {
		final ArrayAccess arrayAccess = (ArrayAccess) expr;
		final ObjectAccess inputExpr = (ObjectAccess) arrayAccess
			.getInputExpression();
		final String input = inputExpr.getInputExpression().toString();
		final String attr = inputExpr.getField();
		inputs.add(input);
		joinParts.put(input, attr);
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
	@Name(adjective = "as")
	public void setMappingExpression(final ArrayCreation assignment) {

		MappingValueCorrespondence corr = null;

		HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys = new HashMap<SpicyPathExpression, SpicyPathExpression>();

		this.createDefaultSourceSchema(this.getInputs().size());
		this.createDefaultTargetSchema(this.getNumOutputs());

		// iterate over schema mapping groupings
		final MappingInformation mappingInformation = this.spicyMappingTransformation.getMappingInformation();
		for (int index = 0; index < assignment.size(); index++) { // operator
																	// level

			final NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment
				.get(index);
			final Operator<?> operator = nestedOperator.getOperator();
			String[] groupingKey = null;
			try {
				// TODO: remove hack
				groupingKey = ReflectUtil.invoke(operator, "getGroupingKey", 0)
					.toString().split("\\.");
			} catch (Throwable e) {
				e.printStackTrace();
			} // e.g.
				// in0.id
			final String targetInputStr = EntityMapping.inputPrefixStr
				+ String.valueOf(index); // e.g.
			// in0
			final String sourceInputStr = groupingKey[0]; // e.g. in0
			final String keyStr = groupingKey[1]; // e.g. id
			String sourceNesting = this.createNesting(EntityMapping.sourceStr,
				sourceInputStr); // e.g.
			// source.entities_in0.entity_in0
			final String targetNesting = this.createNesting(
				EntityMapping.targetStr, targetInputStr); // e.g.
			// target.entities_in0.entity_in0

			// add source grouping key to source schema
			this.extendSourceSchemaBy(keyStr, sourceInputStr);

			// add target entity id to target schema
			this.extendTargetSchemaBy(EntityMapping.idStr, targetInputStr);

			// create primary key: target entity id
			MappingKeyConstraint targetKey = new MappingKeyConstraint(
				targetNesting, EntityMapping.idStr);
			mappingInformation.getTarget().addKeyConstraint(targetKey);

			// create value correspondence: source grouping key -> target entity
			// id
			corr = this.createValueCorrespondence(sourceNesting, keyStr,
				targetNesting, EntityMapping.idStr);
			mappingInformation
				.getValueCorrespondences().add(corr);

			ObjectCreation resultProjection = null;
			try {
				// TODO: remove hack
				resultProjection = (ObjectCreation) ReflectUtil.invoke(
					operator, "getResultProjection");
			} catch (Throwable e) {
				e.printStackTrace();
			}
			final List<Mapping<?>> mappings = resultProjection.getMappings();
			for (final Mapping<?> mapping : mappings) { // mapping level

				final EvaluationExpression expr = mapping.getExpression(); // TODO:
																			// can
																			// be
																			// expression
																			// or
																			// ObjectCreation
																			// for
																			// nested
																			// object
				final String sourceInputExpr = expr.toString().split("\\.")[0]; // e.g.
																				// in0
				final String sourceExpr = expr.toString().split("\\.")[1]; // e.g.
																			// name_original
				final String targetExpr = mapping.getTarget().toString(); // e.g.
																			// name_person
				sourceNesting = this.createNesting(EntityMapping.sourceStr,
					sourceInputExpr); // e.g.
				// source.entities_in0.entity_in0

				if (sourceInputExpr.contains(this.getClass().getSimpleName())) { // foreign
																					// key

					MappingJoinCondition targetJoinCondition;
					final String fkSource = sourceInputExpr.replaceAll(
						"[^0-9]", ""); // TODO:
										// better
										// way?
					sourceNesting = this.createNesting(EntityMapping.targetStr,
						EntityMapping.inputPrefixStr + fkSource);

					// create join condition for foreign keys, but no value
					// correspondence
					targetJoinCondition =
						this.createJoinCondition(targetNesting, targetExpr, sourceNesting, sourceExpr);
					mappingInformation.getTargetJoinConditions().add(targetJoinCondition);

					// store foreign keys to add missing (transitive) value
					// correspondences later
					foreignKeys.put(targetJoinCondition.getFromPaths().get(0),
						targetJoinCondition.getToPaths().get(0));

				} else { // no foreign key

					// add source attribute to source schema
					this.extendSourceSchemaBy(sourceExpr, sourceInputExpr);

					// create value correspondence: source attribute -> target
					// attribute
					corr = this.createValueCorrespondence(sourceNesting,
						sourceExpr, targetNesting, targetExpr);
					mappingInformation
						.getValueCorrespondences().add(corr);
				}

				// add target attribute to target schema
				this.extendTargetSchemaBy(targetExpr, targetInputStr);

				// add join attributes to source schema
				if (mappingInformation
					.getSourceJoinCondition() != null) {

					String joinAttr = mappingInformation
						.getSourceJoinCondition()
						.getFromPaths()
						.get(0).getLastStep();
					String joinSource = mappingInformation.getSourceJoinCondition()
						.getFromPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr))
						this.extendSourceSchemaBy(joinAttr, sourceInputExpr);

					joinAttr = mappingInformation
						.getSourceJoinCondition()
						.getToPaths()
						.get(0)
						.getLastStep();
					joinSource = mappingInformation.getSourceJoinCondition()
						.getToPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr))
						this.extendSourceSchemaBy(joinAttr, sourceInputExpr);
				}
			}
		}

		// create transitive value correspondences from foreign keys
		List<MappingValueCorrespondence> transitiveValueCorrespondences = createTransitiveValueCorrespondences(
				this.spicyMappingTransformation.getMappingInformation()
						.getValueCorrespondences(), foreignKeys);

		for (MappingValueCorrespondence cond : transitiveValueCorrespondences) {
			mappingInformation
				.getValueCorrespondences().add(cond);
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
		this.spicyMappingTransformation.getMappingInformation()
			.setSourceSchema(
				new MappingSchema(size, EntityMapping.sourceStr));
	}

	private void createDefaultTargetSchema(final int size) {
		this.spicyMappingTransformation
			.getMappingInformation()
			.getTarget()
			.setTargetSchema(
				new MappingSchema(size, EntityMapping.targetStr));

	}

	private void extendSourceSchemaBy(final String attr, final String inputStr) {
		this.spicyMappingTransformation.getMappingInformation()
			.getSourceSchema().addKeyToInput(inputStr, attr);
	}

	private void extendTargetSchemaBy(final String attr, final String inputStr) {
		this.spicyMappingTransformation.getMappingInformation().getTarget()
			.getTargetSchema().addKeyToInput(inputStr, attr);
	}

	private String createNesting(final String type, final String count) {

		final String separator = ".";

		final StringBuilder builder = new StringBuilder().append(type)
			.append(separator).append(EntityMapping.entitiesStr)
			.append(count).append(separator)
			.append(EntityMapping.entityStr).append(count);

		return builder.toString();
	}

	private MappingValueCorrespondence createValueCorrespondence(
			final String sourceNesting, final String sourceAttr,
			final String targetNesting, final String targetAttr) {
		final SpicyPathExpression sourceSteps = new SpicyPathExpression(sourceNesting, sourceAttr);
		final SpicyPathExpression targetSteps = new SpicyPathExpression(targetNesting, targetAttr);

		return new MappingValueCorrespondence(sourceSteps, targetSteps);
	}

	private MappingJoinCondition createJoinCondition(
			final String sourceNesting, final String sourceAttr,
			final String targetNesting, final String targetAttr) {
		MappingJoinCondition joinCondition = new MappingJoinCondition(
			Collections.singletonList(new SpicyPathExpression(sourceNesting, sourceAttr)),
			Collections.singletonList(new SpicyPathExpression(targetNesting, targetAttr)),
			true, true);

		return joinCondition;
	}

	private List<MappingValueCorrespondence> createTransitiveValueCorrespondences(
			List<MappingValueCorrespondence> valueCorrespondences,
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.Operator#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		this.spicyMappingTransformation.getMappingInformation().appendAsString(appendable);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator. SopremoModule,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(final SopremoModule module,
			final EvaluationContext context) {
		Map<String, Integer> inputIndex = new HashMap<String, Integer>();
		for (int i = 0; i < this.getNumInputs(); i++) {
			inputIndex.put(entitiesStr + inputPrefixStr + i, i);
		}
		Map<String, Integer> outputIndex = new HashMap<String, Integer>();
		//FIXME hack to solve #output problem 
		this.getOutputs();
		
		for (int i = 0; i < this.getNumOutputs(); i++) {
			outputIndex.put(entitiesStr + inputPrefixStr + i, i);
		}
		this.spicyMappingTransformation.setInputIndex(inputIndex);
		this.spicyMappingTransformation.setOutputIndex(outputIndex);
		this.spicyMappingTransformation.addImplementation(module, context);
	}
}