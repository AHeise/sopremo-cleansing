package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.datasource.KeyConstraint;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;
import it.unibas.spicy.model.datasource.nodes.SequenceNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.paths.PathExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Grouping;
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
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;

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
	private static final String entitiesStr = "entities_";
	private static final String entityStr = "entity_";
	private static final String idStr = "id";
	private static final String inputPrefixStr = "in";
	private static INode dummy = new LeafNode("dummy");

	private SpicyMappingTransformation spicyMappingTransformation = new SpicyMappingTransformation();

	@Property
	@Name(preposition = "where")
	public void setJoinConditions(final BooleanExpression assignment) {

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

		final String sourceNesting = this.createNesting(EntityMapping.sourceStr, inputs.get(0));
		final String targetNesting = this.createNesting(EntityMapping.sourceStr, inputs.get(1));

		this.spicyMappingTransformation.getMappingInformation().setSourceJoinCondition(
				this.createJoinCondition(sourceNesting, joinParts.get(inputs.get(0)), targetNesting, joinParts.get(inputs.get(1))));
	}

	private void evaluateKeyRelationship(final List<String> inputs, final HashMap<String, String> joinParts, final EvaluationExpression expr) {
		final ArrayAccess arrayAccess = (ArrayAccess) expr;
		final ObjectAccess inputExpr = (ObjectAccess) arrayAccess.getInputExpression();
		final String input = inputExpr.getInputExpression().toString();
		final String attr = inputExpr.getField();
		inputs.add(input);
		joinParts.put(input, attr);
	}

	/**
	 * Mapping Task: value correspondences: mappings and grouping keys source
	 * and target schema: attributes in mappings and grouping keys target keys:
	 * grouping keys join conditions with foreign keys: where-clause and foreign
	 * keys
	 * 
	 */
	@Property
	@Name(adjective = "as")
	public void setMappingExpression(final ArrayCreation assignment) {

		ValueCorrespondence corr = null;

		HashMap<String, String> foreignKeys = new HashMap<String, String>();

		this.createDefaultSourceSchema(this.getInputs().size());
		this.createDefaultTargetSchema(this.getNumOutputs());

		// iterate over schema mapping groupings
		for (int index = 0; index < assignment.size(); index++) { // operator
																	// level

			final NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment.get(index);
			final Grouping operator = (Grouping) nestedOperator.getOperator();
			final String[] groupingKey = operator.getGroupingKey(0).toString().split("\\."); // e.g.
																								// in0.id
			final String targetInputStr = EntityMapping.inputPrefixStr + String.valueOf(index); // e.g.
			// in0
			final String sourceInputStr = groupingKey[0]; // e.g. in0
			final String keyStr = groupingKey[1]; // e.g. id
			String sourceNesting = this.createNesting(EntityMapping.sourceStr, sourceInputStr); // e.g.
			// source.entities_in0.entity_in0
			final String targetNesting = this.createNesting(EntityMapping.targetStr, targetInputStr); // e.g.
			// target.entities_in0.entity_in0

			// add source grouping key to source schema
			this.extendSourceSchemaBy(keyStr, sourceInputStr);

			// add target entity id to target schema
			this.extendTargetSchemaBy(EntityMapping.idStr, targetInputStr);

			// create primary key: target entity id
			KeyConstraint targetKey = this.createKeyConstraints(targetNesting, EntityMapping.idStr);
			this.spicyMappingTransformation.getMappingInformation().getTarget().addKeyConstraint(targetKey);

			// create value correspondence: source grouping key -> target entity
			// id
			corr = this.createValueCorrespondence(sourceNesting, keyStr, targetNesting, EntityMapping.idStr);
			this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences().add(corr);

			final ObjectCreation resultProjection = (ObjectCreation) operator.getResultProjection();
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
				sourceNesting = this.createNesting(EntityMapping.sourceStr, sourceInputExpr); // e.g.
				// source.entities_in0.entity_in0

				if (sourceInputExpr.contains(this.getClass().getSimpleName())) { // foreign
																					// key

					JoinCondition targetJoinCondition;
					final String fkSource = sourceInputExpr.replaceAll("[^0-9]", ""); // TODO:
																						// better
																						// way?
					sourceNesting = this.createNesting(EntityMapping.targetStr, EntityMapping.inputPrefixStr + fkSource);

					// create join condition for foreign keys, but no value
					// correspondence
					targetJoinCondition = this.createJoinCondition(targetNesting, targetExpr, sourceNesting, sourceExpr);
					this.spicyMappingTransformation.getMappingInformation().getTargetJoinConditions().add(targetJoinCondition);

					// store foreign keys to add missing (transitive) value
					// correspondences later
					foreignKeys.put(targetJoinCondition.getFromPaths().get(0).toString(), targetJoinCondition.getToPaths().get(0).toString());

				} else { // no foreign key

					// add source attribute to source schema
					this.extendSourceSchemaBy(sourceExpr, sourceInputExpr);

					// create value correspondence: source attribute -> target
					// attribute
					corr = this.createValueCorrespondence(sourceNesting, sourceExpr, targetNesting, targetExpr);
					this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences().add(corr);
				}

				// add target attribute to target schema
				this.extendTargetSchemaBy(targetExpr, targetInputStr);

				// add join attributes to source schema
				if (this.spicyMappingTransformation.getMappingInformation().getSourceJoinCondition() != null) {

					String joinAttr = this.spicyMappingTransformation.getMappingInformation().getSourceJoinCondition().getFromPaths().get(0).getLastStep();
					String joinSource = this.spicyMappingTransformation.getMappingInformation().getSourceJoinCondition().getFromPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr))
						this.extendSourceSchemaBy(joinAttr, sourceInputExpr);

					joinAttr = this.spicyMappingTransformation.getMappingInformation().getSourceJoinCondition().getToPaths().get(0).getLastStep();
					joinSource = this.spicyMappingTransformation.getMappingInformation().getSourceJoinCondition().getToPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr))
						this.extendSourceSchemaBy(joinAttr, sourceInputExpr);
				}
			}
		}

		// create transitive value correspondences from foreign keys
		List<ValueCorrespondence> transitiveValueCorrespondences = createTransitiveValueCorrespondences(corr, this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences(), foreignKeys);

		for (ValueCorrespondence cond : transitiveValueCorrespondences) {
			this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences().add(cond);
		}
	}

	private void createDefaultSourceSchema(final int size) {
		INode sourceEntities;

		// source : SequenceNode
		// entities_in0 : SetNode
		// entity_in0 : SequenceNode
		// entities_in1 : SetNode
		// entity_in1 : SequenceNode

		for (int index = 0; index < size; index++) {
			final String input = EntityMapping.inputPrefixStr + String.valueOf(index);
			this.spicyMappingTransformation.getMappingInformation().setSourceEntity(new SequenceNode(EntityMapping.entityStr + input));
			sourceEntities = new SetNode(EntityMapping.entitiesStr + input);
			sourceEntities.addChild(this.spicyMappingTransformation.getMappingInformation().getSourceEntity());
			this.spicyMappingTransformation.getMappingInformation().getSourceSchema().addChild(sourceEntities);
		}
		this.spicyMappingTransformation.getMappingInformation().getSourceSchema().setRoot(true);
	}

	private void createDefaultTargetSchema(final int size) {
		INode targetEntities;

		// target : SequenceNode
		// entities_in0 : SetNode
		// entity_in0 : SequenceNode
		// entities_in1 : SetNode
		// entity_in1 : SequenceNode

		for (int index = 0; index < size; index++) {
			final String input = EntityMapping.inputPrefixStr + String.valueOf(index);
			this.spicyMappingTransformation.getMappingInformation().setTargetEntity(new SequenceNode(EntityMapping.entityStr + input));
			targetEntities = new SetNode(EntityMapping.entitiesStr + input);
			targetEntities.addChild(this.spicyMappingTransformation.getMappingInformation().getTargetEntity());
			this.spicyMappingTransformation.getMappingInformation().getTargetSchema().addChild(targetEntities);
		}
		this.spicyMappingTransformation.getMappingInformation().getTargetSchema().setRoot(true);
	}

	private void extendSourceSchemaBy(final String attr, final String inputStr) {
		INode sourceAttr;

		this.spicyMappingTransformation.getMappingInformation().setSourceEntity(
				this.spicyMappingTransformation.getMappingInformation().getSourceSchema().getChild(EntityMapping.entitiesStr + inputStr)
						.getChild(EntityMapping.entityStr + inputStr));
		if (this.spicyMappingTransformation.getMappingInformation().getSourceEntity().getChild(attr) == null) {
			sourceAttr = new AttributeNode(attr);
			sourceAttr.addChild(EntityMapping.dummy);
			this.spicyMappingTransformation.getMappingInformation().getSourceEntity().addChild(sourceAttr);
		}
	}

	private void extendTargetSchemaBy(final String attr, final String inputStr) {
		INode targetAttr;
		this.spicyMappingTransformation.getMappingInformation().setTargetEntity(
				this.spicyMappingTransformation.getMappingInformation().getTargetSchema().getChild(EntityMapping.entitiesStr + inputStr)
						.getChild(EntityMapping.entityStr + inputStr));
		if (this.spicyMappingTransformation.getMappingInformation().getTargetEntity().getChild(attr) == null) {
			targetAttr = new AttributeNode(attr);
			targetAttr.addChild(EntityMapping.dummy);
			this.spicyMappingTransformation.getMappingInformation().getTargetEntity().addChild(targetAttr);
		}
	}

	private String createNesting(final String type, final String count) {

		final String separator = ".";

		final StringBuilder builder = new StringBuilder().append(type).append(separator).append(EntityMapping.entitiesStr).append(count).append(separator)
				.append(EntityMapping.entityStr).append(count);

		return builder.toString();
	}

	private ValueCorrespondence createValueCorrespondence(final String sourceNesting, final String sourceAttr, final String targetNesting,
			final String targetAttr) {

		final List<String> sourceSteps = new ArrayList<String>();
		final List<String> targetSteps = new ArrayList<String>();

		sourceSteps.add(sourceNesting);
		sourceSteps.add(sourceAttr);
		targetSteps.add(targetNesting);
		targetSteps.add(targetAttr);

		final PathExpression sourcePath = new PathExpression(sourceSteps);
		final PathExpression targetPath = new PathExpression(targetSteps);

		return new ValueCorrespondence(sourcePath, targetPath);
	}

	private ValueCorrespondence createValueCorrespondence(final String source, final String target) {

		final List<String> sourceSteps = new ArrayList<String>();
		final List<String> targetSteps = new ArrayList<String>();

		sourceSteps.add(source);
		targetSteps.add(target);

		final PathExpression sourcePath = new PathExpression(sourceSteps);
		final PathExpression targetPath = new PathExpression(targetSteps);

		return new ValueCorrespondence(sourcePath, targetPath);
	}

	private KeyConstraint createKeyConstraints(final String nesting, final String attr) {

		final List<String> list = new ArrayList<String>();
		list.add(nesting);
		list.add(attr);

		final PathExpression key = new PathExpression(list);

		final List<PathExpression> keyPath = new ArrayList<PathExpression>();
		keyPath.add(key);

		return new KeyConstraint(keyPath, true);
	}

	private JoinCondition createJoinCondition(final String sourceNesting, final String sourceAttr, final String targetNesting, final String targetAttr) {

		final List<PathExpression> fromPaths = this.createPaths(sourceNesting, sourceAttr);
		final List<PathExpression> toPaths = this.createPaths(targetNesting, targetAttr);

		final JoinCondition joinCondition = new JoinCondition(fromPaths, toPaths, true);
		joinCondition.setMandatory(true);
		joinCondition.setMonodirectional(true);

		return joinCondition;
	}

	private List<PathExpression> createPaths(final String nesting, final String attr) {

		final List<String> list = new ArrayList<String>();
		list.add(nesting);
		list.add(attr);

		final PathExpression path = new PathExpression(list);

		final List<PathExpression> pathList = new ArrayList<PathExpression>();
		pathList.add(path);

		return pathList;
	}

	private List<ValueCorrespondence> createTransitiveValueCorrespondences(ValueCorrespondence corr, List<ValueCorrespondence> valueCorrespondences,
			HashMap<String, String> foreignKeys) {

		List<ValueCorrespondence> transitiveValueCorrespondences = new ArrayList<ValueCorrespondence>();
		for (String fk : foreignKeys.keySet()) {
			String value = foreignKeys.get(fk).toString();

			for (ValueCorrespondence vc : valueCorrespondences) {
				if (vc.getTargetPath().toString().equals(value)) {

					for (PathExpression pe : vc.getSourcePaths()) {
						corr = this.createValueCorrespondence(pe.toString(), fk);
						transitiveValueCorrespondences.add(corr);
					}
				}
			}
		}

		return transitiveValueCorrespondences;
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
	public void addImplementation(final SopremoModule module, final EvaluationContext context) {
		this.spicyMappingTransformation.addImplementation(module, context);
	}
}