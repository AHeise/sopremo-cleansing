package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.algebra.IAlgebraOperator;
import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.datasource.DataSource;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.datasource.KeyConstraint;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;
import it.unibas.spicy.model.datasource.nodes.SequenceNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.mapping.MappingData;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.persistence.DAOMappingTaskTgds;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.io.Files;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.cleansing.mapping.EntityMapping.SchemaMappingSerializer;
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
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * @author Claudia Lehmann, Arvid Heise
 */
@Name(noun = "map entities from")
@InputCardinality(min = 1)
@OutputCardinality(min = 1)
@DefaultSerializer(SchemaMappingSerializer.class)
public class EntityMapping extends CompositeOperator<EntityMapping> {
	public static class SchemaMappingSerializer extends Serializer<EntityMapping> {

		@SuppressWarnings("unchecked")
		@Override
		public EntityMapping read(final Kryo kryo, final Input input, final Class<EntityMapping> type) {

			EntityMapping schemaMapping = (EntityMapping) kryo.getRegistration(EntityMapping.class.getSuperclass()).getSerializer()
					.read(kryo, input, EntityMapping.class);
			try {
				final DAOMappingTaskTgds tgds = new DAOMappingTaskTgds();
				final File filePath = File.createTempFile("mapping", ".tgd");
				int length = input.readInt(true);
				Files.write(input.readBytes(length), filePath);
				MappingTask task = tgds.loadMappingTask(filePath.getAbsolutePath());
				schemaMapping.mappingTask = task;
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}

			return schemaMapping;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void write(final Kryo kryo, final com.esotericsoftware.kryo.io.Output output, final EntityMapping schemaMapping) {

			kryo.getRegistration(EntityMapping.class.getSuperclass()).getSerializer().write(kryo, output, schemaMapping);
			try {
				// final DAOMappingTaskLines lines = new DAOMappingTaskLines();
				final DAOMappingTaskTgds tgds = new DAOMappingTaskTgds();
				final File filePath = File.createTempFile("mapping", ".tgd");
				tgds.saveMappingTask(schemaMapping.mappingTask, filePath.getAbsolutePath());
				byte[] mappingBytes = Files.toByteArray(filePath);
				output.writeInt(mappingBytes.length, true);
				output.write(mappingBytes);
				filePath.delete();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public EntityMapping copy(final Kryo kryo, final EntityMapping original) {

			EntityMapping schemaMapping = new EntityMapping();
			try {
				final DAOMappingTaskTgds tgds = new DAOMappingTaskTgds();
				final File filePath = File.createTempFile("mapping", ".tgd");
				tgds.saveMappingTask(original.mappingTask, filePath.getAbsolutePath());
				MappingTask task = tgds.loadMappingTask(filePath.getAbsolutePath());
				schemaMapping.mappingTask = task;
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}

			return schemaMapping;
		}
	}

	private MappingTask mappingTask;

	private static final String type = "XML";
	private static final String targetStr = "target";
	private static final String sourceStr = "source";
	private static final String entitiesStr = "entities_";
	private static final String entityStr = "entity_";
	private static final String idStr = "id";
	private static final String inputPrefixStr = "in";
	private static String classStr;

	private INode dummy = new LeafNode("dummy");
	private INode sourceAttr;
	private INode targetAttr;
	private INode sourceEntity;
	private INode targetEntity;
	private INode sourceEntities;
	private INode targetEntities;
	private INode sourceSchema = new SequenceNode(EntityMapping.sourceStr);
	private INode targetSchema = new SequenceNode(EntityMapping.targetStr);

	private DataSource source = new DataSource(type, this.sourceSchema);
	private DataSource target = new DataSource(type, this.targetSchema);

	private JoinCondition sourceJoinCondition;
	private JoinCondition targetJoinCondition;
	private List<JoinCondition> targetJoinConditions = new ArrayList<JoinCondition>();

	private List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();
	private ValueCorrespondence corr;

	private KeyConstraint targetKey;
	private HashMap<String, String> foreignKeys = new HashMap<String, String>();

	@Property
	@Name(preposition = "where")
	public void setJoinConditions(final BooleanExpression assignment) {

		final List<String> inputs = new ArrayList<String>();
		final HashMap<String, String> joinParts = new HashMap<String, String>();

		for (final ChildIterator it = assignment.iterator(); it.hasNext();) {
			final EvaluationExpression expr = it.next();
			//just one key relationship
			if (expr instanceof ArrayAccess) {
				evaluateKeyRelationship(inputs, joinParts, expr);
			//multiple AND-connected relationships
			} else {
				for (final ChildIterator it2 = expr.iterator(); it2.hasNext();) {
					evaluateKeyRelationship(inputs, joinParts, it2.next());
				}
			}
		}

		final String sourceNesting = this.createNesting(EntityMapping.sourceStr, inputs.get(0));
		final String targetNesting = this.createNesting(EntityMapping.sourceStr, inputs.get(1));

		this.sourceJoinCondition = this.createJoinCondition(sourceNesting, joinParts.get(inputs.get(0)), targetNesting, joinParts.get(inputs.get(1)));
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
		EntityMapping.classStr = this.getClass().getSimpleName();

		this.createDefaultSourceSchema(this.getInputs().size());
		this.createDefaultTargetSchema(this.getNumOutputs());
		
		//iterate over schema mapping groupings
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
			this.targetKey = this.createKeyConstraints(targetNesting, EntityMapping.idStr);
			this.target.addKeyConstraint(this.targetKey);

			// create value correspondence: source grouping key -> target entity
			// id
			this.corr = this.createValueCorrespondence(sourceNesting, keyStr, targetNesting, EntityMapping.idStr);
			this.valueCorrespondences.add(this.corr);

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

				if (sourceInputExpr.contains(EntityMapping.classStr)) { // foreign key

					final String fkSource = sourceInputExpr.replaceAll("[^0-9]", ""); // TODO:
																						// better
																						// way?
					sourceNesting = this.createNesting(EntityMapping.targetStr, EntityMapping.inputPrefixStr + fkSource);

					// create join condition for foreign keys, but no value
					// correspondence
					this.targetJoinCondition = this.createJoinCondition(targetNesting, targetExpr, sourceNesting, sourceExpr);
					targetJoinConditions.add(targetJoinCondition);

					// store foreign keys to add missing (transitive) value
					// correspondences later
					this.foreignKeys.put(targetJoinCondition.getFromPaths().get(0).toString(), targetJoinCondition.getToPaths().get(0).toString());

				} else { // no foreign key

					// add source attribute to source schema
					this.extendSourceSchemaBy(sourceExpr, sourceInputExpr);

					// create value correspondence: source attribute -> target
					// attribute
					this.corr = this.createValueCorrespondence(sourceNesting, sourceExpr, targetNesting, targetExpr);
					this.valueCorrespondences.add(this.corr);
				}

				// add target attribute to target schema
				this.extendTargetSchemaBy(targetExpr, targetInputStr);

				// add join attributes to source schema
				if (this.sourceJoinCondition != null) {

					String joinAttr = this.sourceJoinCondition.getFromPaths().get(0).getLastStep();
					String joinSource = this.sourceJoinCondition.getFromPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr))
						this.extendSourceSchemaBy(joinAttr, sourceInputExpr);

					joinAttr = this.sourceJoinCondition.getToPaths().get(0).getLastStep();
					joinSource = this.sourceJoinCondition.getToPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr))
						this.extendSourceSchemaBy(joinAttr, sourceInputExpr);
				}
			}
		}

		// create transitive value correspondences from foreign keys
		List<ValueCorrespondence> transitiveValueCorrespondences = createTransitiveValueCorrespondences();

		for (ValueCorrespondence cond : transitiveValueCorrespondences) {
			valueCorrespondences.add(cond);
		}

		// create mapping task
		this.mappingTask = new MappingTask(this.source, this.target, this.valueCorrespondences);
		if (this.sourceJoinCondition != null)
			this.mappingTask.getSourceProxy().addJoinCondition(this.sourceJoinCondition);

		for (JoinCondition cond : targetJoinConditions) {
			this.mappingTask.getTargetProxy().addJoinCondition(cond);
		}

		SopremoUtil.LOG.debug("mapping task:\n" + this.mappingTask);
	}

//	public MappingTask getMappingTask() {
//		return mappingTask;
//	}
	
	public MappingTask getMappingTask() {
		return mappingTask;
	}

	public void setMappingTask(MappingTask mappingTask) {
		this.mappingTask = mappingTask;
	}

	private void createDefaultSourceSchema(final int size) {

		// source : SequenceNode
		// entities_in0 : SetNode
		// entity_in0 : SequenceNode
		// entities_in1 : SetNode
		// entity_in1 : SequenceNode

		for (int index = 0; index < size; index++) {
			final String input = EntityMapping.inputPrefixStr + String.valueOf(index);
			this.sourceEntity = new SequenceNode(EntityMapping.entityStr + input);
			this.sourceEntities = new SetNode(EntityMapping.entitiesStr + input);
			this.sourceEntities.addChild(this.sourceEntity);
			this.sourceSchema.addChild(this.sourceEntities);
		}
		this.sourceSchema.setRoot(true);
	}

	private void createDefaultTargetSchema(final int size) {

		// target : SequenceNode
		// entities_in0 : SetNode
		// entity_in0 : SequenceNode
		// entities_in1 : SetNode
		// entity_in1 : SequenceNode

		for (int index = 0; index < size; index++) {
			final String input = EntityMapping.inputPrefixStr + String.valueOf(index);
			this.targetEntity = new SequenceNode(EntityMapping.entityStr + input);
			this.targetEntities = new SetNode(EntityMapping.entitiesStr + input);
			this.targetEntities.addChild(this.targetEntity);
			this.targetSchema.addChild(this.targetEntities);
		}
		this.targetSchema.setRoot(true);
	}

	private void extendSourceSchemaBy(final String attr, final String inputStr) {

		this.sourceEntity = this.sourceSchema.getChild(EntityMapping.entitiesStr + inputStr).getChild(EntityMapping.entityStr + inputStr);
		if (this.sourceEntity.getChild(attr) == null) {
			this.sourceAttr = new AttributeNode(attr);
			this.sourceAttr.addChild(this.dummy);
			this.sourceEntity.addChild(this.sourceAttr);
		}
	}

	private void extendTargetSchemaBy(final String attr, final String inputStr) {

		this.targetEntity = this.targetSchema.getChild(EntityMapping.entitiesStr + inputStr).getChild(EntityMapping.entityStr + inputStr);
		if (this.targetEntity.getChild(attr) == null) {
			this.targetAttr = new AttributeNode(attr);
			this.targetAttr.addChild(this.dummy);
			this.targetEntity.addChild(this.targetAttr);
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

	private List<ValueCorrespondence> createTransitiveValueCorrespondences() {

		List<ValueCorrespondence> transitiveValueCorrespondences = new ArrayList<ValueCorrespondence>();
		for (String fk : this.foreignKeys.keySet()) {
			String value = foreignKeys.get(fk).toString();

			for (ValueCorrespondence vc : this.valueCorrespondences) {
				if (vc.getTargetPath().toString().equals(value)) {

					for (PathExpression pe : vc.getSourcePaths()) {
						this.corr = this.createValueCorrespondence(pe.toString(), fk);
						transitiveValueCorrespondences.add(this.corr);
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
		SopremoUtil.trace();
		final MappingData data = this.mappingTask.getMappingData();
		final IAlgebraOperator tree = data.getAlgebraTree();
		SopremoUtil.LOG.debug("generated spicy mapping tree:\n" + tree);

		final SpicyMappingTransformation sopremoOperator = new SpicyMappingTransformation();
		sopremoOperator.setMappingTask(this.mappingTask);

		// TODO: create loop and arbitrary input/output cardinality
		final HashMap<String, Integer> inputIndex = new HashMap<String, Integer>(2);
		inputIndex.put("entities_in0", 0);
		inputIndex.put("entities_in1", 1);
		sopremoOperator.setInputIndex(inputIndex);

		final HashMap<String, Integer> outputIndex = new HashMap<String, Integer>(2);
		outputIndex.put("entities_in0", 0);
		outputIndex.put("entities_in1", 1);
		sopremoOperator.setOutputIndex(outputIndex);

		module.embed(sopremoOperator);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((mappingTask == null) ? 0 : mappingTask.hashCode());
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
		if (mappingTask == null) {
			if (other.mappingTask != null)
				return false;
			else
				return true;
		} else if (other.mappingTask == null) {
			return false;
		}

		return mappingTasksAreEqual(this.mappingTask, other.mappingTask);

	}

	private boolean mappingTasksAreEqual(MappingTask thiz, MappingTask other) {
		//TODO type is not serialized by spicy
//		if(thiz.getType() != other.getType())
//			return false;
		if(!checkCorrespondenceLists(thiz.getValueCorrespondences(), other.getValueCorrespondences())) {
			return false;
		}
		if(!checkCorrespondenceLists(thiz.getCandidateCorrespondences(), other.getCandidateCorrespondences())) {
			return false;
		}
		return true;
	}

	private boolean checkCorrespondenceLists(List<ValueCorrespondence> thisCorrespondences, List<ValueCorrespondence> otherCorrespondences) {
		if(thisCorrespondences.size() != otherCorrespondences.size())
			return false;
		List<ValueCorrespondence> clonedCorrespondences = new ArrayList<ValueCorrespondence>(otherCorrespondences);
		for(ValueCorrespondence correspondence : thisCorrespondences) {
			for(ValueCorrespondence correspondence2 : otherCorrespondences){
				if(correspondencesAreEqual(correspondence, correspondence2)){
					clonedCorrespondences.remove(correspondence2);
					break;
				}
			}
			return false;
		}
		return clonedCorrespondences.isEmpty();
	}

	private boolean correspondencesAreEqual(ValueCorrespondence c1, ValueCorrespondence c2) {
		return  c1.hasEqualPaths(c2);
	}
}