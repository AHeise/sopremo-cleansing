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
import java.util.HashSet;
import java.util.List;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.io.Files;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.cleansing.mapping.SchemaMapping.SchemaMappingSerializer;
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
@InputCardinality(2)	// TODO: @InputCardinality(min = 1, max = Integer.MAX_VALUE)
@OutputCardinality(2)	// TODO: @OutputCardinality(min = 1, max = Integer.MAX_VALUE)
@DefaultSerializer(SchemaMappingSerializer.class)
public class SchemaMapping extends CompositeOperator<SchemaMapping> {
	public static class SchemaMappingSerializer extends Serializer<SchemaMapping> {

		@Override
		public SchemaMapping read(final Kryo kryo, final Input input, final Class<SchemaMapping> type) {

			SchemaMapping schemaMapping = (SchemaMapping) kryo.getRegistration(SchemaMapping.class.getSuperclass()).getSerializer().read(kryo, input, SchemaMapping.class);
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

		@Override
		public void write(final Kryo kryo, final com.esotericsoftware.kryo.io.Output output, final SchemaMapping schemaMapping) {

			kryo.getRegistration(SchemaMapping.class.getSuperclass()).getSerializer().write(kryo, output, schemaMapping);
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
		public SchemaMapping copy(final Kryo kryo, final SchemaMapping original) {
			
			SchemaMapping schemaMapping = new SchemaMapping();
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

	private final String type = "XML";
	private final String targetStr = "target";
	private final String sourceStr = "source";
	private final String entitiesStr = "entities_";
	private final String entityStr = "entity_";
	private final String idStr = "id";
	private final String inputPrefixStr = "in";
	private String classStr;
	private boolean DEBUG = true;

	INode dummy = new LeafNode("dummy");
	INode sourceAttr;
	INode targetAttr;
	INode sourceEntity;
	INode targetEntity;
	INode sourceEntities;
	INode targetEntities;
	INode sourceSchema = new SequenceNode(this.sourceStr);
	INode targetSchema = new SequenceNode(this.targetStr);

	DataSource source = new DataSource(type, this.sourceSchema);
	DataSource target = new DataSource(type, this.targetSchema);

	JoinCondition sourceJoinCondition;
	JoinCondition targetJoinCondition;

	List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();
	ValueCorrespondence corr;

	KeyConstraint targetKey;

	@Property
	@Name(preposition = "where")
	public void setJoinConditions(final BooleanExpression assignment) {

		final List<String> inputs = new ArrayList<String>();
		final HashMap<String, String> joinParts = new HashMap<String, String>();

		for (final ChildIterator it = assignment.iterator(); it.hasNext();) {
			final ArrayAccess expr = (ArrayAccess) it.next();
			final ObjectAccess inputExpr = (ObjectAccess) expr.getInputExpression();
			final String input = inputExpr.getInputExpression().toString();
			final String attr = inputExpr.getField();
			inputs.add(input);
			joinParts.put(input, attr);
		}

		final String sourceNesting = this.createNesting(this.sourceStr, inputs.get(0));
		final String targetNesting = this.createNesting(this.sourceStr, inputs.get(1));

		this.sourceJoinCondition = this.createJoinCondition(sourceNesting, joinParts.get(inputs.get(0)), targetNesting, joinParts.get(inputs.get(1)));
	}

	/**
	 * Mapping Task: 
	 * value correspondences: 				mappings and grouping keys 
	 * source and target schema: 			attributes in mappings and grouping keys 
	 * target keys:							grouping keys 
	 * join conditions with foreign keys: 	where-clause and foreign keys
	 * 
	 */
	@Property
	@Name(adjective = "as")
	public void setMappingTask(final ArrayCreation assignment) {
		this.classStr = this.getClass().getSimpleName();

		final int nrOfInputs = this.getNumberOfInputs(assignment);
		this.createDefaultSourceSchema(nrOfInputs);
		this.createDefaultTargetSchema(assignment.size());

		for (int index = 0; index < assignment.size(); index++) { // operator level

			final NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment.get(index);
			final Grouping operator = (Grouping) nestedOperator.getOperator();
			final String[] groupingKey = operator.getGroupingKey(0).toString().split("\\."); 	// e.g. in0.id
			final String targetInputStr = this.inputPrefixStr + String.valueOf(index); 			// e.g. in0
			final String sourceInputStr = groupingKey[0]; 										// e.g. in0
			final String keyStr = groupingKey[1];												// e.g. id
			String sourceNesting = this.createNesting(this.sourceStr, sourceInputStr); 			// e.g. source.entities_in0.entity_in0
			final String targetNesting = this.createNesting(this.targetStr, targetInputStr); 	// e.g. target.entities_in0.entity_in0

			// add source grouping key to source schema
			this.extendSourceSchemaBy(keyStr, sourceInputStr);

			// add target entity id to target schema
			this.extendTargetSchemaBy(this.idStr, targetInputStr);

			// create primary key: target entity id
			this.targetKey = this.createKeyConstraints(targetNesting, this.idStr);
			this.target.addKeyConstraint(this.targetKey);

			// create value correspondence: source grouping key -> target entity id
			this.corr = this.createValueCorrespondence(sourceNesting, keyStr, targetNesting, this.idStr);
			this.valueCorrespondences.add(this.corr);

			final ObjectCreation resultProjection = (ObjectCreation) operator.getResultProjection();
			final List<Mapping<?>> mappings = resultProjection.getMappings();
			for (final Mapping<?> mapping : mappings) { // mapping level

				final EvaluationExpression expr = mapping.getExpression(); 						// TODO: can be expression or ObjectCreation for nested object
				final String sourceInputExpr = expr.toString().split("\\.")[0]; 				// e.g. in0
				final String sourceExpr = expr.toString().split("\\.")[1]; 						// e.g. name_original
				final String targetExpr = mapping.getTarget().toString(); 						// e.g. name_person
				sourceNesting = this.createNesting(this.sourceStr, sourceInputExpr); 			// e.g. source.entities_in0.entity_in0

				if (sourceInputExpr.contains(this.classStr)) { // foreign key

					final String fkSource = sourceInputExpr.replaceAll("[^0-9]", ""); 			// TODO: better way?
					sourceNesting = this.createNesting(this.targetStr, this.inputPrefixStr + fkSource);

					// create join condition for foreign keys, but no value correspondence
					this.targetJoinCondition = this.createJoinCondition(targetNesting, targetExpr, sourceNesting, sourceExpr);

				} else { // no foreign key

					// add source attribute to source schema
					this.extendSourceSchemaBy(sourceExpr, sourceInputExpr);

					// create value correspondence: source attribute -> target attribute
					this.corr = this.createValueCorrespondence(sourceNesting, sourceExpr, targetNesting, targetExpr);
					this.valueCorrespondences.add(this.corr);
				}

				// add target attribute to target schema
				this.extendTargetSchemaBy(targetExpr, targetInputStr);

				// add join attributes to source schema
				if (this.sourceJoinCondition != null) {

					String joinAttr = this.sourceJoinCondition.getFromPaths().get(0).getLastStep();
					String joinSource = this.sourceJoinCondition.getFromPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr)) this.extendSourceSchemaBy(joinAttr, sourceInputExpr);

					joinAttr = this.sourceJoinCondition.getToPaths().get(0).getLastStep();
					joinSource = this.sourceJoinCondition.getToPaths().get(0).getFirstStep();

					if (joinSource.contains(sourceInputExpr)) this.extendSourceSchemaBy(joinAttr, sourceInputExpr);
				}
			}
		}

		// TODO: still hard coded
		this.corr = this.createValueCorrespondence("source.entities_in1.entity_in1", "worksFor", "target.entities_in0.entity_in0", "worksFor");
		this.valueCorrespondences.add(this.corr);
		
		this.mappingTask = new MappingTask(this.source, this.target, this.valueCorrespondences);
		if (this.sourceJoinCondition != null) this.mappingTask.getSourceProxy().addJoinCondition(this.sourceJoinCondition);
		if (this.targetJoinCondition != null) this.mappingTask.getTargetProxy().addJoinCondition(this.targetJoinCondition);
		if (DEBUG) System.out.println("mapping task:\n" + this.mappingTask);
	}

	private int getNumberOfInputs(final ArrayCreation assignment) {

		final HashSet<String> inputs = new HashSet<String>();
		for (int index = 0; index < assignment.size(); index++) { // operator level

			final NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment.get(index);
			final Grouping operator = (Grouping) nestedOperator.getOperator();
			final String[] groupingKey = operator.getGroupingKey(0).toString().split("\\."); 	// e.g. in0.id
			final String sourceInputStr = groupingKey[0]; 										// e.g. in0

			inputs.add(sourceInputStr);
		}
		return inputs.size();
	}

	private void createDefaultSourceSchema(final int size) {

		// source : SequenceNode
		// entities_in0 : SetNode
		// entity_in0 : SequenceNode
		// entities_in1 : SetNode
		// entity_in1 : SequenceNode

		for (int index = 0; index < size; index++) {
			final String input = this.inputPrefixStr + String.valueOf(index);
			this.sourceEntity = new SequenceNode(this.entityStr + input);
			this.sourceEntities = new SetNode(this.entitiesStr + input);
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
			final String input = this.inputPrefixStr + String.valueOf(index);
			this.targetEntity = new SequenceNode(this.entityStr + input);
			this.targetEntities = new SetNode(this.entitiesStr + input);
			this.targetEntities.addChild(this.targetEntity);
			this.targetSchema.addChild(this.targetEntities);
		}
		this.targetSchema.setRoot(true);
	}

	private void extendSourceSchemaBy(final String attr, final String inputStr) {

		this.sourceEntity = this.sourceSchema.getChild(this.entitiesStr + inputStr).getChild(this.entityStr + inputStr);
		if (this.sourceEntity.getChild(attr) == null) {
			this.sourceAttr = new AttributeNode(attr);
			this.sourceAttr.addChild(this.dummy);
			this.sourceEntity.addChild(this.sourceAttr);
		}
	}

	private void extendTargetSchemaBy(final String attr, final String inputStr) {

		this.targetEntity = this.targetSchema.getChild(this.entitiesStr + inputStr).getChild(this.entityStr + inputStr);
		if (this.targetEntity.getChild(attr) == null) {
			this.targetAttr = new AttributeNode(attr);
			this.targetAttr.addChild(this.dummy);
			this.targetEntity.addChild(this.targetAttr);
		}
	}

	private String createNesting(final String type, final String count) {

		final String separator = ".";

		final StringBuilder builder = new StringBuilder().append(type).append(separator).append(this.entitiesStr).append(count)
				.append(separator).append(this.entityStr).append(count);

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
		if (DEBUG) System.out.println(tree);

		final GeneratedSchemaMapping sopremoOperator = new GeneratedSchemaMapping();
		sopremoOperator.setMappingTask(this.mappingTask);

		//TODO: loop
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
}