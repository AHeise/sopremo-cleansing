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
import it.unibas.spicy.model.mapping.FORule;
import it.unibas.spicy.model.mapping.MappingData;
import it.unibas.spicy.model.mapping.MappingTask;
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

@Name(noun = "map entities from")
@InputCardinality(min = 1, max = Integer.MAX_VALUE)
@OutputCardinality(min = 1, max = Integer.MAX_VALUE)
public class SchemaMapping extends CompositeOperator<SchemaMapping> {
	private MappingTask mappingTask;
	
	private String targetStr = "target";
	private String sourceStr = "source";
	private String entitiesStr = "entities_";
	private String entityStr = "entity_";
	private String idStr = "id";
	private String inputPrefixStr = "in";
	private String classStr = "SchemaMapping";
	
	INode dummy = new LeafNode("dummy");
	INode sourceAttr;
	INode targetAttr;
	INode sourceEntity;
	INode targetEntity;
	INode sourceEntities;
	INode targetEntities;
	INode sourceSchema = new SequenceNode(sourceStr);
	INode targetSchema = new SequenceNode(targetStr);
	DataSource source = new DataSource(null, sourceSchema);
	DataSource target = new DataSource(null, targetSchema);	

	JoinCondition sourceJoinCondition;
	JoinCondition targetJoinCondition;
	
	List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();
	ValueCorrespondence corr;
	
	KeyConstraint targetKey;
	
	@Property
	@Name(preposition = "where")
	public void setJoinConditions(BooleanExpression assignment) {
		
		List<String> inputs = new ArrayList<String>();
		HashMap<String,String> joinParts = new HashMap<String,String>();
		
		for (ChildIterator it = assignment.iterator(); it.hasNext();) {
			ArrayAccess expr = (ArrayAccess) it.next();
			ObjectAccess inputExpr = (ObjectAccess) expr.getInputExpression();
			String input = inputExpr.getInputExpression().toString();
			String attr = inputExpr.getField();
			inputs.add(input);
			joinParts.put(input, attr);
		}
		
		String sourceNesting = createNesting(sourceStr, inputs.get(0));
		String targetNesting = createNesting(sourceStr, inputs.get(1));
		
		sourceJoinCondition = createJoinCondition(sourceNesting, joinParts.get(inputs.get(0)), targetNesting, joinParts.get(inputs.get(1)));	
	}
	
	/**
	 * Mapping Task:
	 * value correspondences					mappings and grouping keys			
	 * source and target schema					attributes in mappings and grouping keys
	 * target keys								grouping keys
	 * join conditions with foreign keys		where-clause and foreign keys
	 * 
	 */
	@Property
	@Name(adjective = "as")
	public void setMappingTask(ArrayCreation assignment) {	
		
		int nrOfInputs = getNumberOfInputs(assignment);
		createDefaultSourceSchema(nrOfInputs);
		createDefaultTargetSchema(assignment.size());
				
		for (int index=0;index<assignment.size();index++) {	// operator level
			
			NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment.get(index);
			Grouping operator = (Grouping) nestedOperator.getOperator();
			String[] groupingKey = operator.getGroupingKey(0).toString().split("\\.");	// e.g. in0.id
			
			String targetInputStr = inputPrefixStr + String.valueOf(index); 			// e.g. in0									//TODO: you cannot switch sources
			String sourceInputStr = groupingKey[0];										// e.g. in0
			String keyStr = groupingKey[1];												// e.g. id
			String sourceNesting = createNesting(sourceStr, sourceInputStr);			// e.g. source.entities_in0.entity_in0
			String targetNesting = createNesting(targetStr, targetInputStr);			// e.g. target.entities_in0.entity_in0

			// add source grouping key to source schema
			extendSourceSchemaBy(keyStr, sourceInputStr);
			
			// add target entity id to target schema
			extendTargetSchemaBy(idStr, targetInputStr);
			
			// create primary key: target entity id
			targetKey = createKeyConstraints(targetNesting, idStr);
			target.addKeyConstraint(targetKey);
			
			// create value correspondence: source grouping key -> target entity id
			corr = createValueCorrespondence(sourceNesting, keyStr, targetNesting, idStr);	//TODO
			valueCorrespondences.add(corr);
			
			ObjectCreation resultProjection = (ObjectCreation) operator.getResultProjection();
			List<Mapping<?>> mappings = resultProjection.getMappings();
			for (Mapping<?> mapping : mappings) {	// mapping level
				
				EvaluationExpression expr = mapping.getExpression();					// TODO: can be expression or ObjectCreation for nested object
				String sourceInputExpr = expr.toString().split("\\.")[0];				// e.g. in0
				String sourceExpr = expr.toString().split("\\.")[1];					// e.g. name_original
				String targetExpr = mapping.getTarget().toString();						// e.g. name_person
				sourceNesting = createNesting(sourceStr, sourceInputExpr);				// e.g. source.entities_in0.entity_in0
				
				if (sourceInputExpr.contains(classStr)) { // foreign key	
					
					String fkSource = sourceInputExpr.replaceAll("[^0-9]", "");			// TODO: better way?
					sourceNesting = createNesting(targetStr, inputPrefixStr + fkSource);	
					
					// create join condition for foreign keys, but no value correspondence
					targetJoinCondition = createJoinCondition(sourceNesting, sourceExpr, targetNesting, targetExpr);
				}
				else { // no foreign key
					// add source attribute to source schema
					extendSourceSchemaBy(sourceExpr, sourceInputExpr);
					
					// create value correspondence: source attribute -> target attribute 
					corr = createValueCorrespondence(sourceNesting, sourceExpr, targetNesting, targetExpr);
					valueCorrespondences.add(corr);
				}

				// add target attribute to target schema
				extendTargetSchemaBy(targetExpr, targetInputStr);
				
				// add join attributes to source schema
				if (this.sourceJoinCondition != null) {

					String joinAttr = this.sourceJoinCondition.getFromPaths().get(0).getLastStep();
					String joinSource = this.sourceJoinCondition.getFromPaths().get(0).getFirstStep();
					
					if (joinSource.contains(sourceInputExpr)) {
						extendSourceSchemaBy(joinAttr, sourceInputExpr);
					}
					
					joinAttr = this.sourceJoinCondition.getToPaths().get(0).getLastStep();
					joinSource = this.sourceJoinCondition.getToPaths().get(0).getFirstStep();
					
					if (joinSource.contains(sourceInputExpr)) {
						extendSourceSchemaBy(joinAttr, sourceInputExpr);
					}
				}
			}
		}
		
		mappingTask = new MappingTask(source, target, valueCorrespondences);
		mappingTask.getConfig().setRewriteOverlaps(true); //TODO remove?
		mappingTask.getSourceProxy().addJoinCondition(this.sourceJoinCondition);
		mappingTask.getTargetProxy().addJoinCondition(this.targetJoinCondition);
		System.out.println("### mapping task:\n" + this.mappingTask);
		
		
		MappingData data = mappingTask.getMappingData();
		IAlgebraOperator tree = data.getAlgebraTree();
//		IAlgebraOperator tree = mappingTask.getMappingData().getAlgebraTree();
		System.out.println(tree);
		
		
		
		
		GeneratedSchemaMapping sopremoOperator = new GeneratedSchemaMapping();
		sopremoOperator.setMappingTask(mappingTask);
		  
	  	HashMap<String, Integer> inputIndex = new HashMap<String, Integer>(2);
		inputIndex.put("entities_in0", 0);
		inputIndex.put("entities_in1", 1);
		sopremoOperator.setInputIndex(inputIndex);
		  
		HashMap<String, Integer> outputIndex = new HashMap<String, Integer>(2);
		outputIndex.put("entities_in0", 0);
		outputIndex.put("entities_in1", 1);
		sopremoOperator.setOutputIndex(outputIndex);
	}
	
	private int getNumberOfInputs(ArrayCreation assignment) {
		//TODO: should be number of input sources, not number of groups
			
		return 2;		
	}
	
	private void createDefaultSourceSchema(int size) {
		
//		source : SequenceNode
//	    	entities_in0 : SetNode
//	       		entity_in0 : SequenceNode
//	    	entities_in1 : SetNode
//	        	entity_in1 : SequenceNode

		for (int index=0;index<size;index++) {
			String input = inputPrefixStr + String.valueOf(index);
			sourceEntity = new SequenceNode(entityStr + input);
			sourceEntities = new SetNode(entitiesStr + input);
			sourceEntities.addChild(sourceEntity);
			sourceSchema.addChild(sourceEntities);
		}
		sourceSchema.setRoot(true);
	}
	
	private void createDefaultTargetSchema(int size) {

//		target : SequenceNode
//	    	entities_in0 : SetNode
//	        	entity_in0 : SequenceNode
//	    	entities_in1 : SetNode
//	        	entity_in1 : SequenceNode
	        
		for (int index=0;index<size;index++) {
			String input = inputPrefixStr + String.valueOf(index);
			targetEntity = new SequenceNode(entityStr + input);
			targetEntities = new SetNode(entitiesStr + input);
			targetEntities.addChild(targetEntity);
			targetSchema.addChild(targetEntities);
		}
		targetSchema.setRoot(true);
	}
	
	private void extendSourceSchemaBy(String attr, String inputStr) {
		
		sourceEntity = sourceSchema.getChild(entitiesStr + inputStr).getChild(entityStr + inputStr);
		if (sourceEntity.getChild(attr) == null) {
			sourceAttr = new AttributeNode(attr);
			sourceAttr.addChild(dummy);
			sourceEntity.addChild(sourceAttr);
		}
	}
	
	private void extendTargetSchemaBy(String attr, String inputStr) {
		
		targetEntity = targetSchema.getChild(entitiesStr + inputStr).getChild(entityStr + inputStr);
		if (targetEntity.getChild(attr) == null) {
			targetAttr = new AttributeNode(attr);
			targetAttr.addChild(dummy);
			targetEntity.addChild(targetAttr);
		}
	}

	private String createNesting(String type, String count) {
		
		String separator = ".";
		
		StringBuilder builder = new StringBuilder()
			.append(type)
			.append(separator)
			.append(entitiesStr)
			.append(count)
			.append(separator)
			.append(entityStr)
			.append(count);
					
		return builder.toString();
	}
	
	private ValueCorrespondence createValueCorrespondence(String sourceNesting, String sourceAttr, String targetNesting, String targetAttr) {
		
		List<String> sourceSteps = new ArrayList<String>();
		List<String> targetSteps = new ArrayList<String>();

		sourceSteps.add(sourceNesting);
		sourceSteps.add(sourceAttr);
		targetSteps.add(targetNesting);
		targetSteps.add(targetAttr);
		
		PathExpression sourcePath = new PathExpression(sourceSteps);
		PathExpression targetPath = new PathExpression(targetSteps);
		
		return new ValueCorrespondence(sourcePath, targetPath);
	}
	
	private KeyConstraint createKeyConstraints(String nesting, String attr) {
		
		List<String> list = new ArrayList<String>();
		list.add(nesting);
		list.add(attr);
		
		PathExpression key = new PathExpression(list);
		
		List<PathExpression> keyPath = new ArrayList<PathExpression>();
		keyPath.add(key);
		
		return new KeyConstraint(keyPath, true);
	}
	
	private JoinCondition createJoinCondition(String sourceNesting, String sourceAttr, String targetNesting, String targetAttr) {
				
		List<PathExpression> fromPaths = createPaths(sourceNesting, sourceAttr);
		List<PathExpression> toPaths = createPaths(targetNesting, targetAttr);
		
		JoinCondition joinCondition = new JoinCondition(fromPaths, toPaths, true);
		joinCondition.setMandatory(true);
		joinCondition.setMonodirectional(true);
		
		return joinCondition;
	}
	
	private List<PathExpression> createPaths(String nesting, String attr) {
		
		List<String> list = new ArrayList<String>();
		list.add(nesting);
		list.add(attr);
		
		PathExpression path = new PathExpression(list);
		
		List<PathExpression> pathList = new ArrayList<PathExpression>();
		pathList.add(path);
		
		return pathList;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(eu.stratosphere.sopremo.operator.
	 * SopremoModule, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
	}
}