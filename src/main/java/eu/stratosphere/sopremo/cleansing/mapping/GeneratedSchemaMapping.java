/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.createPath;
import it.unibas.spicy.model.algebra.Compose;
import it.unibas.spicy.model.algebra.IAlgebraOperator;
import it.unibas.spicy.model.algebra.Merge;
import it.unibas.spicy.model.algebra.Nest;
import it.unibas.spicy.model.algebra.Project;
import it.unibas.spicy.model.algebra.SelectOnTargetValues;
import it.unibas.spicy.model.algebra.Unnest;
import it.unibas.spicy.model.generators.IValueGenerator;
import it.unibas.spicy.model.generators.TGDGeneratorsMap;
import it.unibas.spicy.model.mapping.FORule;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.model.paths.SetAlias;
import it.unibas.spicy.model.paths.VariableCorrespondence;
import it.unibas.spicy.model.paths.VariablePathExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.TwoSourceJoin;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * @author Andrina Mascher, Arvid Heise
 */
@InputCardinality(2)
@OutputCardinality(2)
public class GeneratedSchemaMapping extends CompositeOperator<GeneratedSchemaMapping> {

	private transient SopremoModule module;
	//the mapping task holds tgds and schema information
	private transient MappingTask mappingTask = null;
	//index of name to tgd
	//define input and output order by the sources'/sink's names	
	private HashMap<String, Integer> inputIndex = null; 
	private HashMap<String, Integer> outputIndex = null; 

	private static final String LEAF = "LEAF";
	private transient HashMap<String, FORule> tgdIndex = null;
	//reuse sources
	private HashMap<String, Projection> reuseProjections = null;
	//to reuse joins and antijoins by their spicy id
	private HashMap<String, TwoSourceJoin> reuseJoins = null; 
	NestedProjectionFromTGD nestedProjection = null;
	
	public HashMap<String, Integer> getInputIndex() {
		return inputIndex;
	}

	public void setInputIndex(HashMap<String, Integer> inputIndex) {
		this.inputIndex = inputIndex;
	}
	
	public HashMap<String, Integer> getOutputIndex() {
		return outputIndex;
	}

	public void setOutputIndex(HashMap<String, Integer> outputIndex) {
		this.outputIndex = outputIndex;
	}

	public MappingTask getMappingTask() {
		return mappingTask;
	}

	public void setMappingTask(MappingTask mappingTask) {
		this.mappingTask = mappingTask;
	}
	
	/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public void addImplementation(SopremoModule module, EvaluationContext context) {
			
			if(mappingTask == null) {
				throw new IllegalStateException("MappingGenerator needs mappingTask with TGDs");
			}
			if(inputIndex == null || outputIndex == null) {
				throw new IllegalStateException("MappingGenerator needs inputIndex and outputIndex");
			}
			
			//init
			this.module = module; 
			nestedProjection = new NestedProjectionFromTGD();
			nestedProjection.setContext(context);
			reuseProjections = new HashMap<String, Projection>(inputIndex.size()); 
			reuseJoins = new HashMap<String, TwoSourceJoin>(4); 
			tgdIndex = new HashMap<String, FORule>();
			for(FORule tgd : this.mappingTask.getMappingData().getRewrittenRules()) {
				tgdIndex.put(tgd.getId(), tgd);
			}
			IAlgebraOperator tree = mappingTask.getMappingData().getAlgebraTree(); //tree contains rewritten tgd rules
			processTree(tree); 
			
			
			//build exemplary sopremoPlan
//			Compose compose = (Compose) tree; //pick attributes for every target sink, always root
//			Operator<?> union = processChild(tree.getChildren().get(0));
//			Merge merge = (Merge) compose.getChildren().get(0); //collect all attributes, always 1 child only
//			
//			//nest_v2
//			Operator<?> transform_0 = processChild(merge.getChildren().get(0)); //nest_v2
//			Nest nest_v2 = (Nest) merge.getChildren().get(0);  //rename attributes according to TGD
//			Project project = (Project) nest_v2.getChildren().get(0); //select attributes from source
//			Operator<?> antiJoin = processChild(project.getChildren().get(0));	//TODO play with cardinalities. is it always solved as Difference?
//			it.unibas.spicy.model.algebra.Difference difference = (it.unibas.spicy.model.algebra.Difference) project.getChildren().get(0);
//			Operator<?> join = processChild(difference.getChildren().get(1));
//						
//			//nest_v2v3
//			Operator<?> transform_1 = processChild(merge.getChildren().get(1)); //nest_v2v3
//			Nest nest_v2v3 = (Nest) merge.getChildren().get(1);  //further includes project and same join
//			
//			
//			module.getOutput(0).setInput(0, join);
//			module.getOutput(0).setInput(0, antiJoin);
//			module.getOutput(0).setInput(0, transform_0);
//			module.getOutput(0).setInput(0, transform_1);
//			module.getOutput(0).setInput(0, union);
			
	}

	private Operator<?> processChild(IAlgebraOperator treeElement) {
		if(treeElement instanceof it.unibas.spicy.model.algebra.Difference) {
			return processDifference(treeElement);
		} else if(treeElement instanceof it.unibas.spicy.model.algebra.DifferenceOnTargetValues) {
			return processDifferenceOnTargetV(treeElement);	
		} else if(treeElement instanceof it.unibas.spicy.model.algebra.Join) {
			return processJoin(treeElement);	
		} else if(treeElement instanceof it.unibas.spicy.model.algebra.JoinOnTargetValues) {
			return processJoinOnTargetV(treeElement);
		} else if(treeElement instanceof Unnest) {
			return processUnnest(treeElement);
		} else if(treeElement instanceof SelectOnTargetValues) {
			return processSelectOnTargetV(treeElement);
		} else {
			throw new IllegalArgumentException("Schema is too complex and cannot be parsed. Spicy tree element cannot be processed " + treeElement);
		}
	}

	private Operator<?> processSelectOnTargetV(IAlgebraOperator treeElement) {
		SelectOnTargetValues spicySelect = (SelectOnTargetValues) treeElement;
		Operator<?> child = processChild(treeElement.getChildren().get(0));
		
//		System.out.println(spicySelect);
		return child;
	}

	private void processTree(IAlgebraOperator treeRoot) {
		Compose compose = (Compose) treeRoot;
		Merge merge = (Merge) compose.getChildren().get(0); //always one merge as child
		
		// build input list for every target
		// each setAlias represents one target instance, e.g. <v2 as legalEntities>
		HashMap<SetAlias, List<Operator<?>>> targetInputMapping = new HashMap<SetAlias, List<Operator<?>>>(); 
		for(IAlgebraOperator child : merge.getChildren()) {
			Nest nest = (Nest) child;
			List<SetAlias> targetsOfTgd = getTargetsOfTGD(nest); //use with getId()
			Operator<?> childOperator = processNest(nest);
			
			for(SetAlias target : targetsOfTgd) {
				if(targetInputMapping.containsKey(target)) {
					targetInputMapping.get(target).add(childOperator);
				} else {
					List<Operator<?>> newList = new ArrayList<Operator<?>>();
					newList.add(childOperator);
					targetInputMapping.put(target, newList);
				}
			}
		}
		
		//build operator for every target using the input
		HashMap<SetAlias, Operator<?>> finalOperatorsIndex = new HashMap<SetAlias, Operator<?>>();
		for(Entry<SetAlias, List<Operator<?>>> targetInput : targetInputMapping.entrySet()) {
			//		Entry<SetAlias, List<Operator<?>>> targetInput = (Entry<SetAlias, List<Operator<?>>>) targetInputMapping.entrySet().iterator().next();
			String targetName = SchemaMappingUtil.getSourceId(targetInput.getKey()); 

			UnionAll unionAll = new UnionAll().withInputs(targetInput.getValue());

			Selection selectAndTransform = new Selection().
					withInputs( unionAll).
					withCondition(new UnaryExpression(new ObjectAccess(targetName))).
					withResultProjection(new ObjectCreation(new ObjectCreation.CopyFields(createPath(targetName))));

			Union union = new Union().withInputs(selectAndTransform); //duplicate removal
			finalOperatorsIndex.put(targetInput.getKey(), union);
		}
		
		for(Entry<SetAlias, Operator<?>> entry : finalOperatorsIndex.entrySet()) {
			String targetName = entry.getKey().getAbsoluteBindingPathExpression().getPathSteps().get(1);
			
			int i = outputIndex.get(targetName);
			System.out.println("output " + outputIndex.get(targetName) + " is " + targetName);
			module.getOutput(i).setInput(0, entry.getValue());
		}
		System.out.println(module);
	}
		
	private List<SetAlias> getTargetsOfTGD(Nest nest) {
		return tgdIndex.get(nest.getId()).getTargetView().getVariables();
	}

	private Operator<?> processNest(Nest nest) {
		Project project = (Project) nest.getChildren().get(0); //TODO can we ignore project here? because created transformation includes a projection, too
		Operator<?> child = processChild( project.getChildren().get(0) );
		
		ObjectCreation objectCreationForTargets = new ObjectCreation();
		//rename attributes according to TGD
		//we need to add mappings to objectCreation like this:
//		addMapping("v3", new ObjectCreation(). 
//			aeddMapping("id", createPath("v1", "id_old")). 
//			addMapping("name", createPath("v1", "name_old"))
//				).
//		addMapping("v2", new ObjectCreation().
//			addMapping("id", skolemWorksFor() ). 
//			addMapping("name", createPath("v0", "worksFor_old"))
//		);	

		TGDGeneratorsMap generators = nest.getGenerators();
		for(SetAlias setAlias : getTargetsOfTGD(nest)) { //generate source-to-target mapping for v2 or v3 and add to projection
			Map<PathExpression, IValueGenerator> generatorVi = generators.getGeneratorsForVariable(setAlias); 
			
			TreeMap<PathExpression, IValueGenerator> st_map = new TreeMap<PathExpression, IValueGenerator>();
			for(Entry<PathExpression, IValueGenerator> tgdAttribute :  generatorVi.entrySet()) { //all mappings to this target				
				PathExpression spicyTargetPath = tgdAttribute.getKey();
				if( !spicyTargetPath.getLastStep().equals(LEAF) ) //consider only leafs
					continue;
				st_map.put(spicyTargetPath, tgdAttribute.getValue()); //("fullname.firstname", createPath("v0", "old_name")) 
			}		
			ObjectCreation objectVi = nestedProjection.createNestedObjectFromSpicyPaths(st_map, setAlias);
			objectCreationForTargets.addMapping( SchemaMappingUtil.getSourceId(setAlias), objectVi); //e.g. v3, {target-attributes}
		}
				
		Projection tgd = new Projection().
				withResultProjection(objectCreationForTargets).				
				withInputs(child);
		
		return tgd;
	}	

	private Operator<?> processDifference(IAlgebraOperator treeElement) {
		it.unibas.spicy.model.algebra.Difference difference = (it.unibas.spicy.model.algebra.Difference) treeElement;
		
		if(reuseProjections.containsKey(difference.getId()))
			return reuseProjections.get(difference.getId());	
		
		Operator<?> child0 = processChild( difference.getChildren().get(0) );
		Operator<?> child1 = processChild( difference.getChildren().get(1) );
		
		TwoSourceJoin antiJoin = new TwoSourceJoin().
				withInputs(child0, child1).
				withCondition( new ElementInSetExpression(
						SchemaMappingUtil.convertSpicyPath("0", difference.getLeftPaths().get(0)) , //TODO can we have multiple conditions?
						Quantor.EXISTS_NOT_IN, 
						SchemaMappingUtil.convertSpicyPath("1", difference.getRightPaths().get(0)) ));
		
		reuseJoins.put(difference.getId(), antiJoin);
		return antiJoin;
	}
	
	private Operator<?> processDifferenceOnTargetV(IAlgebraOperator treeElement) {
		it.unibas.spicy.model.algebra.DifferenceOnTargetValues difference = (it.unibas.spicy.model.algebra.DifferenceOnTargetValues) treeElement;
		
		if(reuseProjections.containsKey(difference.getId()))
			return reuseProjections.get(difference.getId());	
		
		
		Operator<?> child0 = processChild( difference.getChildren().get(0) );
		Operator<?> child1 = processChild( difference.getChildren().get(1) );

		ArrayCreation arrayLeft = new ArrayCreation();
		for(VariableCorrespondence varCor : difference.getLeftCorrespondences()) {
			for(VariablePathExpression path : varCor.getSourcePaths()) {
				arrayLeft.add( SchemaMappingUtil.convertSpicyPath("0", path) ); 
			}
		}
		ArrayCreation arrayRight = new ArrayCreation();
		for(VariableCorrespondence varCor : difference.getRightCorrespondences()) {
			for(VariablePathExpression path : varCor.getSourcePaths()) {
				arrayRight.add( SchemaMappingUtil.convertSpicyPath("1", path) ); 
			}			
		}
		
		TwoSourceJoin antiJoin = new TwoSourceJoin().
				withInputs(child0, child1).	
				withCondition( new ElementInSetExpression( arrayLeft, Quantor.EXISTS_NOT_IN, arrayRight ));
		
		reuseJoins.put(difference.getId(), antiJoin);
		return antiJoin;
	}
	
	private Operator<?> processJoin(IAlgebraOperator treeElement) {
		it.unibas.spicy.model.algebra.Join spicyJoin = (it.unibas.spicy.model.algebra.Join) treeElement;
		
		if(reuseProjections.containsKey(spicyJoin.getId()))
			return reuseProjections.get(spicyJoin.getId());	
		
		Operator<?> child0 = processChild( spicyJoin.getChildren().get(0) );
		Operator<?> child1 = processChild( spicyJoin.getChildren().get(1) );
		
		TwoSourceJoin sopremoJoin = new TwoSourceJoin().
				withInputs(child0, child1). 
				withCondition(new ComparativeExpression(
						SchemaMappingUtil.convertSpicyPath("0", spicyJoin.getJoinCondition().getFromPaths().get(0)), //TODO multiple join paths?
						BinaryOperator.EQUAL, 
						SchemaMappingUtil.convertSpicyPath("1", spicyJoin.getJoinCondition().getToPaths().get(0))  
				));
		
		reuseJoins.put(spicyJoin.getId(), sopremoJoin);
		return sopremoJoin;
	}

	private Operator<?> processJoinOnTargetV(IAlgebraOperator treeElement) {
		it.unibas.spicy.model.algebra.JoinOnTargetValues spicyJoin = (it.unibas.spicy.model.algebra.JoinOnTargetValues) treeElement;
		
		if(reuseProjections.containsKey(spicyJoin.getId()))
			return reuseProjections.get(spicyJoin.getId());	
		
		Operator<?> child0 = processChild( spicyJoin.getChildren().get(0) );
		Operator<?> child1 = processChild( spicyJoin.getChildren().get(1) );
		
		//just correspondences
//		ArrayCreation arrayLeft = new ArrayCreation();
//		for(VariableCorrespondence varCor : spicyJoin.getLeftCorrespondences()) {
//			for(VariablePathExpression path : varCor.getSourcePaths()) {
//				arrayLeft.add( SchemaMappingUtil.convertSpicyPath("0", path) ); 
//			}
//		}
//		ArrayCreation arrayRight = new ArrayCreation();
//		for(VariableCorrespondence varCor : spicyJoin.getRightCorrespondences()) {
//			for(VariablePathExpression path : varCor.getSourcePaths()) {
//				arrayRight.add( SchemaMappingUtil.convertSpicyPath("1", path) ); 
//			}			
//		}
		
		//TODO use join on target
//		TwoSourceJoin sopremoJoin = new TwoSourceJoin().
//				withInputs(child0, child1). 
//				withCondition(new ComparativeExpression( arrayLeft, BinaryOperator.EQUAL, arrayRight)
//				);
//		reuseJoins.put(spicyJoin.getId(), sopremoJoin);
		
		
		return child0;
	}
	
	private Operator<?> processUnnest(IAlgebraOperator treeElement) {
		Unnest unnest = (Unnest) treeElement; //e.g. "unnest v0 in usCongress.usCongressMembers"
		SetAlias sourceAlias = unnest.getVariable(); 
		String sourceId = SchemaMappingUtil.getSourceId(sourceAlias); //v0
		String sourceName = sourceAlias.getBindingPathExpression().getLastStep();  //usCongressMembers
		//TODO can path be nested further? 
		
		if(reuseProjections.containsKey(sourceId))
			return reuseProjections.get(sourceId);	
		
		//TODO unnest can have selectionCondition and provenanceCondition (what's that?)
		
		Projection projection = new Projection().
				withResultProjection(new ObjectCreation().
						addMapping(sourceId, new ObjectCreation(new ObjectCreation.CopyFields(createPath("0")))) //TODO do we need to unnest i.e. flatten all attributes?
						).
				withInputs( module.getInput(inputIndex.get(sourceName)) );
		
		System.out.println("Source Projection from: " + unnest.toString() + " reads input named " + sourceName + " at input index " + inputIndex.get(sourceName));
		
		reuseProjections.put(sourceId, projection);
		return projection;
	}
}
