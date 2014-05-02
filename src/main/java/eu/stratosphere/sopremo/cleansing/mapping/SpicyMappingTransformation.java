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
import it.unibas.spicy.model.datasource.DataSource;
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

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.ArrayUnion;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.TwoSourceJoin;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.EntityMapping;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.operator.*;
import eu.stratosphere.sopremo.pact.SopremoUtil;

/**
 * Reads a Spicy MappingTask and create Sopremo Operator
 * 
 * @author Andrina Mascher, Arvid Heise, Fabian Tschirschnitz, Tommy Neubert
 */
@InputCardinality(2)
// TODO arbitrary in-/output
@OutputCardinality(2)
@Internal
// @DefaultSerializer(value = SpicyMappingTransformation.SpicyMappingTransformationSerializer.class)
public class SpicyMappingTransformation extends
		CompositeOperator<SpicyMappingTransformation> {
	private MappingInformation mappingInformation = new MappingInformation();

	private transient SopremoModule module;

	// the mapping task holds tgds and schema information
	private transient MappingTask mappingTask = null;

	// index of name to tgd
	// define input and output order by the sources'/sink's names
	private Map<String, Integer> inputIndex = new HashMap<String, Integer>();

	private Map<String, Integer> outputIndex = new HashMap<String, Integer>();

	private static final String LEAF = "LEAF";

	private transient HashMap<String, FORule> tgdIndex = null;

	// reuse sources
	private transient HashMap<Integer, Projection> reuseProjections = null;

	// to reuse joins and antijoins by their spicy id
	private transient HashMap<String, TwoSourceJoin> reuseJoins = null;

	private transient SpicyCorrespondenceTransformation correspondenceTransformation = null;

	public Map<String, Integer> getInputIndex() {
		return this.inputIndex;
	}

	public void setInputIndex(Map<String, Integer> inputIndex) {
		this.inputIndex = inputIndex;
	}

	public Map<String, Integer> getOutputIndex() {
		return this.outputIndex;
	}

	public void setOutputIndex(Map<String, Integer> outputIndex) {
		this.outputIndex = outputIndex;
	}

	public MappingTask getMappingTask() {
		return this.mappingTask;
	}

	public void setMappingTask(MappingTask mappingTask) {
		this.mappingTask = mappingTask;
	}

	public MappingInformation getMappingInformation() {
		return this.mappingInformation;
	}

	public void setMappingInformation(MappingInformation mappingInformation) {
		this.mappingInformation = mappingInformation;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.inputIndex.hashCode();
		result = prime * result + this.mappingInformation.hashCode();
		result = prime * result + this.outputIndex.hashCode();
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
		SpicyMappingTransformation other = (SpicyMappingTransformation) obj;
		return this.inputIndex.equals(other.inputIndex) && this.outputIndex.equals(other.outputIndex) &&
			this.mappingInformation.equals(other.mappingInformation);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere
	 * .sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module) {

		if (this.mappingTask == null) {
			createMappingTaskFromMappingInformation();
		}
		if (this.inputIndex == null || this.outputIndex == null) {
			throw new IllegalStateException(
				"MappingGenerator needs inputIndex and outputIndex");
		}

		// init
		this.module = module;
		this.correspondenceTransformation = new SpicyCorrespondenceTransformation();
		this.reuseProjections = new HashMap<Integer, Projection>(this.inputIndex.size());
		this.reuseJoins = new HashMap<String, TwoSourceJoin>(4);
		this.tgdIndex = new HashMap<String, FORule>();
		for (FORule tgd : this.mappingTask.getMappingData().getRewrittenRules()) {
			this.tgdIndex.put(tgd.getId(), tgd);
		}
		IAlgebraOperator tree = this.mappingTask.getMappingData().getAlgebraTree(); // tree
		// contains
		// rewritten
		// tgd
		// rules
		SopremoUtil.LOG.debug("Spicy mapping tree:\n" + tree);
		processTree(tree);
	}

	private void createMappingTaskFromMappingInformation() {
		// create mapping task
		this.mappingTask = new MappingTask(new DataSource(EntityMapping.type,
			this.mappingInformation.getSourceSchema().generateSpicyType()),
			this.mappingInformation.getTarget().generateSpicyType(),
			this.mappingInformation.getValueCorrespondencesAsSpicyTypes());
		
		for (MappingJoinCondition cond : this.mappingInformation
				.getSourceJoinConditions()) {
				this.mappingTask.getSourceProxy().addJoinCondition(
					cond.generateSpicyType());
			}

		for (MappingJoinCondition cond : this.mappingInformation
			.getTargetJoinConditions()) {
			this.mappingTask.getTargetProxy().addJoinCondition(
				cond.generateSpicyType());
		}

	}

	private Operator<?> processChild(IAlgebraOperator treeElement,
			ArrayCreation arrayCreationForTargets) {
		// pass on objectCreationForTargets until end of "OnTargetValues" is
		// reached
		if (treeElement instanceof it.unibas.spicy.model.algebra.JoinOnTargetValues) {
			return processJoinOnTargetValues(treeElement);
		} else if (treeElement instanceof it.unibas.spicy.model.algebra.DifferenceOnTargetValues) {
			return processDifferenceOnTargetValues(treeElement,
					arrayCreationForTargets);
		} else if (treeElement instanceof SelectOnTargetValues) {
			return processSelectOnTargetValues(treeElement,
					arrayCreationForTargets);
		}

		// projection is included the first time that no "onTargetValues" is
		// used
		Operator<?> child = null;
		if (treeElement instanceof it.unibas.spicy.model.algebra.Join) {
			child = processJoin(treeElement);
		} else if (treeElement instanceof it.unibas.spicy.model.algebra.Difference) {
			child = processDifference(treeElement);
		} else if (treeElement instanceof Unnest) {
			child = processUnnest(treeElement);
		} else {
			throw new IllegalArgumentException(
				"Schema is too complex and cannot be parsed. Spicy tree element cannot be processed "
					+ treeElement);
		}

		if (arrayCreationForTargets != null) {
			Projection tgd = new Projection().withResultProjection(
					arrayCreationForTargets).withInputs(child);
			return tgd; // don't save tgd-projection for reuse
		} else {
			return child;
		}
	}
	
	private void processTree(IAlgebraOperator treeRoot) {
		Compose compose = (Compose) treeRoot;
		Merge merge = (Merge) compose.getChildren().get(0); // always one merge
															// as child

		// build input list for every target
		// each setAlias represents one target instance, e.g. <v2 as
		// legalEntities>
		HashMap<SetAlias, List<Operator<?>>> targetInputMapping = new HashMap<SetAlias, List<Operator<?>>>();
		for (IAlgebraOperator child : merge.getChildren()) {
			Nest nest = (Nest) child;
			List<SetAlias> targetsOfTgd = getTargetsOfTGD(nest); // use with
																	// getId()
			Operator<?> childOperator = processNest(nest);

			for (SetAlias target : targetsOfTgd) {
				if (targetInputMapping.containsKey(target)) {
					targetInputMapping.get(target).add(childOperator);
				} else {
					List<Operator<?>> newList = new ArrayList<Operator<?>>();
					newList.add(childOperator);
					targetInputMapping.put(target, newList);
				}
			}
		}

		// build operator for every target using the input
		HashMap<SetAlias, Operator<?>> finalOperatorsIndex = new HashMap<SetAlias, Operator<?>>();
		int outputIndex = 0;
		for (Entry<SetAlias, List<Operator<?>>> targetInput : targetInputMapping
			.entrySet()) {
			int target = targetInput.getKey().getId();
			
			UnionAll unionAll = new UnionAll().withInputs(targetInput
				.getValue());
			
			Selection selectAndTransform = new Selection()
				.withInputs(unionAll)
				.withCondition(
					new UnaryExpression(new ArrayAccess(target)))
				.withResultProjection(new ArrayAccess(target));
			
			// duplicate removal
			ObjectCreation finalSchema = createTargetSchemaFromOperatorList(targetInput.getValue(), target, outputIndex);

			Grouping grouping = new Grouping().withInputs(selectAndTransform).
					withGroupingKey(new ObjectAccess(EntityMapping.idStr)).
					withResultProjection(finalSchema);
			
			finalOperatorsIndex.put(targetInput.getKey(), grouping);
			outputIndex++;
		}

		for (Entry<SetAlias, Operator<?>> entry : finalOperatorsIndex
			.entrySet()) {
			String targetName = entry.getKey()
				.getAbsoluteBindingPathExpression().getPathSteps().get(1);

			int i = this.outputIndex.get(targetName);
			SopremoUtil.LOG.debug("output " + this.outputIndex.get(targetName)
				+ " is " + targetName);
			this.module.getOutput(i).setInput(0, entry.getValue());
		}
		SopremoUtil.LOG.info("generated schema mapping module:\n " + this.module);
	}

	private  ObjectCreation createTargetSchemaFromOperatorList(List<Operator<?>> operatorList, int targetIndex, int outputIndex) {
		ObjectCreation finalSchema = new ObjectCreation();
		for(Operator<?> op : operatorList){
			ElementaryOperator<?> eop = (ElementaryOperator<?>)op;
			ArrayCreation aa = eop.getResultProjection().findFirst(ArrayCreation.class);
			if (aa != null) {
				ObjectCreation oc = (ObjectCreation) aa.get(targetIndex);
				if (oc != null) {
					for (Mapping<?> mapping : oc.getMappings()) {
						String fieldName = mapping.getTargetExpression().findFirst(ObjectAccess.class).getField();
						SpicyPathExpression lookupPath = new SpicyPathExpression(EntityMapping.targetStr + EntityMapping.separator + EntityMapping.entitiesStr
								+ outputIndex + EntityMapping.separator + EntityMapping.entityStr + outputIndex , fieldName);
						
						boolean takeAll = false;
						for(MappingValueCorrespondence mvc : this.mappingInformation.getValueCorrespondences()){
							if(mvc.getTargetPath().equals(lookupPath) && mvc.isTakeAllValuesOfGrouping()){
								takeAll = true;
								break;
							}
						}
						if(takeAll){
							finalSchema.addMapping(fieldName,
									FunctionUtil.createFunctionCall(CoreFunctions.ALL, new ObjectAccess(fieldName).withInputExpression(new InputSelection(0))));
						}else{
							finalSchema.addMapping(fieldName,
									FunctionUtil.createFunctionCall(CoreFunctions.FIRST, new ObjectAccess(fieldName).withInputExpression(new InputSelection(0))));
						}
					}
				}
			}
		}
		return finalSchema;
	}

	private List<SetAlias> getTargetsOfTGD(Nest nest) {
		return this.tgdIndex.get(nest.getId()).getTargetView().getVariables();
	}

	private Operator<?> processNest(Nest tgd) {
		/*
		 * rename attributes according to this TGD we need to add mappings to
		 * objectCreation like this: // addMapping("v3", new ObjectCreation().
		 * // addMapping("id", createPath("v1", "id_old")). //
		 * addMapping("name", createPath("v1", "name_old")) // ). //
		 * addMapping("v2", new ObjectCreation(). // addMapping("id",
		 * skolemWorksFor() ). // addMapping("name", createPath("v0",
		 * "worksFor_old")) // );
		 */

		ArrayCreation arrayCreationForTargets = new ArrayCreation();
		TGDGeneratorsMap generators = tgd.getGenerators();
		for (SetAlias setAlias : getTargetsOfTGD(tgd)) { // generate
															// source-to-target
															// mapping for v2 or
															// v3 and add to
															// projection
			Map<PathExpression, IValueGenerator> generatorVi = generators
				.getGeneratorsForVariable(setAlias);

			TreeMap<PathExpression, IValueGenerator> st_map = new TreeMap<PathExpression, IValueGenerator>();
			for (Entry<PathExpression, IValueGenerator> tgdAttribute : generatorVi
				.entrySet()) { // all
								// mappings
								// to
								// this
								// target
				PathExpression spicyTargetPath = tgdAttribute.getKey();
				if (!spicyTargetPath.getLastStep().equals(LEAF)) // consider
																	// only
																	// leafs
					continue;
				st_map.put(spicyTargetPath, tgdAttribute.getValue()); // ("fullname.firstname",
																		// createPath("v0",
																		// "old_name")
																		// +
																		// function)
			}
			ObjectCreation objectVi = this.correspondenceTransformation
				.createNestedObjectFromSpicyPaths(st_map, setAlias);
			//TODO bäääh
			List<EvaluationExpression> currentElements = arrayCreationForTargets.getElements();
			while(currentElements.size()<=setAlias.getId()){
				currentElements.add(ConstantExpression.MISSING);
			}
			currentElements.set(setAlias.getId(), objectVi);
			arrayCreationForTargets= new ArrayCreation(currentElements); // e.g.
																	// v3,
																	// {target-attributes}
		}

		Project project = (Project) tgd.getChildren().get(0); // we can ignore
																// project here,
																// because
																// created
																// transformation
																// includes a
																// projection,
																// too
		Operator<?> child = processChild(project.getChildren().get(0),
				arrayCreationForTargets); // tgd
										// needs
										// to
										// be
										// included
										// further
										// in
										// the
										// tree
										// and
										// passed
										// on
		return child;
	}

	private Operator<?> processSelectOnTargetValues(
			IAlgebraOperator treeElement,
			ArrayCreation arrayCreationForTargets) {
		Operator<?> child = processChild(treeElement.getChildren().get(0),
			arrayCreationForTargets); // nothin
										// todo
		return child;
	}

	private Operator<?> processDifference(IAlgebraOperator treeElement) {
		it.unibas.spicy.model.algebra.Difference difference = (it.unibas.spicy.model.algebra.Difference) treeElement;

		if (this.reuseProjections.containsKey(difference.getId()))
			return this.reuseProjections.get(difference.getId());

		Operator<?> child0 = processChild(difference.getChildren().get(0), null);
		Operator<?> child1 = processChild(difference.getChildren().get(1), null);

		// antijoin condition
		ArrayCreation arrayLeft = new ArrayCreation();
		for (VariablePathExpression path : difference.getLeftPaths()) {
			arrayLeft.add(EntityMappingUtil.convertSpicyPath("0", path));
		}
		ArrayCreation arrayRight = new ArrayCreation();
		for (VariablePathExpression path : difference.getRightPaths()) {
			arrayRight.add(EntityMappingUtil.convertSpicyPath("1", path));
		}

		TwoSourceJoin antiJoin = new TwoSourceJoin().withInputs(child0, child1).withCondition(
			new ElementInSetExpression(arrayLeft, Quantor.EXISTS_NOT_IN, arrayRight));
		antiJoin.setResultProjection(new AggregationExpression(new ArrayUnion()));

		this.reuseJoins.put(difference.getId(), antiJoin);
		return antiJoin;
	}

	private Operator<?> processDifferenceOnTargetValues(
			IAlgebraOperator treeElement,
			ArrayCreation arrayCreationForTargets) {
		it.unibas.spicy.model.algebra.DifferenceOnTargetValues difference =
			(it.unibas.spicy.model.algebra.DifferenceOnTargetValues) treeElement;

		if (this.reuseProjections.containsKey(difference.getId()))
			return this.reuseProjections.get(difference.getId());

		Operator<?> child0 = processChild(difference.getChildren().get(0),
			arrayCreationForTargets);
		Operator<?> child1 = processChild(difference.getChildren().get(1),
			arrayCreationForTargets);

		// antijoin condition
		ArrayCreation arrayLeft = new ArrayCreation();
		for (VariableCorrespondence varCor : difference
			.getLeftCorrespondences()) {
			arrayLeft.add(EntityMappingUtil.convertSpicyPath("0",
				varCor.getTargetPath()));
		}
		ArrayCreation arrayRight = new ArrayCreation();
		for (VariableCorrespondence varCor : difference
			.getRightCorrespondences()) {
			arrayRight.add(EntityMappingUtil.convertSpicyPath("1",
				varCor.getTargetPath()));
		}
		TwoSourceJoin antiJoin = new TwoSourceJoin().withInputs(child0, child1)
			.withCondition(
				new ElementInSetExpression(arrayLeft,
					Quantor.EXISTS_NOT_IN, arrayRight));
		antiJoin.setResultProjection(new AggregationExpression(new ArrayUnion()));

		this.reuseJoins.put(difference.getId(), antiJoin);
		return antiJoin;
	}

	private Operator<?> processJoin(IAlgebraOperator treeElement) {
		it.unibas.spicy.model.algebra.Join spicyJoin = (it.unibas.spicy.model.algebra.Join) treeElement;

		if (this.reuseProjections.containsKey(spicyJoin.getId()))
			return this.reuseProjections.get(spicyJoin.getId());

		Operator<?> child0 = processChild(spicyJoin.getChildren().get(0), null);
		Operator<?> child1 = processChild(spicyJoin.getChildren().get(1), null);

		// join conditions
		// TODO multiple join paths result in concurrency problem. join may not
		// be executed
		// ArrayCreation arrayLeft = new ArrayCreation();
		// for(VariablePathExpression path :
		// spicyJoin.getJoinCondition().getFromPaths()) {
		// arrayLeft.add( EntityMappingUtil.convertSpicyPath("0", path) );
		// }
		// ArrayCreation arrayRight = new ArrayCreation();
		// for(VariablePathExpression path :
		// spicyJoin.getJoinCondition().getToPaths()) {
		// arrayRight.add( EntityMappingUtil.convertSpicyPath("1", path) );
		// }
		//
		// TwoSourceJoin sopremoJoin = new TwoSourceJoin().
		// withInputs(child0, child1).
		// withCondition(new ComparativeExpression( arrayLeft,
		// BinaryOperator.EQUAL, arrayRight)
		// );
		//
		TwoSourceJoin sopremoJoin =
			new TwoSourceJoin().withInputs(child0, child1).
				withCondition(
					new ComparativeExpression(EntityMappingUtil.convertSpicyPath("0",
						spicyJoin.getJoinCondition().getFromPaths().get(0)), BinaryOperator.EQUAL,
						EntityMappingUtil.convertSpicyPath("1", spicyJoin.getJoinCondition().getToPaths().get(0))));
		sopremoJoin.setResultProjection(new AggregationExpression(new ArrayUnion()));

		this.reuseJoins.put(spicyJoin.getId(), sopremoJoin);
		return sopremoJoin;
	}

	private Operator<?> processJoinOnTargetValues(IAlgebraOperator treeElement) {
		it.unibas.spicy.model.algebra.JoinOnTargetValues spicyJoin =
			(it.unibas.spicy.model.algebra.JoinOnTargetValues) treeElement;

		if (this.reuseProjections.containsKey(spicyJoin.getId()))
			return this.reuseProjections.get(spicyJoin.getId());

		// rewrite projections left and right with correspondences
		ArrayCreation arrayCreationForTargetsLeft = this.correspondenceTransformation
			.createArrayFromSpicyPaths(spicyJoin
				.getLeftCorrespondences());
		ArrayCreation arrayCreationForTargetsRight = this.correspondenceTransformation
			.createArrayFromSpicyPaths(spicyJoin
				.getRightCorrespondences());

		Operator<?> child0 = processChild(spicyJoin.getChildren().get(0),
				arrayCreationForTargetsLeft);
		Operator<?> child1 = processChild(spicyJoin.getChildren().get(1),
				arrayCreationForTargetsRight);

		// join conditions
		// TODO
		// ArrayCreation arrayLeft = new ArrayCreation();
		// for(VariablePathExpression path :
		// spicyJoin.getJoinCondition().getFromPaths()) {
		// arrayLeft.add( EntityMappingUtil.convertSpicyPath("0", path) );
		// }
		// ArrayCreation arrayRight = new ArrayCreation();
		// for(VariablePathExpression path :
		// spicyJoin.getJoinCondition().getToPaths()) {
		// arrayRight.add( EntityMappingUtil.convertSpicyPath("1", path) );
		// }
		//
		// TwoSourceJoin sopremoJoin = new TwoSourceJoin().
		// withInputs(child0, child1).
		// withCondition(new ComparativeExpression( arrayLeft,
		// BinaryOperator.EQUAL, arrayRight)
		// );

		TwoSourceJoin sopremoJoin = new TwoSourceJoin().withInputs(child0,
			child1).withCondition(
			new ComparativeExpression(EntityMappingUtil
				.convertSpicyPath("0", spicyJoin.getJoinCondition()
					.getFromPaths().get(0)), BinaryOperator.EQUAL,
				EntityMappingUtil.convertSpicyPath("1", spicyJoin
					.getJoinCondition().getToPaths().get(0))));

		this.reuseJoins.put(spicyJoin.getId(), sopremoJoin);
		return sopremoJoin;
	}

	private Operator<?> processUnnest(IAlgebraOperator treeElement) {
		Unnest unnest = (Unnest) treeElement; // e.g.
												// "unnest v0 in usCongress.usCongressMembers"
		SetAlias sourceAlias = unnest.getVariable();
		int sourceId = sourceAlias.getId(); // v0
		String sourceName = sourceAlias.getBindingPathExpression()
			.getLastStep(); // usCongressMembers

		if (this.reuseProjections.containsKey(sourceId))
			return this.reuseProjections.get(sourceId);

		// TODO unnest can have selectionCondition and provenanceCondition,
		// research: what is this

		ArrayCreation positionEncodedArray = new ArrayCreation();
		for(int i = 0; i<sourceId; i++){
			positionEncodedArray.add(ConstantExpression.MISSING);
		}
		positionEncodedArray.add(new ObjectCreation(new ObjectCreation.CopyFields(createPath("0"))));
		
		Projection projection = new Projection().withResultProjection(positionEncodedArray)
			.withInputs(this.module.getInput(this.inputIndex.get(sourceName)));

		SopremoUtil.LOG.debug("Source Projection from: " + unnest.toString()
			+ " reads input named " + sourceName + " at input index "
			+ this.inputIndex.get(sourceName));

		this.reuseProjections.put(sourceId, projection);
		return projection;
	}
}
