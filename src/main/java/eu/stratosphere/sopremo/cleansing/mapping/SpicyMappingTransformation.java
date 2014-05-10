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
import it.unibas.spicy.model.algebra.query.operators.xquery.GenerateXQuery;
import it.unibas.spicy.model.algebra.query.operators.xquery.XQBlocks;
import it.unibas.spicy.model.algebra.query.operators.xquery.XQNames;
import it.unibas.spicy.model.algebra.query.operators.xquery.XQUtility;
import it.unibas.spicy.model.datasource.DataSource;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.MetadataNode;
import it.unibas.spicy.model.datasource.nodes.SequenceNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.datasource.nodes.TupleNode;
import it.unibas.spicy.model.generators.IValueGenerator;
import it.unibas.spicy.model.generators.NullValueGenerator;
import it.unibas.spicy.model.generators.TGDGeneratorsMap;
import it.unibas.spicy.model.mapping.ComplexConjunctiveQuery;
import it.unibas.spicy.model.mapping.ComplexQueryWithNegations;
import it.unibas.spicy.model.mapping.FORule;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.mapping.NegatedComplexQuery;
import it.unibas.spicy.model.mapping.SimpleConjunctiveQuery;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.model.paths.SetAlias;
import it.unibas.spicy.model.paths.VariableCorrespondence;
import it.unibas.spicy.model.paths.VariableJoinCondition;
import it.unibas.spicy.model.paths.VariablePathExpression;
import it.unibas.spicy.model.paths.VariableSelectionCondition;
import it.unibas.spicy.model.paths.operators.GeneratePathExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.amazonaws.auth.policy.conditions.BooleanCondition;
import com.google.common.collect.Lists;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.ArrayUnion;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Selection;
import eu.stratosphere.sopremo.base.TwoSourceJoin;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.base.UnionAll;
import eu.stratosphere.sopremo.cleansing.EntityMapping;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyUtil.InputManager;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyUtil.StreamManager;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BatchAggregationExpression;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.UnaryExpression;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Internal;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.JsonUtil;

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

	public void setInputIndex(final Map<String, Integer> inputIndex) {
		this.inputIndex = inputIndex;
	}

	public Map<String, Integer> getOutputIndex() {
		return this.outputIndex;
	}

	public void setOutputIndex(final Map<String, Integer> outputIndex) {
		this.outputIndex = outputIndex;
	}

	public MappingTask getMappingTask() {
		return this.mappingTask;
	}

	public void setMappingTask(final MappingTask mappingTask) {
		this.mappingTask = mappingTask;
	}

	public MappingInformation getMappingInformation() {
		return this.mappingInformation;
	}

	public void setMappingInformation(final MappingInformation mappingInformation) {
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
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final SpicyMappingTransformation other = (SpicyMappingTransformation) obj;
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
	public void addImplementation(final SopremoModule module) {

		if (this.mappingTask == null)
			this.createMappingTaskFromMappingInformation();
		if (this.inputIndex == null || this.outputIndex == null)
			throw new IllegalStateException(
				"MappingGenerator needs inputIndex and outputIndex");

		List<FORule> tgds = mappingTask.getMappingData().getRewrittenRules();

		StreamManager variableToSourceMapper = new StreamManager();
		for (FORule tgd : tgds)
			for (SetAlias setAlias : tgd.getComplexSourceQuery().getVariables()) {
				VariablePathExpression bindingPathExpression = setAlias.getBindingPathExpression();
				String entityName = bindingPathExpression.getAbsolutePath().getPathSteps().get(1);
				int id = Integer.parseInt(entityName);
				variableToSourceMapper.put(setAlias.toShortString(), module.getInput(id));
			}

		List<JsonStream> tdgOutput = new ArrayList<JsonStream>();
		for (FORule tgd : tgds) {
			tdgOutput.add(generate(tgd.getComplexSourceQuery(), mappingTask, variableToSourceMapper));
		}

		materializeRules(mappingTask, tgds, variableToSourceMapper);
		materializeResultOfExchange(mappingTask, tgds, variableToSourceMapper);
		finalNest(mappingTask, variableToSourceMapper);

		List<JsonStream> outputs = new ArrayList<JsonStream>();
		INode schema = mappingTask.getTargetProxy().getIntermediateSchema();
		for (INode child : schema.getChildren()) {
			VariablePathExpression relativePath = new GeneratePathExpression().generateRelativePath(child, mappingTask.getTargetProxy());
			SetAlias setVariable = relativePath.getStartingVariable();
			outputs.add(variableToSourceMapper.getStream(XQNames.finalXQueryNameSTExchange(setVariable)));
		}

		for (int index = 0; index < outputs.size(); index++)
			module.getOutput(index).setInput(0, outputs.get(index));
	}

	private void finalNest(MappingTask mappingTask, StreamManager variableToSourceMapper) {
		INode schema = mappingTask.getTargetProxy().getIntermediateSchema();
		recursivelyFinalGroupAndJoin(schema, mappingTask, variableToSourceMapper);
	}

	private void recursivelyFinalGroupAndJoin(INode node, MappingTask mappingTask, StreamManager variableToSourceMapper) {
		List<INode> children = node.getChildren();
		if (children == null)
			return;
		for (INode child : children) {
			recursivelyFinalGroupAndJoin(child, mappingTask, variableToSourceMapper);
		}
		if (node instanceof SetNode) {
			VariablePathExpression relativePath = new GeneratePathExpression().generateRelativePath(node, mappingTask.getTargetProxy());
			SetAlias setVariable = relativePath.getStartingVariable();
			String setNodeVariableName = XQNames.finalXQueryNameSTExchange(setVariable);
			JsonStream input = variableToSourceMapper.getStream(setNodeVariableName);
			if (input == null) {
				throw new UnsupportedOperationException();
			}
			SetAlias fatherVariable = setVariable.getBindingPathExpression().getStartingVariable();
			if (fatherVariable != null) {
				ObjectCreation agg = new ObjectCreation();
				BatchAggregationExpression bae = new BatchAggregationExpression();
				agg.addMapping("content", bae.add(CoreFunctions.ALL, getFinalProjectionForSet(mappingTask, children.get(0))));
				agg.addMapping(XQUtility.SET_ID, bae.add(CoreFunctions.FIRST, new ObjectAccess(XQUtility.SET_ID)));

				Grouping grouping = new Grouping().
					withInputs(input).
					withGroupingKey(new ObjectAccess(XQUtility.SET_ID)).
					withResultProjection(agg);

				ObjectCreation oc2 = new ObjectCreation();
				oc2.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
				ObjectCreation contentExpr = new ObjectCreation();
				contentExpr.addMapping(new ObjectCreation.CopyFields(JsonUtil.createPath("0", "content")));
				for (INode child : children) {
					contentExpr.addMapping(child.getLabel(), JsonUtil.createPath("1", "content"));
				}
				oc2.addMapping("content", contentExpr);

				String streamName = XQNames.finalXQueryNameSTExchange(fatherVariable);
				Join join = new Join().
					withInputs(variableToSourceMapper.getStream(streamName), grouping).
					withJoinCondition(
						new ComparativeExpression(JsonUtil.createPath("0", setVariable.getBindingPathExpression().getLastStep()),
							BinaryOperator.EQUAL, JsonUtil.createPath("1", XQUtility.SET_ID))).
					withResultProjection(oc2);
				variableToSourceMapper.put(streamName, join);
			} else {
				INode firstChild = children.get(0);
				Projection projection = new Projection().
					withInputs(input).
					withResultProjection(getFinalProjectionForSet(mappingTask, firstChild));
				variableToSourceMapper.put(setNodeVariableName, projection);
			}
		}
	}

	/**
	 * @param mappingTask
	 * @param firstChild
	 * @return
	 */
	private ObjectCreation getFinalProjectionForSet(MappingTask mappingTask, INode firstChild) {
		ObjectCreation oc = new ObjectCreation();
		if (firstChild instanceof TupleNode) {
			for (INode child : firstChild.getChildren()) {
				if (child instanceof SetNode) {
					oc.addMapping(child.getLabel(), JsonUtil.createPath("content", child.getLabel()));
				} else {
					VariablePathExpression childPath =
						new GeneratePathExpression().generateRelativePath(child, mappingTask.getTargetProxy());
					oc.addMapping(child.getLabel(), JsonUtil.createPath("content", SpicyUtil.nameForPath(childPath)));
				}
			}
		} else {
			oc.addMapping(firstChild.getLabel(), JsonUtil.createPath("content"));
		}
		return oc;
	}

	private JsonStream generate(ComplexQueryWithNegations complexSourceQuery, MappingTask mappingTask,
			StreamManager var2Stream) {
		JsonStream cached = var2Stream.getStream(complexSourceQuery.getId());
		if (cached == null)
			var2Stream.put(XQNames.xQueryNameForPositiveView(complexSourceQuery),
				cached = generatePositiveQuery(complexSourceQuery, mappingTask, var2Stream));
		return cached;
	}

	private Operator<?> generatePositiveQuery(ComplexQueryWithNegations query, MappingTask mappingTask,
			StreamManager var2Stream) {
		List<JsonStream> inputs = var2Stream.getStreams(query.getVariables());
		InputManager inputManager = new InputManager(query.getVariables());
		AndExpression whereClauseForView = generateWhereClauseForView(query, inputManager);
		if (whereClauseForView.getExpressions().isEmpty()) {
			Projection projection = new Projection().withInputs(inputs);
			projection.setResultProjection(generateSimpleCopyValuesFromSource(query, mappingTask, inputManager));
			return projection;
		}
		Join join = new Join().withInputs(inputs);
		join.setJoinCondition(whereClauseForView);
		join.setResultProjection(generateSimpleCopyValuesFromSource(query, mappingTask, inputManager));
		return join;
	}

	private EvaluationExpression generateSimpleCopyValuesFromSource(ComplexQueryWithNegations query, MappingTask mappingTask,
			InputManager var2Stream) {
		ObjectCreation oc = new ObjectCreation();
		List<VariablePathExpression> sourceAttributes = extractAttributePaths(query, mappingTask);
		for (int i = 0; i < sourceAttributes.size(); i++) {
			final VariablePathExpression attributePath = sourceAttributes.get(i);
			final EvaluationExpression expression;
			INode attributeNode = attributePath.getLastNode(mappingTask.getSourceProxy().getIntermediateSchema());
			if (attributeNode instanceof MetadataNode) {
				expression = SpicyUtil.createRelativePathForMetadataNode(attributePath, attributeNode, var2Stream);
			} else if (attributeNode.isVirtual()) {
				expression = SpicyUtil.createRelativePathForVirtualAttribute(attributePath, attributeNode, var2Stream);
			} else if (isSingleAttributeWithVirtualFathers((AttributeNode) attributeNode)) {
				expression = SpicyUtil.createRelativePathForSingleAttribute(attributePath, attributeNode, var2Stream);
			} else {
				expression = SpicyUtil.createRelativePath(attributePath, var2Stream);
			}
			oc.addMapping(SpicyUtil.nameForPath(attributePath), expression);
		}
		return oc;
	}

	private boolean isSingleAttributeWithVirtualFathers(AttributeNode attributeNode) {
		INode father = attributeNode.getFather();
		INode ancestor = father.getFather();
		return ((father instanceof TupleNode) && father.getChildren().size() == 1 && father.isVirtual()
			&& (ancestor instanceof SetNode) && ancestor.isVirtual());
	}

	private List<VariablePathExpression> extractAttributePaths(ComplexQueryWithNegations query, MappingTask mappingTask) {
		List<VariablePathExpression> attributePaths = new ArrayList<VariablePathExpression>();
		for (SimpleConjunctiveQuery simpleConjunctiveQuery : query.getComplexQuery().getConjunctions()) {
			attributePaths.addAll(simpleConjunctiveQuery.getAttributePaths(mappingTask.getSourceProxy().getIntermediateSchema()));
		}
		return attributePaths;
	}

	private AndExpression generateWhereClauseForView(ComplexQueryWithNegations query, InputManager var2Stream) {
		List<VariableSelectionCondition> selectionConditions = query.getComplexQuery().getAllSelections();
		List<VariableJoinCondition> joinConditions = new ArrayList<VariableJoinCondition>(query.getComplexQuery().getJoinConditions());
		for (SimpleConjunctiveQuery simpleConjunctiveQuery : query.getComplexQuery().getConjunctions()) {
			joinConditions.addAll(simpleConjunctiveQuery.getAllJoinConditions());
		}
		List<BooleanExpression> expressions = new ArrayList<BooleanExpression>();
		expressions.addAll(generateWhereContentFromJoinConditions(joinConditions, query.getComplexQuery().getAllCorrespondences(),
			var2Stream));
		expressions.addAll(generateWhereContentFromSelectionConditions(selectionConditions));
		expressions.addAll(generateWhereContentFromIntersection(query.getComplexQuery(), var2Stream));
		return new AndExpression(expressions);
	}

	private List<BooleanExpression> generateWhereContentFromIntersection(ComplexConjunctiveQuery view, InputManager var2Stream) {
		List<BooleanExpression> expressions = new ArrayList<BooleanExpression>();
		if (view.hasIntersection()) {
			List<VariablePathExpression> leftIntersectionPaths =
				generateTargetPaths(view.getIntersectionEqualities().getLeftCorrespondences());
			List<VariablePathExpression> rightIntersectionPaths =
				generateTargetPaths(view.getIntersectionEqualities().getRightCorrespondences());
			List<VariableCorrespondence> allCorrespondences = new ArrayList<VariableCorrespondence>();
			allCorrespondences.addAll(view.getIntersectionEqualities().getLeftCorrespondences());
			allCorrespondences.addAll(view.getIntersectionEqualities().getRightCorrespondences());

			for (int i = 0; i < leftIntersectionPaths.size(); i++) {
				VariablePathExpression leftPath = leftIntersectionPaths.get(i);
				VariablePathExpression rightPath = rightIntersectionPaths.get(i);
				VariablePathExpression leftSourcePath = findSourcePathWithEqualsId(allCorrespondences, leftPath);
				if (leftSourcePath == null) {
					leftSourcePath = leftPath;
				}
				VariablePathExpression rightSourcePath = findSourcePathWithEqualsId(allCorrespondences, rightPath);
				if (rightSourcePath == null) {
					rightSourcePath = rightPath;
				}
				expressions.add(new ComparativeExpression(SpicyUtil.createRelativePath(leftSourcePath, var2Stream),
					BinaryOperator.EQUAL, SpicyUtil.createRelativePath(rightSourcePath, var2Stream)));
			}
		}
		return expressions;
	}

	private static List<VariablePathExpression> generateTargetPaths(List<VariableCorrespondence> correspondences) {
		List<VariablePathExpression> result = new ArrayList<VariablePathExpression>();
		for (VariableCorrespondence correspondence : correspondences) {
			result.add(correspondence.getTargetPath());
		}
		return result;
	}

	private static VariablePathExpression findSourcePathWithEqualsId(List<VariableCorrespondence> correspondences,
			VariablePathExpression targetPath) {
		for (VariableCorrespondence variableCorrespondence : correspondences) {
			if (variableCorrespondence.getTargetPath().equalsAndHasSameVariableId(targetPath)) {
				return variableCorrespondence.getFirstSourcePath();
			}
		}
		return null;
	}

	private List<BooleanExpression> generateWhereContentFromJoinConditions(List<VariableJoinCondition> joinConditions,
			List<VariableCorrespondence> correspondences, InputManager var2Stream) {
		List<BooleanExpression> expressions = new ArrayList<BooleanExpression>();
		// if (!joinConditions.isEmpty()) {
		for (int i = 0; i < joinConditions.size(); i++) {
			VariableJoinCondition joinCondition = joinConditions.get(i);
			List<VariablePathExpression> fromPaths = joinCondition.getFromPaths();
			List<VariablePathExpression> toPaths = joinCondition.getToPaths();
			for (int j = 0; j < fromPaths.size(); j++) {

				VariablePathExpression fromPath = fromPaths.get(j);
				VariablePathExpression toPath = toPaths.get(j);
				VariablePathExpression fromSourcePath = XQUtility.findSourcePath(correspondences, fromPath);
				if (fromSourcePath == null) {
					fromSourcePath = fromPath;
				}
				VariablePathExpression toSourcePath = XQUtility.findSourcePath(correspondences, toPath);
				if (toSourcePath == null) {
					toSourcePath = toPath;
				}
				expressions.add(new ComparativeExpression(SpicyUtil.createRelativePath(fromSourcePath, var2Stream),
					BinaryOperator.EQUAL, SpicyUtil.createRelativePath(toSourcePath, var2Stream)));
			}
		}
		// }
		return expressions;
	}

	private List<BooleanExpression> generateWhereContentFromSelectionConditions(
			List<VariableSelectionCondition> selectionConditions) {
		List<BooleanExpression> expressions = new ArrayList<BooleanExpression>();
		for (int i = 0; i < selectionConditions.size(); i++) {
			VariableSelectionCondition selectionCondition = selectionConditions.get(i);
			expressions.add((BooleanExpression) SpicyUtil.fromExpression(selectionCondition.getCondition()));
		}
		return expressions;
	}

	// -- TGD

	private void materializeRules(MappingTask mappingTask, List<FORule> rules, StreamManager streamManager) {
		StringBuilder result = new StringBuilder();
		Collections.sort(rules);
		for (int i = 0; i < rules.size(); i++) {
			FORule tgd = rules.get(i);
			materializeRule(tgd, mappingTask, streamManager);
		}
	}

	private void materializeRule(FORule rule, MappingTask mappingTask, StreamManager streamManager) {

		// // if (isComplexRewriting(mappingTask)) {
		// if (XQUtility.hasDifferences(rule)) {
		// result.append(generateDifferenceForFinalRule(rule));
		// // result.append(",\n");
		// }
		//
		// result.append("\n");
		// // //// TODO: verificare se e' possibile unire questi due metodi
		if (XQUtility.hasDifferences(rule)) {
			throw new UnsupportedOperationException();
			// result.append(generateTargetValueViewWithDifference(rule, mappingTask));
		} else {
			generateTargetValueViewWithoutDifference(rule, mappingTask, streamManager);
		}
	}

	private void generateTargetValueViewWithoutDifference(FORule rule, MappingTask mappingTask, StreamManager streamManager) {
		String fromViewName = "";
		if (rule.getComplexSourceQuery().getComplexQuery().hasIntersection()) {
			fromViewName = XQNames.xQueryNameForViewWithIntersection(rule.getComplexSourceQuery());
		} else {
			fromViewName = XQNames.xQueryNameForPositiveView(rule.getComplexSourceQuery());
		}
		JsonStream input = streamManager.getStream(fromViewName);
		Projection projection = new Projection().
			withInputs(input).
			withResultProjection(projectionOnValues(mappingTask, rule, new InputManager("$variable")));

		String viewName = XQNames.xQueryFinalTgdName(rule);
		streamManager.put(viewName, projection);
	}

	private EvaluationExpression projectionOnValues(MappingTask mappingTask, FORule tgd, InputManager inputManager) {
		ObjectCreation oc = new ObjectCreation();
		List<VariablePathExpression> generatedAttributes = new ArrayList<VariablePathExpression>();

		List<SetAlias> generators = tgd.getTargetView().getGenerators();
		for (int i = 0; i < generators.size(); i++) {
			SetAlias generator = generators.get(i);
			List<VariablePathExpression> attributes = generator.getAttributes(mappingTask.getTargetProxy().getIntermediateSchema());
			for (int j = 0; j < attributes.size(); j++) {
				VariablePathExpression attribute = attributes.get(j);
				if (generatedAttributes.contains(attribute)) {
					continue;
				}
				generatedAttributes.add(attribute);
				IValueGenerator leafGenerator = getLeafGenerator(attribute, tgd, mappingTask, generator);

				String elementName = XQNames.xQueryNameForPath(attribute);
				oc.addMapping(elementName, SpicyUtil.xqueryValueForLeaf(leafGenerator, attribute, tgd, mappingTask, inputManager));
			}
		}
		return oc;
	}

	private IValueGenerator getLeafGenerator(VariablePathExpression attributePath, FORule tgd, MappingTask mappingTask, SetAlias variable) {
		INode attributeNode = attributePath.getLastNode(mappingTask.getTargetProxy().getIntermediateSchema());
		INode leafNode = attributeNode.getChild(0);
		PathExpression leafPath = new GeneratePathExpression().generatePathFromRoot(leafNode);
		Map<PathExpression, IValueGenerator> generatorsForVariable = tgd.getGenerators(mappingTask).getGeneratorsForVariable(variable);
		// //** added to avoid exceptions in XML scenarios
		if (generatorsForVariable == null) {
			return NullValueGenerator.getInstance();
		}
		for (PathExpression pathExpression : generatorsForVariable.keySet()) {
			IValueGenerator generator = generatorsForVariable.get(pathExpression);
			if (pathExpression.equalsUpToClones(leafPath)) {
				return generator;
			}
		}
		throw new IllegalStateException();
	}

	private void materializeResultOfExchange(MappingTask mappingTask, List<FORule> tgds, StreamManager streamManager) {
		List<SetAlias> targetVariables = mappingTask.getTargetProxy().getMappingData().getVariables();
		for (int i = 0; i < targetVariables.size(); i++) {
			SetAlias targetVariable = targetVariables.get(i);
			List<FORule> relevantTGDs = findRelevantTGDs(targetVariable, mappingTask.getMappingData().getRewrittenRules());
			if (!relevantTGDs.isEmpty()) {
				String viewName = XQNames.finalXQueryNameSTExchange(targetVariable);
				streamManager.put(viewName, generateBlockForShredding(targetVariable, mappingTask, relevantTGDs, streamManager));
			}
		}
	}

	private JsonStream generateBlockForShredding(SetAlias targetVariable, MappingTask mappingTask, List<FORule> relevantTgds,
			StreamManager streamManager) {
		List<Projection> projections = new ArrayList<Projection>();
		for (int i = 0; i < relevantTgds.size(); i++) {
			FORule rule = relevantTgds.get(i);
			// String fromViewName = XQUtility.findViewName(tgd, mappingTask);
			String fromViewName = XQNames.xQueryFinalTgdName(rule);
			Projection projection = new Projection().
				withInputs(streamManager.getStream(fromViewName)).
				withResultProjection(generateReturnClauseForSTResult(targetVariable, mappingTask, rule, new InputManager("$variable")));

			projections.add(projection);
		}
		return new Union().withInputs(projections);
	}

	private static EvaluationExpression generateReturnClauseForSTResult(SetAlias targetVariable, MappingTask mappingTask, FORule tgd,
			InputManager inputManager) {
		ObjectCreation oc = new ObjectCreation();
		SetNode setNode = targetVariable.getBindingNode(mappingTask.getTargetProxy().getIntermediateSchema());
		INode tupleNode = setNode.getChild(0);
		oc.addMapping(XQUtility.SET_ID, createIdFromGenerators(setNode, mappingTask, tgd, targetVariable, inputManager));
		List<VariablePathExpression> targetPaths = targetVariable.getAttributes(mappingTask.getTargetProxy().getIntermediateSchema());
		List<INode> setNodeChildren = findSetChildren(tupleNode);
		// generate copy values from tgd view
		ObjectCreation content = new ObjectCreation();
		for (int i = 0; i < targetPaths.size(); i++) {
			VariablePathExpression targetPath = targetPaths.get(i);
			if (targetPath.getStartingVariable().equals(targetVariable)) {
				String targetPathName = XQNames.xQueryNameForPath(targetPath);
				content.addMapping(targetPathName, new ObjectAccess(targetPathName).withInputExpression(new InputSelection(0)));
			}
		}
		oc.addMapping("content", content);
		// add the set id of all children sets
		for (int i = 0; i < setNodeChildren.size(); i++) {
			SetNode targetNode = (SetNode) setNodeChildren.get(i);
			oc.addMapping(targetNode.getLabel(), createIdFromGenerators(targetNode, mappingTask, tgd, targetVariable, inputManager));
		}
		return oc;
	}

	private List<FORule> findRelevantTGDs(SetAlias targetVariable, List<FORule> tgds) {
		Collections.sort(tgds);
		List<FORule> result = new ArrayList<FORule>();
		for (FORule tgd : tgds) {
			if (tgd.getTargetView().getGenerators().contains(targetVariable)) {
				result.add(tgd);
			}
		}
		return result;
	}

	private static EvaluationExpression createIdFromGenerators(INode node, MappingTask mappingTask, FORule tgd, SetAlias variable,
			InputManager inputManager) {
		// List<EvaluationExpression> ids = new ArrayList<EvaluationExpression>();
		// do {
		// PathExpression pathExpression = new GeneratePathExpression().generatePathFromRoot(node);
		// IValueGenerator valueGenerator = getNodeGenerator(pathExpression, mappingTask, tgd, variable);
		// ids.add(0, SpicyUtil.xqueryValueForIntermediateNode(valueGenerator, mappingTask, inputManager));
		// node = node.getFather();
		// } while (node != null);
		//
		// if (ids.size() == 1)
		// return ids.get(0);
		// return FunctionUtil.createFunctionCall(CoreFunctions.CONCAT, new ArrayCreation(ids));
		PathExpression pathExpression = new GeneratePathExpression().generatePathFromRoot(node);
		IValueGenerator valueGenerator = getNodeGenerator(pathExpression, mappingTask, tgd, variable);
		return SpicyUtil.xqueryValueForIntermediateNode(valueGenerator, mappingTask, inputManager);
	}

	private static List<INode> findSetChildren(INode tupleNode) {
		List<INode> result = new ArrayList<INode>();
		return findSetChildrenRecursive(tupleNode, result);
	}

	private static List<INode> findSetChildrenRecursive(INode tupleNode, List<INode> result) {
		for (INode child : tupleNode.getChildren()) {
			if (child instanceof TupleNode) {
				findSetChildrenRecursive(child, result);
			} else if (child instanceof SetNode) {
				result.add(child);
			}
		}
		return result;
	}

	private static IValueGenerator getNodeGenerator(PathExpression nodePath, MappingTask mappingTask, FORule tgd, SetAlias variable) {
		return tgd.getGenerators(mappingTask).getGeneratorsForFatherVariable(variable).get(nodePath);
	}

	private void createMappingTaskFromMappingInformation() {
		// create mapping task
		this.mappingTask = new MappingTask(new DataSource(EntityMapping.type,
			this.mappingInformation.getSourceSchema().generateSpicyType()),
			this.mappingInformation.getTarget().generateSpicyType(),
			this.mappingInformation.getValueCorrespondencesAsSpicyTypes());

		for (final MappingJoinCondition cond : this.mappingInformation
			.getSourceJoinConditions())
			this.mappingTask.getSourceProxy().addJoinCondition(
				cond.generateSpicyType());

		for (final MappingJoinCondition cond : this.mappingInformation
			.getTargetJoinConditions())
			this.mappingTask.getTargetProxy().addJoinCondition(
				cond.generateSpicyType());

	}

	private Operator<?> processChild(final IAlgebraOperator treeElement,
			final ArrayCreation arrayCreationForTargets) {
		// pass on objectCreationForTargets until end of "OnTargetValues" is
		// reached
		if (treeElement instanceof it.unibas.spicy.model.algebra.JoinOnTargetValues)
			return this.processJoinOnTargetValues(treeElement);
		else if (treeElement instanceof it.unibas.spicy.model.algebra.DifferenceOnTargetValues)
			return this.processDifferenceOnTargetValues(treeElement, arrayCreationForTargets);
		else if (treeElement instanceof SelectOnTargetValues)
			return this.processSelectOnTargetValues(treeElement, arrayCreationForTargets);

		// projection is included the first time that no "onTargetValues" is
		// used
		Operator<?> child = null;
		if (treeElement instanceof it.unibas.spicy.model.algebra.Join)
			child = this.processJoin(treeElement);
		else if (treeElement instanceof it.unibas.spicy.model.algebra.Difference)
			child = this.processDifference(treeElement);
		else if (treeElement instanceof Unnest)
			child = this.processUnnest(treeElement);
		else
			throw new IllegalArgumentException(
				"Schema is too complex and cannot be parsed. Spicy tree element cannot be processed "
					+ treeElement);

		if (arrayCreationForTargets != null) {
			final Projection tgd = new Projection().withInputs(child).
				withResultProjection(arrayCreationForTargets).
				withName("Projection " + treeElement.getId());
			return tgd; // don't save tgd-projection for reuse
		}
		return child;
	}

	private void processTree(final IAlgebraOperator treeRoot) {
		final Compose compose = (Compose) treeRoot;
		final Merge merge = (Merge) compose.getChildren().get(0); // always one merge
		// as child

		// build input list for every target
		// each setAlias represents one target instance, e.g. <v2 as
		// legalEntities>
		final HashMap<SetAlias, List<Operator<?>>> targetInputMapping = new HashMap<SetAlias, List<Operator<?>>>();
		for (final IAlgebraOperator child : merge.getChildren()) {
			final Nest nest = (Nest) child;
			final List<SetAlias> targetsOfTgd = this.getTargetsOfTGD(nest); // use with
			// getId()
			final Operator<?> childOperator = this.processNest(nest);

			for (final SetAlias target : targetsOfTgd)
				if (targetInputMapping.containsKey(target))
					targetInputMapping.get(target).add(childOperator);
				else {
					final List<Operator<?>> newList = new ArrayList<Operator<?>>();
					newList.add(childOperator);
					targetInputMapping.put(target, newList);
				}
		}

		// build operator for every target using the input
		final HashMap<SetAlias, Operator<?>> finalOperatorsIndex = new HashMap<SetAlias, Operator<?>>();
		int outputIndex = 0;
		for (final Entry<SetAlias, List<Operator<?>>> targetInput : targetInputMapping.entrySet()) {
			final int target = targetInput.getKey().getId();

			final UnionAll unionAll = new UnionAll().withInputs(targetInput
				.getValue());

			final Selection selectAndTransform = new Selection()
				.withInputs(unionAll)
				.withCondition(
					new UnaryExpression(new ArrayAccess(target)))
				.withResultProjection(new ArrayAccess(target));

			// duplicate removal
			final ObjectCreation finalSchema =
				this.createTargetSchemaFromOperatorList(targetInput.getValue(), target, outputIndex);

			final Grouping grouping = new Grouping().withInputs(selectAndTransform).
				withGroupingKey(new ObjectAccess(EntityMapping.idStr)).
				withResultProjection(finalSchema).
				withName("Grouping " + target);

			finalOperatorsIndex.put(targetInput.getKey(), grouping);
			outputIndex++;
		}

		for (final Entry<SetAlias, Operator<?>> entry : finalOperatorsIndex
			.entrySet()) {
			final String targetName = entry.getKey()
				.getAbsoluteBindingPathExpression().getPathSteps().get(1);

			final int i = this.outputIndex.get(targetName);
			SopremoUtil.LOG.debug("output " + this.outputIndex.get(targetName)
				+ " is " + targetName);
			this.module.getOutput(i).setInput(0, entry.getValue());
		}
		SopremoUtil.LOG.info("generated schema mapping module:\n " + this.module);
	}

	private ObjectCreation createTargetSchemaFromOperatorList(final List<Operator<?>> operatorList,
			final int targetIndex, final int outputIndex) {
		final ObjectCreation finalSchema = new ObjectCreation();
		for (final Operator<?> op : operatorList) {
			final ElementaryOperator<?> eop = (ElementaryOperator<?>) op;
			final ArrayCreation aa = eop.getResultProjection().findFirst(ArrayCreation.class);
			if (aa != null) {
				final ObjectCreation oc = (ObjectCreation) aa.get(targetIndex);
				if (oc != null)
					for (final Mapping<?> mapping : oc.getMappings()) {
						final String fieldName = mapping.getTargetExpression().findFirst(ObjectAccess.class).getField();
						// SpicyPathExpression lookupPath = new SpicyPathExpression(EntityMapping.targetStr +
						// EntityMapping.separator + EntityMapping.entitiesStr
						// + outputIndex + EntityMapping.separator + EntityMapping.entityStr + outputIndex , fieldName);
						//
						// boolean takeAll = false;
						// for(MappingValueCorrespondence mvc : this.mappingInformation.getValueCorrespondences()){
						// if(mvc.getTargetPath().equals(lookupPath) && mvc.isTakeAllValuesOfGrouping()){
						// takeAll = true;
						// break;
						// }
						// }
						// if(takeAll){
						// finalSchema.addMapping(fieldName,
						// FunctionUtil.createFunctionCall(CoreFunctions.ALL, new
						// ObjectAccess(fieldName).withInputExpression(new InputSelection(0))));
						// }else{
						// finalSchema.addMapping(fieldName,
						// FunctionUtil.createFunctionCall(CoreFunctions.FIRST, new
						// ObjectAccess(fieldName).withInputExpression(new InputSelection(0))));
						// }
						this.mappingTask.getTargetProxy().getSchema().getChildren();
						finalSchema.addMapping(
							fieldName,
							FunctionUtil.createFunctionCall(CoreFunctions.ALL,
								new ObjectAccess(fieldName).withInputExpression(new InputSelection(0))));
					}
			}
		}
		return finalSchema;
	}

	private List<SetAlias> getTargetsOfTGD(final Nest nest) {
		return this.tgdIndex.get(nest.getId()).getTargetView().getVariables();
	}

	private Operator<?> processNest(final Nest tgd) {
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
		final TGDGeneratorsMap generators = tgd.getGenerators();
		for (final SetAlias setAlias : this.getTargetsOfTGD(tgd)) { // generate
			// source-to-target
			// mapping for v2 or
			// v3 and add to
			// projection
			final Map<PathExpression, IValueGenerator> generatorVi = generators
				.getGeneratorsForVariable(setAlias);

			final TreeMap<PathExpression, IValueGenerator> st_map = new TreeMap<PathExpression, IValueGenerator>();
			for (final Entry<PathExpression, IValueGenerator> tgdAttribute : generatorVi.entrySet()) {
				// all mappings to this target
				final PathExpression spicyTargetPath = tgdAttribute.getKey();
				// consider only leafs
				if (!spicyTargetPath.getLastStep().equals(LEAF))
					continue;
				st_map.put(spicyTargetPath, tgdAttribute.getValue()); // ("fullname.firstname", createPath("v0",
																		// "old_name")
																		// +
																		// function)
			}
			final EvaluationExpression objectVi = this.correspondenceTransformation
				.createNestedObjectFromSpicyPaths(generatorVi, setAlias, this.mappingTask.getTargetProxy().getSchema());
			// TODO bäääh
			final List<EvaluationExpression> currentElements = arrayCreationForTargets.getElements();
			while (currentElements.size() <= setAlias.getId())
				currentElements.add(ConstantExpression.MISSING);
			currentElements.set(setAlias.getId(), objectVi);
			arrayCreationForTargets = new ArrayCreation(currentElements); // e.g.
			// v3,
			// {target-attributes}
		}

		final Project project = (Project) tgd.getChildren().get(0); // we can ignore
		// project here,
		// because
		// created
		// transformation
		// includes a
		// projection,
		// too
		final Operator<?> child = this.processChild(project.getChildren().get(0),
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
			final IAlgebraOperator treeElement,
			final ArrayCreation arrayCreationForTargets) {
		final Operator<?> child = this.processChild(treeElement.getChildren().get(0),
			arrayCreationForTargets); // nothin
										// todo
		return child;
	}

	private Operator<?> processDifference(final IAlgebraOperator treeElement) {
		final it.unibas.spicy.model.algebra.Difference difference =
			(it.unibas.spicy.model.algebra.Difference) treeElement;

		if (this.reuseProjections.containsKey(difference.getId()))
			return this.reuseProjections.get(difference.getId());

		final Operator<?> child0 = this.processChild(difference.getChildren().get(0), null);
		final Operator<?> child1 = this.processChild(difference.getChildren().get(1), null);

		// antijoin condition
		final ArrayCreation arrayLeft = new ArrayCreation();
		for (final VariablePathExpression path : difference.getLeftPaths())
			arrayLeft.add(EntityMappingUtil.convertSpicyPath("0", path));
		final ArrayCreation arrayRight = new ArrayCreation();
		for (final VariablePathExpression path : difference.getRightPaths())
			arrayRight.add(EntityMappingUtil.convertSpicyPath("1", path));

		final TwoSourceJoin antiJoin = new TwoSourceJoin().withInputs(child0, child1).withCondition(
			new ElementInSetExpression(arrayLeft, Quantor.EXISTS_NOT_IN, arrayRight));
		antiJoin.setResultProjection(new AggregationExpression(new ArrayUnion()));

		this.reuseJoins.put(difference.getId(), antiJoin);
		return antiJoin;
	}

	private Operator<?> processDifferenceOnTargetValues(
			final IAlgebraOperator treeElement,
			final ArrayCreation arrayCreationForTargets) {
		final it.unibas.spicy.model.algebra.DifferenceOnTargetValues difference =
			(it.unibas.spicy.model.algebra.DifferenceOnTargetValues) treeElement;

		if (this.reuseProjections.containsKey(difference.getId()))
			return this.reuseProjections.get(difference.getId());

		final Operator<?> child0 = this.processChild(difference.getChildren().get(0),
			arrayCreationForTargets);
		final Operator<?> child1 = this.processChild(difference.getChildren().get(1),
			arrayCreationForTargets);

		// antijoin condition
		final ArrayCreation arrayLeft = new ArrayCreation();
		for (final VariableCorrespondence varCor : difference
			.getLeftCorrespondences())
			arrayLeft.add(EntityMappingUtil.convertSpicyPath("0",
				varCor.getTargetPath()));
		final ArrayCreation arrayRight = new ArrayCreation();
		for (final VariableCorrespondence varCor : difference
			.getRightCorrespondences())
			arrayRight.add(EntityMappingUtil.convertSpicyPath("1",
				varCor.getTargetPath()));
		final TwoSourceJoin antiJoin = new TwoSourceJoin().withInputs(child0, child1).
			withCondition(new ElementInSetExpression(arrayLeft, Quantor.EXISTS_NOT_IN, arrayRight));

		this.reuseJoins.put(difference.getId(), antiJoin);
		return antiJoin;
	}

	private Operator<?> processJoin(final IAlgebraOperator treeElement) {
		final it.unibas.spicy.model.algebra.Join spicyJoin = (it.unibas.spicy.model.algebra.Join) treeElement;

		if (this.reuseProjections.containsKey(spicyJoin.getId()))
			return this.reuseProjections.get(spicyJoin.getId());

		final Operator<?> child0 = this.processChild(spicyJoin.getChildren().get(0), null);
		final Operator<?> child1 = this.processChild(spicyJoin.getChildren().get(1), null);

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
		final TwoSourceJoin sopremoJoin =
			new TwoSourceJoin().withInputs(child0, child1).
				withCondition(
					new ComparativeExpression(EntityMappingUtil.convertSpicyPath("0",
						spicyJoin.getJoinCondition().getFromPaths().get(0)), BinaryOperator.EQUAL,
						EntityMappingUtil.convertSpicyPath("1", spicyJoin.getJoinCondition().getToPaths().get(0))));
		sopremoJoin.setResultProjection(new AggregationExpression(new ArrayUnion()));

		this.reuseJoins.put(spicyJoin.getId(), sopremoJoin);
		return sopremoJoin;
	}

	private Operator<?> processJoinOnTargetValues(final IAlgebraOperator treeElement) {
		final it.unibas.spicy.model.algebra.JoinOnTargetValues spicyJoin =
			(it.unibas.spicy.model.algebra.JoinOnTargetValues) treeElement;

		if (this.reuseProjections.containsKey(spicyJoin.getId()))
			return this.reuseProjections.get(spicyJoin.getId());

		// rewrite projections left and right with correspondences
		final ArrayCreation arrayCreationForTargetsLeft = this.correspondenceTransformation
			.createArrayFromSpicyPaths(spicyJoin
				.getLeftCorrespondences());
		final ArrayCreation arrayCreationForTargetsRight = this.correspondenceTransformation
			.createArrayFromSpicyPaths(spicyJoin
				.getRightCorrespondences());

		final Operator<?> child0 = this.processChild(spicyJoin.getChildren().get(0),
			arrayCreationForTargetsLeft);
		final Operator<?> child1 = this.processChild(spicyJoin.getChildren().get(1),
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

		final TwoSourceJoin sopremoJoin = new TwoSourceJoin().withInputs(child0,
			child1).withCondition(
			new ComparativeExpression(EntityMappingUtil
				.convertSpicyPath("0", spicyJoin.getJoinCondition()
					.getFromPaths().get(0)), BinaryOperator.EQUAL,
				EntityMappingUtil.convertSpicyPath("1", spicyJoin
					.getJoinCondition().getToPaths().get(0))));

		this.reuseJoins.put(spicyJoin.getId(), sopremoJoin);
		return sopremoJoin;
	}

	private Operator<?> processUnnest(final IAlgebraOperator treeElement) {
		final Unnest unnest = (Unnest) treeElement; // e.g.
		// "unnest v0 in usCongress.usCongressMembers"
		final SetAlias sourceAlias = unnest.getVariable();
		final int sourceId = sourceAlias.getId(); // v0
		final String sourceName = sourceAlias.getBindingPathExpression()
			.getLastStep(); // usCongressMembers

		if (this.reuseProjections.containsKey(sourceId))
			return this.reuseProjections.get(sourceId);

		// TODO unnest can have selectionCondition and provenanceCondition,
		// research: what is this

		final ArrayCreation positionEncodedArray = new ArrayCreation();
		for (int i = 0; i < sourceId; i++)
			positionEncodedArray.add(ConstantExpression.MISSING);
		positionEncodedArray.add(new ObjectCreation(new ObjectCreation.CopyFields(createPath("0"))));

		final Projection projection = new Projection().withResultProjection(positionEncodedArray)
			.withInputs(this.module.getInput(this.inputIndex.get(sourceName)));

		SopremoUtil.LOG.debug("Source Projection from: " + unnest.toString()
			+ " reads input named " + sourceName + " at input index "
			+ this.inputIndex.get(sourceName));

		this.reuseProjections.put(sourceId, projection);
		return projection;
	}

	/**
	 * @param mappingExpression
	 * @param foreignKeys
	 */
	public void setMappingTask(ArrayCreation mappingExpression, BooleanExpression foreignKeys) {
		this.mappingTask = new MappingTask(null, null, ICON_COLOR_16x16);
	}
}
