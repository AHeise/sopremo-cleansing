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

import it.unibas.spicy.model.algebra.query.operators.xquery.XQNames;
import it.unibas.spicy.model.algebra.query.operators.xquery.XQUtility;
import it.unibas.spicy.model.correspondence.ConstantValue;
import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.datasource.DataSource;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.datasource.KeyConstraint;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.MetadataNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.datasource.nodes.TupleNode;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.generators.IValueGenerator;
import it.unibas.spicy.model.generators.NullValueGenerator;
import it.unibas.spicy.model.generators.TGDGeneratorsMap;
import it.unibas.spicy.model.mapping.*;
import it.unibas.spicy.model.paths.*;
import it.unibas.spicy.model.paths.operators.GeneratePathExpression;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTaskTgds;

import java.util.*;

import com.google.common.collect.Lists;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.base.Grouping;
import eu.stratosphere.sopremo.base.Join;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyUtil.InputManager;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyUtil.StreamManager;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.ObjectCreation.SymbolicAssignment;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Internal;
import eu.stratosphere.sopremo.operator.JsonStream;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Reads a Spicy MappingTask and create Sopremo Operator
 */
@Internal
public class SpicyMappingTransformation extends DataTransformationBase<SpicyMappingTransformation> {

	private transient Map<FORule, TGDGeneratorsMap> generatorsMaps = new HashMap<FORule, TGDGeneratorsMap>();

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere
	 * .sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(final SopremoModule module) {
		try {
			this.generatorsMaps.clear();
			MappingTask mappingTask = getMappingTask();
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
				VariablePathExpression relativePath =
					this.gpe.generateRelativePath(child, mappingTask.getTargetProxy());
				SetAlias setVariable = relativePath.getStartingVariable();
				outputs.add(variableToSourceMapper.getStream(XQNames.finalXQueryNameSTExchange(setVariable)));
			}

			for (int index = 0; index < outputs.size(); index++)
				module.getOutput(index).setInput(0, outputs.get(index));
		} catch (RuntimeException e) {
			throw e;
		}
		this.generatorsMaps.clear();
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
			VariablePathExpression relativePath =
				this.gpe.generateRelativePath(node, mappingTask.getTargetProxy());
			SetAlias setVariable = relativePath.getStartingVariable();
			String setNodeVariableName = XQNames.finalXQueryNameSTExchange(setVariable);
			JsonStream input = variableToSourceMapper.getStream(setNodeVariableName);
			if (input == null) {
				variableToSourceMapper.put(setNodeVariableName, new Source(new ArrayCreation()));
				return;
			}
			SetAlias fatherVariable = setVariable.getBindingPathExpression().getStartingVariable();
			if (fatherVariable != null) {
				ObjectCreation agg = new ObjectCreation();
				BatchAggregationExpression bae = new BatchAggregationExpression();
				agg.addMapping("content",
					bae.add(CoreFunctions.ALL, getFinalProjectionForSet(mappingTask, children.get(0))));
				agg.addMapping(XQUtility.SET_ID, bae.add(CoreFunctions.FIRST, new ObjectAccess(XQUtility.SET_ID)));

				Grouping grouping = new Grouping().
					withName("Final nest " + setNodeVariableName).
					withInputs(input).
					withGroupingKey(new ObjectAccess(XQUtility.SET_ID)).
					withResultProjection(agg);

				ObjectCreation oc2 = new ObjectCreation();
				oc2.addMapping(new ObjectCreation.CopyFields(new InputSelection(0)));
				ObjectCreation contentExpr = new ObjectCreation();
				contentExpr.addMapping(new ObjectCreation.CopyFields(JsonUtil.createPath("0", "content")));
				// for (INode child : children) {
				VariablePathExpression childPath =
					this.gpe.generateRelativePath(node, mappingTask.getTargetProxy());
				contentExpr.addMapping(SpicyUtil.nameForPath(childPath), JsonUtil.createPath("1", "content"));
				// }
				oc2.addMapping("content", contentExpr);

				String streamName = XQNames.finalXQueryNameSTExchange(fatherVariable);
				Join join =
					new Join().
						withName("Final nest " + setNodeVariableName).
						withInputs(variableToSourceMapper.getStream(streamName), grouping).
						withJoinCondition(
							new ComparativeExpression(JsonUtil.createPath("0",
								setVariable.getBindingPathExpression().getLastStep()),
								BinaryOperator.EQUAL, JsonUtil.createPath("1", XQUtility.SET_ID))).
						withResultProjection(oc2);
				variableToSourceMapper.put(streamName, join);
			} else {
				INode firstChild = children.get(0);
				Projection projection = new Projection().
					withName("Final nest " + setNodeVariableName).
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
	private EvaluationExpression getFinalProjectionForSet(MappingTask mappingTask, INode firstChild) {
		// ObjectCreation oc = new ObjectCreation();
		// if (firstChild instanceof TupleNode) {
		// for (INode child : firstChild.getChildren()) {
		// if (child instanceof SetNode) {
		// oc.addMapping(child.getLabel(), JsonUtil.createPath("content", child.getLabel()));
		// } else {
		// VariablePathExpression childPath =
		// gpe.generateRelativePath(child, mappingTask.getTargetProxy());
		// oc.addMapping(child.getLabel(), JsonUtil.createPath("content", SpicyUtil.nameForPath(childPath)));
		// }
		// }
		// } else {
		// oc.addMapping(firstChild.getLabel(), JsonUtil.createPath("content"));
		// }
		return SpicyUtil.spicyToSopremo(firstChild, mappingTask.getTargetProxy());
	}

	private JsonStream generate(ComplexQueryWithNegations query, MappingTask mappingTask,
			StreamManager streamManager) {
		String posView = XQNames.xQueryNameForPositiveView(query);
		JsonStream cached = streamManager.getStream(posView);
		if (cached == null)
			streamManager.put(posView,
				cached = generatePositiveQuery(query, mappingTask, streamManager).withName(posView));
		if (!query.getNegatedComplexQueries().isEmpty()) {
			for (NegatedComplexQuery negatedComplexQuery : query.getNegatedComplexQueries()) {
				generate(negatedComplexQuery.getComplexQuery(), mappingTask, streamManager);
			}
			String viewName = XQNames.xQueryNameForViewWithIntersection(query);
			generateDifferenceForNegation(query, viewName, posView, streamManager);
		}
		return cached;
	}

	private void generateDifferenceForNegation(ComplexQueryWithNegations query, String viewName,
			String positiveViewName,
			StreamManager streamManager) {
		List<JsonStream> inputs = Lists.newArrayList(streamManager.getStream(positiveViewName));
		List<BooleanExpression> joinConditions = new ArrayList<BooleanExpression>();
		for (int i = 0; i < query.getNegatedComplexQueries().size(); i++) {
			NegatedComplexQuery negatedComplexQuery = query.getNegatedComplexQueries().get(i);
			inputs.add(findInputForNegativeQuery(negatedComplexQuery, streamManager));
			joinConditions.addAll(generateAttributesForDifference(negatedComplexQuery, i + 1));
		}

		// bug in Spicy caused by constant expressions
		if (joinConditions.isEmpty())
			streamManager.put(viewName, new Source(new ArrayCreation()));
		else
			streamManager.put(viewName, new Join().withInputs(inputs).withName(viewName).
				withJoinCondition(new AndExpression(joinConditions)).
				withResultProjection(new InputSelection(0)));
	}

	private JsonStream findInputForNegativeQuery(NegatedComplexQuery negation, StreamManager streamManager) {
		ComplexQueryWithNegations negatedQuery = negation.getComplexQuery();
		JsonStream negativeStream = streamManager.getStream(negatedQuery.getId());
		if (negativeStream != null)
			return negativeStream;

		if (negatedQuery.getNegatedComplexQueries().isEmpty())
			return streamManager.getStream(XQNames.xQueryNameForPositiveView(negatedQuery));

		return streamManager.getStream(XQNames.xQueryNameForViewWithIntersection(negatedQuery));
	}

	private Operator<?> generatePositiveQuery(ComplexQueryWithNegations query, MappingTask mappingTask,
			StreamManager streamManager) {
		List<JsonStream> inputs = streamManager.getStreams(query.getVariables());
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

	private List<ElementInSetExpression> generateAttributesForDifference(NegatedComplexQuery negation, int inputIndex) {
		List<ElementInSetExpression> expressions = new ArrayList<ElementInSetExpression>();
		InputSelection firstIn = new InputSelection(0);
		InputSelection secondIn = new InputSelection(inputIndex);
		if (negation.isTargetDifference()) {
			for (int i = 0; i < negation.getTargetEqualities().getLeftCorrespondences().size(); i++) {
				VariableCorrespondence leftCorrespondence =
					negation.getTargetEqualities().getLeftCorrespondences().get(i);
				VariableCorrespondence rightCorrespondence =
					negation.getTargetEqualities().getRightCorrespondences().get(i);
				if (!leftCorrespondence.isConstant()) {
					String leftPathName = SpicyUtil.nameForPath(leftCorrespondence.getFirstSourcePath());
					String rightPathName = SpicyUtil.nameForPath(rightCorrespondence.getFirstSourcePath());
					expressions.add(new ElementInSetExpression(
						new ObjectAccess(leftPathName).withInputExpression(firstIn),
						Quantor.EXISTS_NOT_IN, new ObjectAccess(rightPathName).withInputExpression(secondIn)));
				}
			}
		} else {
			for (int i = 0; i < negation.getSourceEqualities().getLeftPaths().size(); i++) {
				VariablePathExpression leftPath = negation.getSourceEqualities().getLeftPaths().get(i);
				VariablePathExpression rightPath = negation.getSourceEqualities().getRightPaths().get(i);
				String leftPathName = SpicyUtil.nameForPath(leftPath);
				String rightPathName = SpicyUtil.nameForPath(rightPath);
				expressions.add(new ElementInSetExpression(new ObjectAccess(leftPathName).withInputExpression(firstIn),
					Quantor.EXISTS_NOT_IN, new ObjectAccess(rightPathName).withInputExpression(secondIn)));
			}
		}
		return expressions;
	}

	private EvaluationExpression generateSimpleCopyValuesFromSource(ComplexQueryWithNegations query,
			MappingTask mappingTask,
			InputManager streamManager) {
		ObjectCreation oc = new ObjectCreation();
		List<VariablePathExpression> sourceAttributes = extractAttributePaths(query, mappingTask);
		for (int i = 0; i < sourceAttributes.size(); i++) {
			final VariablePathExpression attributePath = sourceAttributes.get(i);
			final EvaluationExpression expression;
			INode attributeNode = attributePath.getLastNode(mappingTask.getSourceProxy().getIntermediateSchema());
			if (attributeNode instanceof MetadataNode) {
				expression = SpicyUtil.createRelativePathForMetadataNode(attributePath, attributeNode, streamManager);
			} else if (attributeNode.isVirtual()) {
				expression =
					SpicyUtil.createRelativePathForVirtualAttribute(attributePath, attributeNode, streamManager);
			} else if (isSingleAttributeWithVirtualFathers((AttributeNode) attributeNode)) {
				expression =
					SpicyUtil.createRelativePathForSingleAttribute(attributePath, attributeNode, streamManager);
			} else {
				expression = SpicyUtil.createRelativePath(attributePath, streamManager);
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

	private AndExpression generateWhereClauseForView(ComplexQueryWithNegations query, InputManager streamManager) {
		List<VariableSelectionCondition> selectionConditions = query.getComplexQuery().getAllSelections();
		List<VariableJoinCondition> joinConditions =
			new ArrayList<VariableJoinCondition>(query.getComplexQuery().getJoinConditions());
		for (SimpleConjunctiveQuery simpleConjunctiveQuery : query.getComplexQuery().getConjunctions()) {
			joinConditions.addAll(simpleConjunctiveQuery.getAllJoinConditions());
		}
		List<BooleanExpression> expressions = new ArrayList<BooleanExpression>();
		expressions.addAll(generateWhereContentFromJoinConditions(joinConditions,
			query.getComplexQuery().getAllCorrespondences(),
			streamManager));
		expressions.addAll(generateWhereContentFromSelectionConditions(selectionConditions));
		expressions.addAll(generateWhereContentFromIntersection(query.getComplexQuery(), streamManager));
		return new AndExpression(expressions);
	}

	private List<BooleanExpression> generateWhereContentFromIntersection(ComplexConjunctiveQuery view,
			InputManager streamManager) {
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
				expressions.add(new ComparativeExpression(SpicyUtil.createRelativePath(leftSourcePath, streamManager),
					BinaryOperator.EQUAL, SpicyUtil.createRelativePath(rightSourcePath, streamManager)));
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
			List<VariableCorrespondence> correspondences, InputManager streamManager) {
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
				expressions.add(new ComparativeExpression(SpicyUtil.createRelativePath(fromSourcePath, streamManager),
					BinaryOperator.EQUAL, SpicyUtil.createRelativePath(toSourcePath, streamManager)));
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
		if (XQUtility.hasDifferences(rule)) {
			generateDifferenceForFinalRule(rule, streamManager);
			generateTargetValueViewWithDifference(rule, mappingTask, streamManager);
		} else {
			generateTargetValueViewWithoutDifference(rule, mappingTask, streamManager);
		}
	}

	private void generateTargetValueViewWithDifference(FORule rule, MappingTask mappingTask, StreamManager streamManager) {
		String fromViewName = XQNames.xQueryNameForTgd(rule);
		String viewName = XQNames.xQueryFinalTgdName(rule);
		JsonStream input = streamManager.getStream(fromViewName);
		Projection projection = new Projection().withName(viewName).
			withInputs(input).
			withResultProjection(projectionOnValues(mappingTask, rule, new InputManager("$variable")));

		streamManager.put(viewName, projection);

	}

	private void generateDifferenceForFinalRule(FORule rule, StreamManager streamManager) {
		String viewName = XQNames.xQueryNameForTgd(rule);
		String positiveViewName = XQNames.xQueryNameForViewWithIntersection(rule.getComplexSourceQuery());
		generateDifferenceForNegation(rule.getComplexSourceQuery(), viewName, positiveViewName, streamManager);
	}

	private void generateTargetValueViewWithoutDifference(FORule rule, MappingTask mappingTask,
			StreamManager streamManager) {
		String fromViewName;
		if (rule.getComplexSourceQuery().getComplexQuery().hasIntersection()) {
			fromViewName = XQNames.xQueryNameForViewWithIntersection(rule.getComplexSourceQuery());
		} else {
			fromViewName = XQNames.xQueryNameForPositiveView(rule.getComplexSourceQuery());
		}
		JsonStream input = streamManager.getStream(fromViewName);
		String viewName = XQNames.xQueryFinalTgdName(rule);
		Projection projection = new Projection().withName(viewName).
			withInputs(input).
			withResultProjection(projectionOnValues(mappingTask, rule, new InputManager("$variable")));

		streamManager.put(viewName, projection);
	}

	private EvaluationExpression projectionOnValues(MappingTask mappingTask, FORule tgd, InputManager inputManager) {
		ObjectCreation oc = new ObjectCreation();
		Set<VariablePathExpression> generatedAttributes = new HashSet<VariablePathExpression>();

		List<SetAlias> generators = tgd.getTargetView().getGenerators();
		TGDGeneratorsMap tgdGeneratorsMap = getGeneratorsMap(tgd, mappingTask);
		for (int i = 0; i < generators.size(); i++) {
			SetAlias generator = generators.get(i);
			List<VariablePathExpression> attributes =
				generator.getAttributes(mappingTask.getTargetProxy().getIntermediateSchema());
			for (int j = 0; j < attributes.size(); j++) {
				VariablePathExpression attribute = attributes.get(j);
				if (generatedAttributes.contains(attribute)) {
					continue;
				}
				generatedAttributes.add(attribute);
				IValueGenerator leafGenerator = getLeafGenerator(attribute, tgdGeneratorsMap, mappingTask, generator);

				String elementName = SpicyUtil.nameForPath(attribute);
				oc.addMapping(elementName,
					SpicyUtil.valueForLeaf(leafGenerator, attribute, tgd, mappingTask));
			}
		}
		return oc;
	}

	private TGDGeneratorsMap getGeneratorsMap(FORule tgd, MappingTask mappingTask) {
		// TGDGeneratorsMap map = this.generatorsMaps.get(tgd);
		// if (map != null)
		// return map;
		// TGDGeneratorsMap original =
		// new it.unibas.spicy.model.generators.operators.GenerateValueGenerators().generateValueGenerators(tgd,
		// mappingTask);
		TGDGeneratorsMap custom = new GenerateValueGenerators().generateValueGenerators(tgd, mappingTask);
		this.generatorsMaps.put(tgd, custom);
		return custom;
	}

	private IValueGenerator getLeafGenerator(VariablePathExpression attributePath, TGDGeneratorsMap tgdGeneratorsMap,
			MappingTask mappingTask, SetAlias variable) {
		INode attributeNode = attributePath.getLastNode(mappingTask.getTargetProxy().getIntermediateSchema());
		INode leafNode = attributeNode.getChild(0);
		PathExpression leafPath = this.gpe.generatePathFromRoot(leafNode);
		Map<PathExpression, IValueGenerator> generatorsForVariable =
			tgdGeneratorsMap.getGeneratorsForVariable(variable);
		// //** added to avoid exceptions in XML scenarios
		if (generatorsForVariable == null) {
			return NullValueGenerator.getInstance();
		}
		for (PathExpression pathExpression : generatorsForVariable.keySet()) {
			if (pathExpression.equalsUpToClones(leafPath)) {
				return generatorsForVariable.get(pathExpression);
			}
		}
		throw new IllegalStateException();
	}

	private void materializeResultOfExchange(MappingTask mappingTask, List<FORule> tgds, StreamManager streamManager) {
		List<SetAlias> targetVariables = mappingTask.getTargetProxy().getMappingData().getVariables();
		for (int i = 0; i < targetVariables.size(); i++) {
			SetAlias targetVariable = targetVariables.get(i);
			List<FORule> relevantTGDs =
				findRelevantTGDs(targetVariable, mappingTask.getMappingData().getRewrittenRules());
			if (!relevantTGDs.isEmpty()) {
				String viewName = XQNames.finalXQueryNameSTExchange(targetVariable);
				streamManager.put(viewName,
					generateBlockForShredding(targetVariable, mappingTask, relevantTGDs, streamManager));
			}
		}
	}

	private JsonStream generateBlockForShredding(SetAlias targetVariable, MappingTask mappingTask,
			List<FORule> relevantTgds,
			StreamManager streamManager) {
		List<Projection> projections = new ArrayList<Projection>();
		for (int i = 0; i < relevantTgds.size(); i++) {
			FORule rule = relevantTgds.get(i);
			// String fromViewName = XQUtility.findViewName(tgd, mappingTask);
			String fromViewName = XQNames.xQueryFinalTgdName(rule);
			Projection projection =
				new Projection().withName(XQNames.finalXQueryNameSTExchange(targetVariable)).
					withInputs(streamManager.getStream(fromViewName)).
					withResultProjection(
						generateReturnClauseForSTResult(targetVariable, mappingTask, rule,
							new InputManager("$variable")));

			projections.add(projection);
		}
		return new Union().withInputs(projections);
	}

	private EvaluationExpression generateReturnClauseForSTResult(SetAlias targetVariable,
			MappingTask mappingTask, FORule rule, InputManager inputManager) {
		ObjectCreation oc = new ObjectCreation();
		TGDGeneratorsMap tgdGeneratorsMap = getGeneratorsMap(rule, mappingTask);
		SetNode setNode = targetVariable.getBindingNode(mappingTask.getTargetProxy().getIntermediateSchema());
		INode tupleNode = setNode.getChild(0);
		oc.addMapping(XQUtility.SET_ID,
			createIdFromGenerators(setNode, mappingTask, tgdGeneratorsMap, targetVariable, inputManager));
		List<INode> setNodeChildren = findSetChildren(tupleNode);
		// generate copy values from tgd view
		ObjectCreation content = new ObjectCreation();
		// if (targetVariable.getBindingPathExpression().getStartingVariable() == null) {
		// List<VariablePathExpression> targetPaths =
		// targetVariable.getAttributes(mappingTask.getTargetProxy().getIntermediateSchema());
		// for (int i = 0; i < targetPaths.size(); i++) {
		// VariablePathExpression targetPath = targetPaths.get(i);
		// if (targetPath.getStartingVariable().equals(targetVariable)) {
		// String targetPathName = SpicyUtil.nameForPath(targetPath);
		// content.addMapping(targetPathName,
		// new ObjectAccess(SpicyUtil.nameForPath(targetPath)).withInputExpression(new InputSelection(0)));
		// }
		// }
		// }
		// else
		for (SetAlias ruleVariable : rule.getTargetView().getVariables()) {
			for (SetAlias sourceVariable = ruleVariable; sourceVariable != null; sourceVariable =
				sourceVariable.getBindingPathExpression().getStartingVariable())
				if (sourceVariable.getAbsoluteBindingPathExpression().equals(
					targetVariable.getAbsoluteBindingPathExpression())) {
					List<VariablePathExpression> targetPaths =
						sourceVariable.getAttributes(mappingTask.getTargetProxy().getIntermediateSchema());
					for (VariablePathExpression targetPath : targetPaths) {
						if (targetPath.getStartingVariable().equals(sourceVariable)) {
							VariablePathExpression sourcePath = targetPath;
							String targetPathName = SpicyUtil.nameForPath(new VariablePathExpression(targetVariable,
								targetPath.getPathSteps()));
							content.addMapping(
								targetPathName,
								new ObjectAccess(SpicyUtil.nameForPath(sourcePath)).withInputExpression(new InputSelection(
									0)));
						}
					}
				}
		}
		oc.addMapping("content", content);
		// add the set id of all children sets
		for (int i = 0; i < setNodeChildren.size(); i++) {
			SetNode targetNode = (SetNode) setNodeChildren.get(i);
			oc.addMapping(targetNode.getLabel(),
				createIdFromGenerators(targetNode, mappingTask, tgdGeneratorsMap, targetVariable, inputManager));
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

	private EvaluationExpression createIdFromGenerators(INode node, MappingTask mappingTask,
			TGDGeneratorsMap tgdGeneratorsMap, SetAlias variable,
			InputManager inputManager) {
		// List<EvaluationExpression> ids = new ArrayList<EvaluationExpression>();
		// do {
		// PathExpression pathExpression = gpe.generatePathFromRoot(node);
		// IValueGenerator valueGenerator = getNodeGenerator(pathExpression, mappingTask, tgd, variable);
		// ids.add(0, SpicyUtil.xqueryValueForIntermediateNode(valueGenerator, mappingTask, inputManager));
		// node = node.getFather();
		// } while (node != null);
		//
		// if (ids.size() == 1)
		// return ids.get(0);
		// return FunctionUtil.createFunctionCall(CoreFunctions.CONCAT, new ArrayCreation(ids));
		PathExpression pathExpression = this.gpe.generatePathFromRoot(node);
		IValueGenerator valueGenerator = tgdGeneratorsMap.getGeneratorsForFatherVariable(variable).get(pathExpression);
		return SpicyUtil.valueForIntermediateNode(valueGenerator, mappingTask, inputManager);
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

	private final GeneratePathExpression gpe = new GeneratePathExpression();

	private String taskPath;

	/**
	 * Sets the taskPath to the specified value.
	 * 
	 * @param taskPath
	 *        the taskPath to set
	 */
	public void setTaskPath(String taskPath) {
		if (taskPath == null)
			throw new NullPointerException("taskPath must not be null");

		this.taskPath = taskPath;
		try {
			loadMappingTask();
		} catch (DAOException e) {
			throw new IllegalArgumentException("Invalid task description", e);
		}
	}

	/**
	 * Returns the taskPath.
	 * 
	 * @return the taskPath
	 */
	public String getTaskPath() {
		return this.taskPath;
	}

	private MappingTask getMappingTask() {
		if (this.taskPath != null)
			try {
				return loadMappingTask();
			} catch (DAOException e) {
				// should not happen, since we already checked the description
				throw new IllegalStateException("Invalid task description", e);
			}

		INode targetSchema = SpicyUtil.toSpicySchema(getTargetSchema(), "Target");
		final DataSource target = new DataSource("XML", targetSchema);

		PathExpression sourceRoot = new PathExpression(Lists.newArrayList("Source")), targetRoot =
			new PathExpression(Lists.newArrayList("Target"));
		final DataSource source = new DataSource("XML", SpicyUtil.toSpicySchema(getSourceSchema(), "Source"));
		BitSet sourcePkSet = new BitSet(), targetPkSet = new BitSet();
		for (EvaluationExpression key : getSourceKeys()) {
			int sourceIndex = key.findFirst(InputSelection.class).getIndex();
			source.addKeyConstraint(new KeyConstraint(SpicyUtil.toSpicyPath(key, sourceRoot),
				!sourcePkSet.get(sourceIndex)));
			sourcePkSet.set(sourceIndex, true);
		}
		for (EvaluationExpression key : getTargetKeys()) {
			int targetIndex = key.findFirst(InputSelection.class).getIndex();
			target.addKeyConstraint(new KeyConstraint(SpicyUtil.toSpicyPath(key, targetRoot),
				!targetPkSet.get(targetIndex)));
			targetPkSet.set(targetIndex, true);
		}

		List<ValueCorrespondence> corrs = new ArrayList<ValueCorrespondence>();
		for (SymbolicAssignment valueCorrespondence : getSourceToValueCorrespondences()) {
			final List<PathExpression> sourcePaths = new ArrayList<PathExpression>();
			SopremoFunctionExpression transformationFunction =
				SpicyUtil.extractTransformation(valueCorrespondence.getExpression(), sourceRoot, sourcePaths);
			// constant expression
			if (sourcePaths.isEmpty())
				corrs.add(new ValueCorrespondence(null, new SopremoFunctionConstantValue(transformationFunction),
					SpicyUtil.toSpicyPath(valueCorrespondence.getTargetTagExpression(), targetRoot),
					transformationFunction, 1));
			else
				corrs.add(new ValueCorrespondence(sourcePaths,
					SpicyUtil.toSpicyPath(valueCorrespondence.getTargetTagExpression(), targetRoot),
					transformationFunction));
		}
		// create mapping task
		MappingTask mappingTask = new MappingTask(source, target, corrs);
		mappingTask.getConfig().setRewriteEGDs(true);
		mappingTask.getConfig().setRewriteOverlaps(true);

		for (final SymbolicAssignment cond : getSourceFKs())
			mappingTask.getSourceProxy().addJoinCondition(
				new JoinCondition(SpicyUtil.toSpicyPath(cond.getExpression(), sourceRoot),
					SpicyUtil.toSpicyPath(cond.getTargetTagExpression(), sourceRoot)));
		for (final SymbolicAssignment cond : getTargetFKs())
			mappingTask.getTargetProxy().addJoinCondition(
				new JoinCondition(Lists.newArrayList(SpicyUtil.toSpicyPath(cond.getExpression(), targetRoot)),
					Lists.newArrayList(SpicyUtil.toSpicyPath(cond.getTargetTagExpression(), targetRoot)), true));

		return mappingTask;
	}

	private MappingTask loadMappingTask() throws DAOException {
		// read file, create task
		return new DAOMappingTaskTgds().loadMappingTask(this.taskPath);
	}
}
