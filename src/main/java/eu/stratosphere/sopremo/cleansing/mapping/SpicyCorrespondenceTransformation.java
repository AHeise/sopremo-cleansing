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

import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.datasource.nodes.TupleNode;
import it.unibas.spicy.model.datasource.operators.FindNode;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.generators.FunctionGenerator;
import it.unibas.spicy.model.generators.IValueGenerator;
import it.unibas.spicy.model.generators.NullValueGenerator;
import it.unibas.spicy.model.generators.SkolemFunctionGenerator;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.model.paths.SetAlias;
import it.unibas.spicy.model.paths.VariableCorrespondence;
import it.unibas.spicy.model.paths.VariablePathExpression;

import java.util.*;
import java.util.Map.Entry;

import org.nfunk.jep.ASTConstant;
import org.nfunk.jep.ASTFunNode;
import org.nfunk.jep.ASTVarNode;
import org.nfunk.jep.Node;

import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.util.CollectionUtil;

/**
 * Reads Spicy Correspondence, i.e. source attribute to target attribute and creates Sopremo transformation.
 * Attributes can be nested (no arrays yet) and transformations can include functions.
 * 
 * @author Andrina Mascher, Arvid Heise
 */
public class SpicyCorrespondenceTransformation {

	public static EvaluationExpression createNestedObjectFromSpicyPaths(final Map<PathExpression, IValueGenerator> generatorVi,
			final SetAlias setAlias, final INode schema) {
		FindNode findNode = new FindNode();
		INode rootNode = schema;

		Map<List<String>, EvaluationExpression> expressions = new HashMap<List<String>, EvaluationExpression>();
		for (Entry<PathExpression, IValueGenerator> pathElement : generatorVi.entrySet()) {
			PathExpression path = pathElement.getKey();
			if (path.getLastStep().equals("LEAF")) {
				List<String> steps = path.getPathSteps();
				int incompleteIndex = 0;
				for (; incompleteIndex < steps.size(); incompleteIndex++)
					if (expressions.get(steps.subList(0, incompleteIndex)) == null)
						break;
				EvaluationExpression newChild = recursivelyCreate(steps,
					findNode.findNodeInSchema(new PathExpression(new ArrayList<String>(steps.subList(0, incompleteIndex))), rootNode),
					incompleteIndex, pathElement.getValue(), expressions);
				if (incompleteIndex > 0) {
					EvaluationExpression parentExpr = expressions.get(steps.subList(0, incompleteIndex));
					if (parentExpr instanceof ObjectCreation)
						((ObjectCreation) parentExpr).addMapping(steps.get(incompleteIndex), newChild);
					else
						((ArrayCreation) parentExpr).add(newChild);
				}
			}
		}
		return expressions.get(Collections.EMPTY_LIST);
	}

	private static EvaluationExpression recursivelyCreate(List<String> steps, INode node, int index, IValueGenerator iValueGenerator, Map<List<String>, EvaluationExpression> expressions) {
		final EvaluationExpression result;
		if (node instanceof SetNode) {
			ArrayCreation arrayCreation = new ArrayCreation();
			arrayCreation.add(recursivelyCreate(steps, node.getChild(steps.get(index + 1)), index + 1, iValueGenerator, expressions));
			result = arrayCreation;
		} else if (node instanceof TupleNode) {
			ObjectCreation objectCreation = new ObjectCreation();
			objectCreation.addMapping(steps.get(index + 1),
				recursivelyCreate(steps, node.getChild(steps.get(index + 1)), index + 1, iValueGenerator, expressions));
			result = objectCreation;
		} else if (node instanceof AttributeNode) {
			if (iValueGenerator instanceof FunctionGenerator) {
				final Expression function = ((FunctionGenerator) iValueGenerator).getFunction();
				final Node topNode = function.getJepExpression().getTopNode();
				result = processJepFunctionNode(topNode, function.getAttributePaths());
			} else if (iValueGenerator instanceof SkolemFunctionGenerator)
				result = ConstantExpression.NULL;
			else if (iValueGenerator instanceof NullValueGenerator)
				result = ConstantExpression.NULL;
			else
				throw new IllegalStateException("Unknown generator " + iValueGenerator);
		} else
			throw new IllegalStateException("Unknown node type " + node);
		expressions.put(steps.subList(0, index+1), result);
		return result;
	}

	public ArrayCreation createArrayFromSpicyPaths(final List<VariableCorrespondence> correspondences) {
		final Map<String, List<TargetAttributeCreation>> map = new HashMap<String, List<TargetAttributeCreation>>();
		for (final VariableCorrespondence varCor : correspondences) {
			final List<String> targetPathList =
				EntityMappingUtil.getRelevantPathStepsWithoutInput(varCor.getTargetPath()); // split target and add
																							// first as key to map e.g.
																							// [v3,fullname,name]
			final Expression function = varCor.getTransformationFunction(); // includes source paths e.g.
																			// [v4.usCongressBiographies.usCongressBiography.worksFor]
			this.addTargetAttributeCreationToMap(map, targetPathList, new FunctionGenerator(function));
		}
		final ObjectCreation tempObject = this.createdNestedObject(map);

		final List<EvaluationExpression> tempElements = new ArrayList<EvaluationExpression>();

		for (final Mapping<?> mapping : tempObject.getMappings()) {
			final int index = Integer.valueOf(mapping.getTarget().toString());
			CollectionUtil.ensureSize(tempElements, index + 1, ConstantExpression.MISSING);
			tempElements.set(index, mapping.getExpression());
		}
		return new ArrayCreation(tempElements);
	}

	public ObjectCreation createdNestedObject(final Map<String, List<TargetAttributeCreation>> map) {
		final ObjectCreation object = new ObjectCreation();
		for (final Entry<String, List<TargetAttributeCreation>> entry : map.entrySet()) {
			final List<TargetAttributeCreation> tacList = entry.getValue();
			if (tacList.size() == 1 && tacList.get(0).getTargetPath().isEmpty()) {
				// reached leaf, calculate value
				final TargetAttributeCreation tac = tacList.get(0);
				EvaluationExpression sopremoSourcePath = null;
				if (tac.getValueGen() instanceof FunctionGenerator) {
					final Expression function = ((FunctionGenerator) tac.getValueGen()).getFunction();
					final Node topNode = function.getJepExpression().getTopNode();
					sopremoSourcePath = this.processJepFunctionNode(topNode, function.getAttributePaths());
				} else if (tac.getValueGen() instanceof SkolemFunctionGenerator)
					// TODO
					// SkolemFunctionGenerator sourcePathSpicy = (SkolemFunctionGenerator) tac.getValueGen();
					sopremoSourcePath = ConstantExpression.NULL;
				else if (tac.getValueGen() instanceof NullValueGenerator)
					sopremoSourcePath = ConstantExpression.NULL;
				object.addMapping(entry.getKey(), sopremoSourcePath);
			} else {
				// reached multiple inner attributes, create map for inner level
				final Map<String, List<TargetAttributeCreation>> innerMap =
					new HashMap<String, List<TargetAttributeCreation>>();
				for (final TargetAttributeCreation tac : tacList)
					// add innerTAC to inner map
					this.addTargetAttributeCreationToMap(innerMap, tac.getTargetPath(), tac.getValueGen());
				object.addMapping(entry.getKey(), this.createdNestedObject(innerMap));
			}
		}
		return object;
	}

	private void addTargetAttributeCreationToMap(final Map<String, List<TargetAttributeCreation>> innerMap,
			final List<String> pathList, final IValueGenerator valueGen) {
		// create TAC
		final TargetAttributeCreation innerTAC = new TargetAttributeCreation();
		innerTAC.setValue(valueGen);
		final String innerKey = pathList.get(0);
		innerTAC.setTargetPath(pathList.subList(1, pathList.size()));
		// add to map
		if (innerMap.containsKey(innerKey)) {
			final List<TargetAttributeCreation> list = innerMap.get(innerKey);
			list.add(innerTAC);
		} else {
			final List<TargetAttributeCreation> list = new ArrayList<TargetAttributeCreation>();
			list.add(innerTAC);
			innerMap.put(innerKey, list);
		}
	}

	private static EvaluationExpression processJepFunctionNode(final Node topNode,
			final List<VariablePathExpression> sourcePaths) {
		if (topNode instanceof ASTVarNode)
			// function
			return createFunctionSourcePath(((ASTVarNode) topNode).getVarName(), sourcePaths);
		// TODO remove JepFunctionFactory?!
		else if (topNode instanceof FunctionNode) {
			final FunctionNode fnNode = (FunctionNode) topNode;
			return fnNode.getExpression();
		} else if (topNode instanceof ASTConstant)
			return new ConstantExpression(((ASTConstant) topNode).getValue());
		else if (topNode instanceof ASTFunNode)
			return JepFunctionFactory.create((ASTFunNode) topNode, sourcePaths);
		throw new IllegalStateException("Unknown node type");
	}

	private static PathSegmentExpression createFunctionSourcePath(final String pathFromFunction,
			final List<VariablePathExpression> sourcePaths) {
		// e.g. pathFromFunction = usCongress.usCongressBiographies.usCongressBiography.worksFor;
		final String[] pathFromFunctionSteps = pathFromFunction.split("\\.");

		// chose suitable sourcePath that matches pathFromFunction
		// e.g. sourcePath[0] = v1.usCongressBiography.worksFor;
		for (final VariablePathExpression exp : sourcePaths)
			if (exp.getLastStep().equals(pathFromFunctionSteps[pathFromFunctionSteps.length - 1]))
				return EntityMappingUtil.convertSpicyPath("0", exp);
		return null;
	}
}

/**
 * used in nested attributes: path to target and how target is calculated
 * 
 * @author Andrina
 */
class TargetAttributeCreation {
	List<String> targetPath;

	IValueGenerator value; // includes function (e.g. + or split) and source paths

	public List<String> getTargetPath() {
		return this.targetPath;
	}

	public void setTargetPath(final List<String> targetPath) {
		this.targetPath = targetPath;
	}

	public IValueGenerator getValueGen() {
		return this.value;
	}

	public void setValue(final IValueGenerator value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "TargetAttributeCreation: " + this.targetPath + " = " + this.value.toString();
	}
}