/*
    Copyright (C) 2007-2011  Database Group - Universita' della Basilicata
    Giansalvatore Mecca - giansalvatore.mecca@unibas.it
    Salvatore Raunich - salrau@gmail.com

    This file is part of ++Spicy - a Schema Mapping and Data Exchange Tool
    
    ++Spicy is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.

    ++Spicy is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with ++Spicy.  If not, see <http://www.gnu.org/licenses/>.
 */

package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.*;
import it.unibas.spicy.model.datasource.operators.INodeVisitor;
import it.unibas.spicy.model.generators.*;
import it.unibas.spicy.model.generators.operators.GenerateSkolemGenerators;
import it.unibas.spicy.model.mapping.FORule;
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.*;
import it.unibas.spicy.model.paths.operators.GeneratePathExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateValueGenerators {
	private GenerateSkolemGenerators skolemGeneratorFinder = new GenerateSkolemGenerators();

	public TGDGeneratorsMap generateValueGenerators(FORule tgd, MappingTask mappingTask) {
		Map<String, IValueGenerator> attributeGenerators = findAttributeGenerators(tgd, mappingTask);
		TGDGeneratorsMap generatorsMap = new TGDGeneratorsMap();
		for (SetAlias targetVariable : tgd.getTargetView().getVariables()) {
			PropagateGeneratorsNodeVisitor visitor =
				new PropagateGeneratorsNodeVisitor(tgd, targetVariable, attributeGenerators, mappingTask.getTargetProxy());
			mappingTask.getTargetProxy().getIntermediateSchema().accept(visitor);
			Map<PathExpression, IValueGenerator> variableGenerators = visitor.getResult();
			generatorsMap.addGeneratorsForVariable(targetVariable, variableGenerators);
		}
		initializeSkolemStrings(generatorsMap, mappingTask);
		return generatorsMap;
	}

	private Map<String, IValueGenerator> findAttributeGenerators(FORule tgd, MappingTask mappingTask) {
		Map<String, IValueGenerator> attributeGenerators = new HashMap<String, IValueGenerator>();
		for (VariableCorrespondence correspondence : tgd.getCoveredCorrespondences()) {
			VariablePathExpression targetPath = correspondence.getTargetPath();
			FunctionGenerator generator = new FunctionGenerator(correspondence.getTransformationFunction());
			attributeGenerators.put(targetPath.toString(), generator);
		}
		List<GeneratorWithPath> functionGeneratorsForSkolemFunctions = findGeneratorsForVariablesInJoin(tgd, attributeGenerators);
		this.skolemGeneratorFinder.findGeneratorsForSkolems(tgd, mappingTask, attributeGenerators, functionGeneratorsForSkolemFunctions);
		return attributeGenerators;
	}

	// NOTE: this method is useless if TGDs are normalized
	private List<GeneratorWithPath> findGeneratorsForVariablesInJoin(FORule tgd, Map<String, IValueGenerator> generators) {
		List<GeneratorWithPath> result = new ArrayList<GeneratorWithPath>();
		List<SetAlias> joinVariables = findJoinVariables(tgd);
		for (SetAlias variable : joinVariables) {
			result.addAll(findGeneratorsForVariable(tgd, generators, variable));
		}
		return result;
	}

	private List<SetAlias> findJoinVariables(FORule tgd) {
		List<SetAlias> result = new ArrayList<SetAlias>();
		if (tgd.getTargetView().getVariables().size() == 1) {
			result.add(tgd.getTargetView().getVariables().get(0));
			return result;
		}
		for (VariableJoinCondition joinCondition : tgd.getTargetView().getAllJoinConditions()) {
			if (!containsVariableWithSameId(result, joinCondition.getFromVariable())) {
				result.add(joinCondition.getFromVariable());
			}
			if (!containsVariableWithSameId(result, joinCondition.getToVariable())) {
				result.add(joinCondition.getToVariable());
			}
		}
		return result;
	}

	private boolean containsVariableWithSameId(List<SetAlias> list, SetAlias variable) {
		for (SetAlias listVariable : list) {
			if (listVariable.getId() == variable.getId()) {
				return true;
			}
		}
		return false;
	}

	private List<GeneratorWithPath> findGeneratorsForVariable(FORule tgd, Map<String, IValueGenerator> generators, SetAlias variable) {
		List<GeneratorWithPath> result = new ArrayList<GeneratorWithPath>();
		for (VariableCorrespondence correspondence : tgd.getCoveredCorrespondences()) {
			if (correspondence.getTargetPath().getStartingVariable().getId() == variable.getId()) {
				VariablePathExpression targetPath = correspondence.getTargetPath();
				IValueGenerator generator = generators.get(targetPath.toString());
				GeneratorWithPath generatorWithPath = new GeneratorWithPath(targetPath, generator);
				result.add(generatorWithPath);
			}
		}
		return result;
	}

	private void initializeSkolemStrings(TGDGeneratorsMap generatorsMap, MappingTask mappingTask) {
		for (Map<PathExpression, IValueGenerator> map : generatorsMap.getMaps()) {
			for (IValueGenerator generator : map.values()) {
				if (generator instanceof SkolemFunctionGenerator) {
					SkolemFunctionGenerator skolemGenerator = (SkolemFunctionGenerator) generator;
					skolemGenerator.getSkolemFunction(mappingTask);
				}
			}
		}
	}
}

class PropagateGeneratorsNodeVisitor implements INodeVisitor {
	private FORule tgd;

	private SetAlias generatingVariable;

	private Map<String, IValueGenerator> attributeGenerators;

	private IDataSourceProxy target;

	private Map<PathExpression, IValueGenerator> result = new HashMap<PathExpression, IValueGenerator>();

	private GeneratePathExpression pathGenerator = new GeneratePathExpression();

	private List<GeneratorWithPath> leafGenerators;

	public PropagateGeneratorsNodeVisitor(FORule tgd, SetAlias generatingVariable, Map<String, IValueGenerator> attributeGenerators,
			IDataSourceProxy target) {
		this.tgd = tgd;
		this.generatingVariable = generatingVariable;
		this.attributeGenerators = attributeGenerators;
		this.target = target;
	}

	@Override
	public void visitSetNode(SetNode node) {
		PathExpression nodePath = this.pathGenerator.generatePathFromRoot(node);
		List<GeneratorWithPath> originalGenerators = this.leafGenerators;
		this.leafGenerators = findLeafGenerators(node);
		visitChildren(node);
		this.leafGenerators = originalGenerators;
		if (!isNodeToGenerate(node))
			this.result.put(nodePath, NullValueGenerator.getInstance());
		else if (node.isRoot()) {
			this.result.put(nodePath, new SkolemFunctionGenerator(nodePath.toString(), false));
		} else
			this.result.put(nodePath, new SkolemFunctionGenerator(nodePath.toString(), false, tgd, leafGenerators));
	}

	@Override
	public void visitTupleNode(TupleNode node) {
		PathExpression nodePath = this.pathGenerator.generatePathFromRoot(node);
		// node not to be generated: null value generators
		if (!isNodeToGenerate(node)) {
			visitChildren(node);
			this.result.put(nodePath, NullValueGenerator.getInstance());
			return;
		}
		// node to be generated: appropriate generators
		List<GeneratorWithPath> originalGenerators = this.leafGenerators;
		this.leafGenerators = findLeafGenerators(node);
		visitChildren(node);
		IValueGenerator generator = null;
		if (node.isRoot()) {
			generator = new SkolemFunctionGenerator(nodePath.toString(), false);
		} else {
			generator = new SkolemFunctionGenerator(nodePath.toString(), false, this.tgd, this.leafGenerators);
		}
		this.result.put(nodePath, generator);
		this.leafGenerators = originalGenerators;
	}

	private List<GeneratorWithPath> findLeafGenerators(INode node) {
		List<GeneratorWithPath> leafGenerators = new ArrayList<GeneratorWithPath>();
		for (INode child : node.getChildren()) {
			if (child instanceof AttributeNode) {
				GeneratorWithPath leafGeneratorWithPath = getLeafGenerator(child);
				if (leafGeneratorWithPath.getGenerator() instanceof NullValueGenerator) {
					continue;
				}
				leafGenerators.add(leafGeneratorWithPath);
			}
		}
		return leafGenerators;
	}

	private GeneratorWithPath getLeafGenerator(INode attributeNode) {
		PathExpression nodeAbsolutePath = this.pathGenerator.generatePathFromRoot(attributeNode);
		VariablePathExpression nodePath =
			this.pathGenerator.generateContextualPathForNode(this.generatingVariable, nodeAbsolutePath, this.target.getIntermediateSchema());
		IValueGenerator leafGenerator = this.attributeGenerators.get(nodePath.toString());
		if (leafGenerator == null) {
			leafGenerator = NullValueGenerator.getInstance();
		}
		GeneratorWithPath generatorWithPath = new GeneratorWithPath(nodePath, leafGenerator);
		return generatorWithPath;
	}

	@Override
	public void visitSequenceNode(SequenceNode node) {
		visitTupleNode(node);
	}

	@Override
	public void visitUnionNode(UnionNode node) {
		visitChildren(node);
	}

	public boolean isNodeToGenerate(INode node) {
		if (node.isRoot()) {
			return true;
		}
		VariablePathExpression relativePath = this.pathGenerator.generateRelativePath(node, this.target);
		if (relativePath != null) {
			SetAlias nodeVariable = relativePath.getStartingVariable();
			for (SetAlias generator : this.generatingVariable.getGenerators()) {
				if (nodeVariable.equalsUpToProvenance(generator)) {
					return true;
				}
			}
		}
		return false;
	}

	private void visitChildren(INode node) {
		for (INode child : node.getChildren()) {
			child.accept(this);
		}
	}

	@Override
	public void visitAttributeNode(AttributeNode node) {
		if (!isNodeToGenerate(node)) {
			PathExpression childPath = this.pathGenerator.generatePathFromRoot(node);
			this.result.put(childPath, NullValueGenerator.getInstance());
			return;
		}
		GeneratorWithPath leafGenerator = getLeafGenerator(node);
		PathExpression childPath = this.pathGenerator.generatePathFromRoot(node);
		IValueGenerator childGenerator = new SkolemFunctionGenerator(childPath.toString(), false, this.tgd, this.leafGenerators);
		this.result.put(childPath, childGenerator);
		INode leafNode = node.getChild(0);
		PathExpression leafPath = this.pathGenerator.generatePathFromRoot(leafNode);
		this.result.put(leafPath, leafGenerator.getGenerator());
	}

	@Override
	public void visitMetadataNode(MetadataNode node) {
		visitAttributeNode(node);
	}

	@Override
	public void visitLeafNode(LeafNode node) {
		return;
	}

	@Override
	public Map<PathExpression, IValueGenerator> getResult() {
		return this.result;
	}
}
