/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
import it.unibas.spicy.model.algebra.query.operators.xquery.XQSkolemHandler;
import it.unibas.spicy.model.algebra.query.operators.xquery.XQUtility;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.generators.AppendSkolemPart;
import it.unibas.spicy.model.generators.FunctionGenerator;
import it.unibas.spicy.model.generators.GeneratorWithPath;
import it.unibas.spicy.model.generators.ISkolemPart;
import it.unibas.spicy.model.generators.IValueGenerator;
import it.unibas.spicy.model.generators.NullValueGenerator;
import it.unibas.spicy.model.generators.SkolemFunctionGenerator;
import it.unibas.spicy.model.generators.StringSkolemPart;
import it.unibas.spicy.model.mapping.FORule;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.model.paths.SetAlias;
import it.unibas.spicy.model.paths.VariableCorrespondence;
import it.unibas.spicy.model.paths.VariablePathExpression;
import it.unibas.spicy.utility.SpicyEngineUtility;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.nfunk.jep.ASTConstant;
import org.nfunk.jep.ASTFunNode;
import org.nfunk.jep.JEP;
import org.nfunk.jep.Node;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.cleansing.mapping.SpicyUtil.InputManager;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.operator.JsonStream;

/**
 * @author arvid
 */
public class SpicyUtil {

	public static EvaluationExpression fromExpression(Expression condition) {
		final JEP jepExpression = condition.getJepExpression();
		final Node topNode = jepExpression.getTopNode();
		if (topNode instanceof FunctionNode) {
			final FunctionNode fnNode = (FunctionNode) topNode;
			return fnNode.getExpression();
		} else if (topNode instanceof ASTConstant)
			return new ConstantExpression(((ASTConstant) topNode).getValue());
		else if (topNode instanceof ASTFunNode)
			return JepFunctionFactory.create((ASTFunNode) topNode, condition.getAttributePaths());
		throw new IllegalStateException("Unknown node type");
	}

	public static EvaluationExpression xqueryValueForIntermediateNode(IValueGenerator generator, MappingTask mappingTask,
			InputManager inputManager) {

		return xqueryValueForLeaf(generator, null, null, mappingTask, inputManager);
	}

	public static EvaluationExpression xqueryValueForLeaf(IValueGenerator generator, VariablePathExpression targetPath, FORule tgd,
			MappingTask mappingTask, InputManager inputManager) {
		if (generator instanceof NullValueGenerator)
			return ConstantExpression.NULL;
		if (generator instanceof FunctionGenerator) {
			VariableCorrespondence correspondence =
				XQUtility.findCorrespondenceFromTargetPathWithSameId(targetPath, tgd.getCoveredCorrespondences());
			if (correspondence.getSourcePaths() != null) {
				VariablePathExpression firstSourcePath = correspondence.getFirstSourcePath();
				return new ObjectAccess(nameForPath(firstSourcePath)).withInputExpression(new InputSelection(0));
			}
			return new ConstantExpression(correspondence.getSourceValue().toString());
		}
		if (generator instanceof SkolemFunctionGenerator) {
			SkolemFunctionGenerator skolemGenerator = (SkolemFunctionGenerator) generator;
			if (generator.getSubGenerators().size() > 0) {
				List<EvaluationExpression> strings = skolemString(skolemGenerator, mappingTask);
				if(strings.size() == 1)
					return strings.get(0);
				return FunctionUtil.createFunctionCall(CoreFunctions.CONCAT, new ArrayCreation(strings));
			}
			return new ConstantExpression(skolemGenerator.getName());
			// SkolemFunctionGenerator skolemGenerator = (SkolemFunctionGenerator) generator;
			// StringBuilder result = new StringBuilder();
			//
			// result.append(XQUtility.XQUERY_FUNCTION).append("(\"").append(removeRootLabel(skolemGenerator.getName())).append("\", (");
			// if (generator.getSubGenerators().size() > 0) {
			// String skolemString = new XQSkolemHandler().skolemString(skolemGenerator, mappingTask, false).toString();
			// result.append(skolemString);
			// }
			// result.append("))");
			// return result.toString();
		}
		throw new UnsupportedOperationException();
	}
	
	public static List<EvaluationExpression> skolemString(SkolemFunctionGenerator generator, MappingTask mappingTask) {
        if (generator.isLeafGenerator()) {
            if (generator.getType() == SkolemFunctionGenerator.STANDARD) {
                if (mappingTask.getConfig().useLocalSkolem()) {
                	throw new UnsupportedOperationException();
//                    return generateLocalSkolemFunction(generator, mappingTask);
                } else {
                	throw new UnsupportedOperationException();
//                    return generateHyperGraphSkolemFunction(generator, mappingTask);
                }
            } else if (generator.getType() == SkolemFunctionGenerator.KEY) {
            	throw new UnsupportedOperationException();
//                return generateSkolemFunctionForKey(generator, mappingTask);
            } else if (generator.getType() == SkolemFunctionGenerator.EGD_BASED) {
            	throw new UnsupportedOperationException();
//                return generateEGDSkolemFunction(generator, mappingTask);
            }
            throw new IllegalArgumentException("Incorrect type for leaf generator: " + generator + " - Type: " + generator.getType());
        }
		return generateSkolemFunctionForIntermediateNode(generator, mappingTask);
    }
	
    private static List<EvaluationExpression> generateSkolemFunctionForIntermediateNode(SkolemFunctionGenerator generator, MappingTask mappingTask) {
        List<EvaluationExpression> expressions = new ArrayList<EvaluationExpression>();
		for (GeneratorWithPath subGeneratorWithPath : generator.getSubGenerators()) {
		    VariablePathExpression subGeneratorPath = subGeneratorWithPath.getTargetPath();
		    expressions.add(new ObjectAccess(nameForPath(subGeneratorPath)));
		}
		return expressions;
    }
    
	public static String nameForPath(VariablePathExpression attributePath) {
		StringBuilder result = new StringBuilder();
		result.append(attributePath.getStartingVariable().toShortString()).append("_").append(attributePath.getLastStep());
		return result.toString();
	}

	public static EvaluationExpression createRelativePathForMetadataNode(VariablePathExpression path, INode metatadaNode,
			InputManager var2Source) {
		throw new UnsupportedOperationException();
		// StringBuilder relativePath = new StringBuilder();
		// INode father = metatadaNode.getFather();
		// INode ancestor = father.getFather();
		// if (ancestor instanceof SetNode) {
		// relativePath.append("xs:string(").append(path.getStartingVariable().toShortStringWithDollar()).append("/@").append(path.getLastStep()).append(")");
		// } else {
		// relativePath.append("xs:string(").append(path.getStartingVariable().toShortStringWithDollar()).append("/");
		// relativePath.append(father.getLabel()).append("/@").append(path.getLastStep()).append(")");
		// }
		// return relativePath.toString();
	}

	public static EvaluationExpression createRelativePathForVirtualAttribute(VariablePathExpression path, INode attributeNode,
			InputManager var2Source) {
		INode father = attributeNode.getFather();
		INode ancestor = father.getFather();
		INode ancestorOfAncestor = ancestor.getFather();
		if (father.isVirtual()) {
			if (ancestorOfAncestor instanceof SetNode) {
				return var2Source.getInput(path.getStartingVariable());
			}
			return new ObjectAccess(ancestor.getLabel()).withInputExpression(var2Source.getInput(path.getStartingVariable()));
		}
		return new ObjectAccess(father.getLabel()).withInputExpression(var2Source.getInput(path.getStartingVariable()));
	}

	public static EvaluationExpression createRelativePathForSingleAttribute(VariablePathExpression path, INode attributeNode,
			InputManager var2Source) {
		return var2Source.getInput(path.getLastStep());
	}

	//
	// public static EvaluationExpression createRelativePathForCoverage(VariablePathExpression path, Coverage coverage)
	// {
	// StringBuilder relativePath = new StringBuilder();
	// FORule tgd = findCoveringTGD(path, coverage);
	// String variableName = XQNames.NameForApplyFunctions(tgd);
	// relativePath.append(variableName).append("/").append(path.getStartingVariable().toShortString()).append("_").append(path.getLastStep()).append("/text()");
	// return relativePath.toString();
	// }
	//
	public static EvaluationExpression createRelativePath(VariablePathExpression path, InputManager var2Source) {
		return new ObjectAccess(path.getLastStep()).withInputExpression(var2Source.getInput(path.getStartingVariable()));
	}

	public static class StreamManager {
		private Map<String, JsonStream> streams = new HashMap<String, JsonStream>();

		public JsonStream getStream(SetAlias alias) {
			return this.getStream(alias.toShortString());
		}

		public JsonStream getStream(String alias) {
			return this.streams.get(alias);
		}

		public List<JsonStream> getStreams(List<SetAlias> alias) {
			ArrayList<JsonStream> streams = new ArrayList<JsonStream>();
			for (SetAlias setAlias : alias)
				streams.add(getStream(setAlias));
			return streams;
		}

		public void put(String name, JsonStream stream) {
			this.streams.put(name, stream);
		}
	}

	public static class InputManager {
		private Object2IntMap<String> aliasesIndex = new Object2IntOpenHashMap<String>();

		public InputManager(List<SetAlias> alias) {
			for (int index = 0; index < alias.size(); index++)
				this.aliasesIndex.put(alias.get(index).toShortString(), index);
		}

		public InputManager(String... alias) {
			for (int index = 0; index < alias.length; index++)
				this.aliasesIndex.put(alias[index], index);
		}

		{
			this.aliasesIndex.defaultReturnValue(-1);
		}

		public EvaluationExpression getInput(String id) {
			return new InputSelection(getPos(id));
		}

		private int getPos(String id) {
			int pos = this.aliasesIndex.getInt(id);
			if (pos == -1)
				throw new IllegalArgumentException("Unknown variable");
			return pos;
		}

		public EvaluationExpression getInput(SetAlias startingVariable) {
			return getInput(startingVariable.toShortString());
		}
	}

	private static ThreadLocal<SopremoPathToSpicyPath> SopremoPathToSpicyPath = new ThreadLocal<SpicyUtil.SopremoPathToSpicyPath>(){
		@Override
		protected SopremoPathToSpicyPath initialValue() {
			return new SopremoPathToSpicyPath();
		};
	};
	
	public static PathExpression toSpicy(EvaluationExpression targetPath) {
		return SopremoPathToSpicyPath.get().convert(targetPath);
	}
	
	private static class SopremoPathToSpicyPath extends TreeHandler<EvaluationExpression> {
		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 *
		 */
		public SopremoPathToSpicyPath() {
			put(ObjectAccess.class, new NodeHandler<ObjectAccess>() {
				@Override
				public void handle(ObjectAccess value, TreeHandler<Object> treeHandler) {
					SopremoPathToSpicyPath.this.steps.add(value.getField());
					treeHandler.handle(value.getInputExpression());
				}
			});
		}
		
		private ArrayList<String> steps = new ArrayList<String>();
		@SuppressWarnings("unchecked")
		public PathExpression convert(EvaluationExpression value) {
			this.steps.clear();			
			handle(value);
			return new PathExpression((List<String>) this.steps.clone());
		}
	}
}
