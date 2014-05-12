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
import static eu.stratosphere.sopremo.pact.SopremoUtil.*;
import it.unibas.spicy.model.algebra.query.operators.xquery.XQSkolemHandler;
import it.unibas.spicy.model.algebra.query.operators.xquery.XQUtility;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;
import it.unibas.spicy.model.datasource.nodes.SequenceNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.datasource.nodes.TupleNode;
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
import it.unibas.spicy.model.mapping.IDataSourceProxy;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.model.paths.SetAlias;
import it.unibas.spicy.model.paths.VariableCorrespondence;
import it.unibas.spicy.model.paths.VariablePathExpression;
import it.unibas.spicy.model.paths.operators.GeneratePathExpression;
import it.unibas.spicy.utility.SpicyEngineUtility;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
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
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.FieldAssignment;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
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
				if (strings.size() == 1)
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
					// return generateLocalSkolemFunction(generator, mappingTask);
				} else {
					throw new UnsupportedOperationException();
					// return generateHyperGraphSkolemFunction(generator, mappingTask);
				}
			} else if (generator.getType() == SkolemFunctionGenerator.KEY) {
				throw new UnsupportedOperationException();
				// return generateSkolemFunctionForKey(generator, mappingTask);
			} else if (generator.getType() == SkolemFunctionGenerator.EGD_BASED) {
				throw new UnsupportedOperationException();
				// return generateEGDSkolemFunction(generator, mappingTask);
			}
			throw new IllegalArgumentException("Incorrect type for leaf generator: " + generator + " - Type: " + generator.getType());
		}
		return generateSkolemFunctionForIntermediateNode(generator, mappingTask);
	}

	private static List<EvaluationExpression> generateSkolemFunctionForIntermediateNode(SkolemFunctionGenerator generator,
			MappingTask mappingTask) {
		List<EvaluationExpression> expressions = new ArrayList<EvaluationExpression>();
		for (GeneratorWithPath subGeneratorWithPath : generator.getSubGenerators()) {
			VariablePathExpression subGeneratorPath = subGeneratorWithPath.getTargetPath();
			expressions.add(new ObjectAccess(nameForPath(subGeneratorPath)));
		}
		return expressions;
	}

	public static String nameForPath(VariablePathExpression attributePath) {
		return attributePath.toString();
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

	private static ThreadLocal<SopremoPathToSpicyPath> SopremoPathToSpicyPath = new ThreadLocal<SpicyUtil.SopremoPathToSpicyPath>() {
		@Override
		protected SopremoPathToSpicyPath initialValue() {
			return new SopremoPathToSpicyPath();
		};
	};

	public static PathExpression toSpicy(EvaluationExpression targetPath) {
		return new PathExpression(new ArrayList<String>(SopremoPathToSpicyPath.get().getSteps(targetPath)));
	}

	public static PathExpression toSpicy(EvaluationExpression targetPath, PathExpression rootExpression) {
		ArrayList<String> steps = new ArrayList<String>(rootExpression.getPathSteps());
		steps.addAll(SopremoPathToSpicyPath.get().getSteps(targetPath));
		return new PathExpression(steps);
	}

	private static class SopremoPathToSpicyPath extends TreeHandler<EvaluationExpression> {
		/**
		 * 
		 */
		private static final String ARRAY_ELEMENT = "element";

		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 */
		public SopremoPathToSpicyPath() {
			put(ObjectAccess.class, new NodeHandler<ObjectAccess>() {
				@Override
				public void handle(ObjectAccess value, TreeHandler<Object> treeHandler) {
					treeHandler.handle(value.getInputExpression());
					SopremoPathToSpicyPath.this.steps.add(value.getField());
				}
			});
			put(InputSelection.class, new NodeHandler<InputSelection>() {
				@Override
				public void handle(InputSelection value, TreeHandler<Object> treeHandler) {
					SopremoPathToSpicyPath.this.steps.add(String.valueOf(value.getIndex()));
					SopremoPathToSpicyPath.this.steps.add(String.valueOf(ARRAY_ELEMENT));
				}
			});
		}

		private List<String> steps = new ArrayList<String>();

		@SuppressWarnings("unchecked")
		public List<String> getSteps(EvaluationExpression value) {
			this.steps.clear();
			handle(value);
			return this.steps;
		}
	}

	public static INode toSpicySchema(List<EvaluationExpression> schema, String rootLabel) {
		SequenceNode root = new SequenceNode(rootLabel);
		root.setRoot(true);
		for (int index = 0; index < schema.size(); index++) {
			SetNode dataset = new SetNode(String.valueOf(index));
			root.addChild(dataset);

			dataset.addChild(SopremoSchemaToSpicySchema.get().convert(schema.get(index)));
		}
		return root;
	}

	private static ThreadLocal<SopremoSchemaToSpicySchema> SopremoSchemaToSpicySchema = new ThreadLocal<SopremoSchemaToSpicySchema>() {
		@Override
		protected SopremoSchemaToSpicySchema initialValue() {
			return new SopremoSchemaToSpicySchema();
		};
	};

	private static class SopremoSchemaToSpicySchema extends TreeHandler<EvaluationExpression> {
		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 */
		public SopremoSchemaToSpicySchema() {
			put(ObjectCreation.class, new NodeHandler<ObjectCreation>() {
				@Override
				public void handle(ObjectCreation value, TreeHandler<Object> treeHandler) {
					SequenceNode sequenceNode = new SequenceNode(SopremoSchemaToSpicySchema.this.childName);
					for (Mapping<?> mapping : value.getMappings()) {
						SopremoSchemaToSpicySchema.this.childName = cast(mapping, FieldAssignment.class, "").getTarget();
						treeHandler.handle(mapping.getExpression());
						AttributeNode attributeNode = new AttributeNode(SopremoSchemaToSpicySchema.this.childName);
						attributeNode.addChild(SopremoSchemaToSpicySchema.this.lastNode);
						sequenceNode.addChild(attributeNode);
					}
					SopremoSchemaToSpicySchema.this.lastNode = sequenceNode;
				}
			});
			put(ArrayCreation.class, new NodeHandler<ArrayCreation>() {
				@Override
				public void handle(ArrayCreation value, TreeHandler<Object> treeHandler) {
					SetNode setNode = new SetNode(SopremoSchemaToSpicySchema.this.childName);
					List<EvaluationExpression> elements = value.getElements();
					for (int index = 0; index < elements.size(); index++) {
						SopremoSchemaToSpicySchema.this.childName = String.valueOf(index);
						treeHandler.handle(elements.get(index));
						setNode.addChild(SopremoSchemaToSpicySchema.this.lastNode);
					}
					SopremoSchemaToSpicySchema.this.lastNode = setNode;
				}
			});
			put(EvaluationExpression.ValueExpression.class, new NodeHandler<EvaluationExpression.ValueExpression>() {
				@Override
				public void handle(EvaluationExpression.ValueExpression value, TreeHandler<Object> treeHandler) {
					SopremoSchemaToSpicySchema.this.lastNode = new LeafNode("string");
				}
			});
		}

		private INode lastNode;

		private String childName;

		public INode convert(EvaluationExpression schema) {
			this.childName = "element";
			this.lastNode = new LeafNode("string");
			handle(schema);
			return this.lastNode;
		}
	}

	public static EvaluationExpression spicyToSopremo(INode schema, IDataSourceProxy dataSource) {
		return SpicySchemaToSopremo.get().convert(schema, dataSource);
	}

	private static ThreadLocal<SpicySchemaToSopremo> SpicySchemaToSopremo = new ThreadLocal<SpicySchemaToSopremo>() {
		@Override
		protected SpicySchemaToSopremo initialValue() {
			return new SpicySchemaToSopremo();
		};
	};

	private static class SpicySchemaToSopremo extends TreeHandler<INode> {
		/**
		 * Initializes SpicyUtil.SopremoPathToSpicyPath.
		 */
		public SpicySchemaToSopremo() {
			put(SequenceNode.class, new NodeHandler<SequenceNode>() {
				@Override
				public void handle(SequenceNode value, TreeHandler<Object> treeHandler) {
					SpicySchemaToSopremo.this.pathSteps.add(value.getLabel());
					ObjectCreation oc = new ObjectCreation();
					for (INode child : value.getChildren()) {
						treeHandler.handle(child);
						oc.addMapping(child.getLabel(), SpicySchemaToSopremo.this.lastExpr);
					}
					SpicySchemaToSopremo.this.lastExpr = oc;
					SpicySchemaToSopremo.this.pathSteps.remove(SpicySchemaToSopremo.this.pathSteps.size() - 1);
				}
			});
			put(SetNode.class, new NodeHandler<SetNode>() {
				@Override
				public void handle(SetNode value, TreeHandler<Object> treeHandler) {
					SpicySchemaToSopremo.this.pathSteps.add(value.getLabel());
					ArrayCreation ac = new ArrayCreation();
					List<INode> elements = value.getChildren();
					for (int index = 0; index < elements.size(); index++) {
						treeHandler.handle(elements.get(index));
						ac.add(SpicySchemaToSopremo.this.lastExpr);
					}
					SpicySchemaToSopremo.this.lastExpr = ac;
					SpicySchemaToSopremo.this.pathSteps.remove(SpicySchemaToSopremo.this.pathSteps.size() - 1);
				}
			});
			put(AttributeNode.class, new NodeHandler<AttributeNode>() {
				@Override
				public void handle(AttributeNode value, TreeHandler<Object> treeHandler) {
					SpicySchemaToSopremo.this.pathSteps.add(value.getLabel());
					treeHandler.handle(value.getChild(0));
					SpicySchemaToSopremo.this.pathSteps.remove(SpicySchemaToSopremo.this.pathSteps.size() - 1);
				}
			});
			put(LeafNode.class, new NodeHandler<LeafNode>() {
				@Override
				public void handle(LeafNode value, TreeHandler<Object> treeHandler) {
					SpicySchemaToSopremo.this.lastExpr = new ObjectAccess(nameForPath(SpicySchemaToSopremo.this.gpe.generateRelativePath(SpicySchemaToSopremo.this.path, SpicySchemaToSopremo.this.dataSource))).withInputExpression(new ObjectAccess("content"));
				}
			});
		}

		private EvaluationExpression lastExpr;

		private IDataSourceProxy dataSource;

		private List<String> pathSteps;

		private PathExpression path;
		
		private GeneratePathExpression gpe = new GeneratePathExpression();

		public EvaluationExpression convert(INode schema, IDataSourceProxy dataSource) {
			this.dataSource = dataSource;
			this.path = this.gpe.generatePathFromRoot(schema.getFather());
			this.pathSteps = this.path.getPathSteps();
			handle(schema);
			return this.lastExpr;
		}
	}
}
