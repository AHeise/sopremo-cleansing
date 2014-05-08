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

import it.unibas.spicy.model.paths.VariablePathExpression;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.nfunk.jep.ASTConstant;
import org.nfunk.jep.ASTFunNode;
import org.nfunk.jep.ASTVarNode;
import org.nfunk.jep.Node;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.aggregation.AggregationFunction;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * @author Arvid Heise, Tommy Neubert
 */
public class JepFunctionFactory {

	private static Map<String, JepFunctionHandler> functions = new HashMap<String, JepFunctionHandler>() {

		private static final long serialVersionUID = 1L;

		{
			this.put("+", new ConcatJepFunctionHandler());
			this.put("substring", new SubstringJepFunctionHandler());
			this.put("sum", new SumJepFunctionHandler());
		}
	};

	public static FunctionCall create(final ASTFunNode topNode, final List<VariablePathExpression> sourcePaths) {
		final String functionName = topNode.getName();
		if (functions.containsKey(functionName)) {
			final JepFunctionHandler handler = functions.get(functionName);
			return handler.create(topNode, sourcePaths);
		} else
			throw new IllegalArgumentException("No such jep-function found: "
				+ functionName);
	}

	public static abstract class JepFunctionHandler {

		private final SopremoFunction function;

		public JepFunctionHandler(final SopremoFunction function) {
			this.function = function;
		}

		public FunctionCall create(final ASTFunNode topNode, final List<VariablePathExpression> sourcePaths) {
			return new FunctionCall(this.function, this.handleInputs(topNode, sourcePaths));
		}

		protected EvaluationExpression processJepFunctionNode(final Node topNode,
				final List<VariablePathExpression> sourcePaths) {
			if (topNode instanceof ASTVarNode)
				// function
				return this.createFunctionSourcePath(
					((ASTVarNode) topNode).getVarName(), sourcePaths);
			else if (topNode instanceof ASTFunNode)
				return JepFunctionFactory.create((ASTFunNode) topNode,
					sourcePaths);
			else if (topNode instanceof ASTConstant)
				return new ConstantExpression(
					((ASTConstant) topNode).getValue());
			return null;
		}

		private PathSegmentExpression createFunctionSourcePath(
				final String pathFromFunction,
				final List<VariablePathExpression> sourcePaths) {
			// path =
			// usCongress.usCongressBiographies.usCongressBiography.worksFor;
			final String[] pathFromFunctionSteps = pathFromFunction.split("\\.");
			// String[] pathFromFunctionStepsTrunc =
			// Arrays.copyOfRange(pathFromFunctionSteps, 2,
			// pathFromFunctionSteps.length);

			// chose suitable sourcePath that matches pathFromFunction
			// sourcePath[0] = v1.usCongressBiography.worksFor;
			for (final VariablePathExpression exp : sourcePaths)
				if (exp.getLastStep()
					.equals(pathFromFunctionSteps[pathFromFunctionSteps.length - 1])) // TODO
																						// check!!!
					return EntityMappingUtil.convertSpicyPath("0", exp);
			return null;
		}

		protected abstract List<EvaluationExpression> handleInputs(
				ASTFunNode topNode, List<VariablePathExpression> sourcePaths);
	}

	private static class ConcatJepFunctionHandler extends JepFunctionHandler {

		public ConcatJepFunctionHandler() {
			super(new AggregationFunction(CoreFunctions.CONCAT));
		}

		@Override
		protected List<EvaluationExpression> handleInputs(final ASTFunNode topNode,
				final List<VariablePathExpression> sourcePaths) {

			final ArrayCreation input = new ArrayCreation(); // (sourcePath, new
			// ConstantExpression("---"));
			for (int childI = 0; childI < topNode.jjtGetNumChildren(); childI++) {
				final Node child = topNode.jjtGetChild(childI);
				input.add(this.processJepFunctionNode(child, sourcePaths));
			}

			final List<EvaluationExpression> inputList = new LinkedList<EvaluationExpression>();
			inputList.add(input);

			return inputList;
		}

	}

	private static class SubstringJepFunctionHandler extends JepFunctionHandler {

		public SubstringJepFunctionHandler() {
			super(CoreFunctions.SUBSTRING);
		}

		@Override
		protected List<EvaluationExpression> handleInputs(final ASTFunNode topNode,
				final List<VariablePathExpression> sourcePaths) {

			final List<EvaluationExpression> inputList = new LinkedList<EvaluationExpression>();

			for (int childI = 0; childI < topNode.jjtGetNumChildren(); childI++) {
				final Node child = topNode.jjtGetChild(childI);
				final EvaluationExpression expr = this.processJepFunctionNode(child,
					sourcePaths);

				inputList.add(childI != 0 ? new CoerceExpression(
					IntNode.class).withInputExpression(expr) : expr);
			}

			inputList.add(1, new ConstantExpression(IntNode.ZERO));

			return inputList;
		}
	}

	private static class SumJepFunctionHandler extends JepFunctionHandler {

		public SumJepFunctionHandler() {
			super(new AggregationFunction(CoreFunctions.SUM));
		}

		@Override
		protected List<EvaluationExpression> handleInputs(final ASTFunNode topNode,
				final List<VariablePathExpression> sourcePaths) {

			final List<EvaluationExpression> inputList = new LinkedList<EvaluationExpression>();

			for (int childI = 0; childI < topNode.jjtGetNumChildren(); childI++) {
				final Node child = topNode.jjtGetChild(childI);
				inputList.add(this.processJepFunctionNode(child, sourcePaths));
			}

			return inputList;
		}
	}
}
