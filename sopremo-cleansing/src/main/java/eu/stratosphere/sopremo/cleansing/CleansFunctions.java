package eu.stratosphere.sopremo.cleansing;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.cleansing.blocking.SoundEx;
import eu.stratosphere.sopremo.cleansing.fusion.BeliefResolution;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityExpression;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityFactory;
import eu.stratosphere.sopremo.cleansing.similarity.set.JaccardSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroWinklerSimilarity;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.MethodPointerExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.packages.ConstantRegistryCallback;
import eu.stratosphere.sopremo.packages.FunctionRegistryCallback;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.TemporaryVariableFactory;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

public class CleansFunctions implements BuiltinProvider, ConstantRegistryCallback, FunctionRegistryCallback {
	private static AtomicLong Id = new AtomicLong();

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.packages.ConstantRegistryCallback#registerConstants(eu.stratosphere.sopremo.packages.
	 * IConstantRegistry)
	 */
	@Override
	public void registerConstants(IConstantRegistry constantRegistry) {
		constantRegistry.put("required", new NonNullRule());
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.packages.FunctionRegistryCallback#registerFunctions(eu.stratosphere.sopremo.packages.
	 * IFunctionRegistry)
	 */
	@Override
	public void registerFunctions(IFunctionRegistry registry) {
		registry.put("jaccard", new SimilarityMacro(new JaccardSimilarity()));
		registry.put("jaroWinkler", new SimilarityMacro(new JaroWinklerSimilarity()));
		registry.put("vote", new VoteMacro());
	}

	public static void generateId(TextNode resultId, TextNode prefix) {
		resultId.clear();
		resultId.append(prefix);
		resultId.append(Id.incrementAndGet());
	}
	
	public static void soundex(TextNode soundex, TextNode input) {
		soundex.clear();
		try {
			SoundEx.generateSoundExInto(input, soundex);
		} catch (IOException e) {
		}
	}

	public static void removeVowels(TextNode result, TextNode node) {
		CoreFunctions.replace(result, node, TextNode.valueOf("(?i)[aeiou]"), TextNode.valueOf(""));
	}

	public static void longest(ArrayNode result, IArrayNode values) {
		result.copyValueFrom(values);
		if (values.size() > 2) {
			int longestLength = 0;
			TextNode text = TemporaryVariableFactory.INSTANCE.alllocateVariable(TextNode.class);
			for (IJsonNode value : values) {
				text = TypeCoercer.INSTANCE.coerce(value, text, TextNode.class);
				longestLength = Math.max(longestLength, text.getJavaValue().length());
			}
			for (int index = 0; index < result.size(); index++) {
				text = TypeCoercer.INSTANCE.coerce(result.get(index), text, TextNode.class);
				if (text.getJavaValue().length() < longestLength)
					result.remove(index);
			}

			TemporaryVariableFactory.INSTANCE.free(text);
		}
	}

	public static void first(ArrayNode result, IArrayNode values) {
		result.clear();
		if (!values.isEmpty())
			result.add(values.get(0));
	}

	/**
	 * @author Arvid Heise
	 */
	private static class SimilarityMacro extends MacroBase {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2086644180411129824L;

		private final Similarity<?> similarity;

		/**
		 * Initializes SimmetricMacro.
		 * 
		 * @param similarity
		 */
		public SimilarityMacro(Similarity<?> similarity) {
			this.similarity = similarity;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object, java.lang.Object,
		 * eu.stratosphere.sopremo.EvaluationContext)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public EvaluationExpression call(EvaluationExpression[] params, EvaluationExpression target,
				EvaluationContext context) {
			Similarity<IJsonNode> similarity;
			if (params.length > 1)
				similarity =
					(Similarity<IJsonNode>) SimilarityFactory.INSTANCE.create(this.similarity, params[0], params[1],
						true);
			else
				similarity =
					(Similarity<IJsonNode>) SimilarityFactory.INSTANCE.create(this.similarity, params[0], params[0],
						true);
			return new SimilarityExpression(similarity);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
		 */
		@Override
		public void toString(StringBuilder builder) {
			this.similarity.toString(builder);
		}
	}

	private static class VoteMacro extends MacroBase {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2673091978964835311L;

		/**
		 * Initializes CleansFunctions.VoteMacro.
		 */
		public VoteMacro() {
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object, java.lang.Object,
		 * eu.stratosphere.sopremo.EvaluationContext)
		 */
		@Override
		public EvaluationExpression call(EvaluationExpression[] params, EvaluationExpression target,
				EvaluationContext context) {
			for (int index = 0; index < params.length; index++)
				if (params[index] instanceof MethodPointerExpression) {
					final String functionName = ((MethodPointerExpression) params[index]).getFunctionName();
					params[index] = new FunctionCall(functionName, context, EvaluationExpression.VALUE);
				}
			return new BeliefResolution(params);
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#toString(java.lang.StringBuilder)
		 */
		@Override
		public void toString(StringBuilder builder) {
			builder.append("vote");
		}
	}
}
