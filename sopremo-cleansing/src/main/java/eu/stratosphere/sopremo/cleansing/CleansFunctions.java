package eu.stratosphere.sopremo.cleansing;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.SopremoRuntime;
import eu.stratosphere.sopremo.aggregation.Aggregation;
import eu.stratosphere.sopremo.aggregation.TransitiveAggregation;
import eu.stratosphere.sopremo.cache.NodeCache;
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
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.packages.ConstantRegistryCallback;
import eu.stratosphere.sopremo.packages.FunctionRegistryCallback;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

@SuppressWarnings("serial")
public class CleansFunctions implements BuiltinProvider, ConstantRegistryCallback, FunctionRegistryCallback {

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.packages.ConstantRegistryCallback#registerConstants(eu.stratosphere.sopremo.packages.
	 * IConstantRegistry)
	 */
	@Override
	public void registerConstants(IConstantRegistry constantRegistry) {
//		constantRegistry.put("required", new NonNullRule());
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

	public static final SopremoFunction GENERATE_ID = new SopremoFunction1<TextNode>("generateId") {
		private final transient AtomicLong id = new AtomicLong();

		private final transient TextNode result = new TextNode();

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.function.SopremoFunction1#call(eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		protected TextNode call(TextNode prefix) {
			this.result.clear();
			this.result.append(prefix);
			this.result.append(this.id.incrementAndGet());
			return this.result;
		}
	};

	public static final SopremoFunction SOUND_EX = new SopremoFunction1<TextNode>("soundEx") {
		private final transient TextNode soundex = new TextNode();

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.function.SopremoFunction1#call(eu.stratosphere.sopremo.type.IJsonNode)
		 */
		@Override
		protected TextNode call(TextNode input) {
			this.soundex.clear();
			try {
				SoundEx.generateSoundExInto(input, this.soundex);
			} catch (IOException e) {
			}
			return this.soundex;
		}
	};

	public static final SopremoFunction REMOVE_VOWELS =
		CoreFunctions.REPLACE.withDefaultParameters(TextNode.valueOf("(?i)[aeiou]"));

	@Name(preposition = "longest")
	public static final Aggregation LONGEST = new TransitiveAggregation<CachingArrayNode>("longest",
		new CachingArrayNode()) {
		private transient int currentLength = 0;

		private transient NodeCache nodeCache = new NodeCache();

		@Override
		public void initialize() {
			this.currentLength = 0;
			this.aggregator.clear();
		}

		@Override
		public CachingArrayNode aggregate(final CachingArrayNode aggregator, final IJsonNode node) {
			TextNode text = TypeCoercer.INSTANCE.coerce(node, this.nodeCache, TextNode.class);
			if (text.length() > this.currentLength) {
				aggregator.clear();
				aggregator.addClone(text);
			} else if (text.length() == this.currentLength)
				aggregator.addClone(text);
			return aggregator;
		}
	};

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
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			Similarity<?> similarity;
			if (params.length > 1)
				similarity = SimilarityFactory.INSTANCE.create(this.similarity, params[0], params[1], true);
			else
				similarity = SimilarityFactory.INSTANCE.create(this.similarity, params[0], params[0], true);
			return new SimilarityExpression((Similarity<IJsonNode>) similarity);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
		 */
		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			this.similarity.appendAsString(appendable);
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
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object)
		 */
		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			for (int index = 0; index < params.length; index++)
				if (params[index] instanceof MethodPointerExpression) {
					final String functionName = ((MethodPointerExpression) params[index]).getFunctionName();
					params[index] = new FunctionCall(functionName,
						SopremoRuntime.getInstance().getCurrentEvaluationContext(), EvaluationExpression.VALUE);
				}
			return new BeliefResolution(params);
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
		 */
		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			appendable.append("vote");
		}
	}
}
