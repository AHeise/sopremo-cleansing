package eu.stratosphere.sopremo.cleansing;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.cleansing.fusion.ChooseRandomResolution;
import eu.stratosphere.sopremo.cleansing.fusion.DefaultValueResolution;
import eu.stratosphere.sopremo.cleansing.fusion.MergeDistinctResolution;
import eu.stratosphere.sopremo.cleansing.fusion.MostFrequentResolution;
import eu.stratosphere.sopremo.cleansing.scrubbing.*;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityExpression;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityFactory;
import eu.stratosphere.sopremo.cleansing.similarity.set.JaccardSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroWinklerSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.LevenshteinSimilarity;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.*;
import eu.stratosphere.sopremo.type.*;
//0.2compability
//import eu.stratosphere.sopremo.SopremoEnvironment;

public class CleansFunctions implements BuiltinProvider,
		ConstantRegistryCallback, FunctionRegistryCallback {

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.packages.ConstantRegistryCallback#registerConstants
	 * (eu.stratosphere.sopremo.packages. IConstantRegistry)
	 */
	@Override
	public void registerConstants(final IConstantRegistry constantRegistry) {

		constantRegistry.put("chooseNearestBound", CHOOSE_NEAREST_BOUND);
		constantRegistry.put("chooseFirst", CHOOSE_FIRST_FROM_LIST);
		constantRegistry.put("removeIllegalCharacters",
			REMOVE_ILLEGAL_CHARACTERS);
		constantRegistry.put("tryCoercing", TRY_COERCING_TO_TYPE);

		this.registerTypeConstants(constantRegistry);

		this.registerConstant(new NonNullConstraint(), constantRegistry);
		this.registerConstant(new MostFrequentResolution(), constantRegistry);
		this.registerConstant(MergeDistinctResolution.INSTANCE,
			constantRegistry);
		this.registerConstant(ChooseRandomResolution.INSTANCE, constantRegistry);
	}

	private void registerTypeConstants(final IConstantRegistry registry) {
		for (final String constant : TypeConstraint.AvailableTypes.keySet())
			registry.put(constant,
				new ConstantExpression(TextNode.valueOf(constant)));
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.packages.FunctionRegistryCallback#registerFunctions
	 * (eu.stratosphere.sopremo.packages. IFunctionRegistry)
	 */
	@Override
	public void registerFunctions(final IFunctionRegistry registry) {
		registry.put("jaccard", new SimilarityMacro(new JaccardSimilarity()));
		registry.put("levenshtein", new SimilarityMacro(new LevenshteinSimilarity()));
		registry.put("jaroWinkler", new SimilarityMacro(new JaroWinklerSimilarity()));
		registry.put("jaro", new SimilarityMacro(new JaroSimilarity()));

		// scrubbing constraint macros
		this.registerMacro(new ConstraintMacro(PatternValidationConstraint.class) {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.cleansing.ConstraintMacro#process(eu.stratosphere.sopremo.expressions.
			 * EvaluationExpression[])
			 */
			@Override
			protected EvaluationExpression process(EvaluationExpression[] params) {
				return new PatternValidationConstraint(
					Pattern.compile(params[0].evaluate(MissingNode.getInstance()).toString()));
			}
		}, registry);
		this.registerMacro(new ConstraintMacro(RangeConstraint.class, 2) {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.cleansing.ConstraintMacro#process(eu.stratosphere.sopremo.expressions.
			 * EvaluationExpression[])
			 */
			@Override
			protected EvaluationExpression process(EvaluationExpression[] params) {
				return new RangeConstraint(params[0].evaluate(MissingNode.getInstance()),
					params[1].evaluate(MissingNode.getInstance()));
			}
		}, registry);
		this.registerMacro(new ConstraintMacro(DefaultValueCorrection.class), registry);
		this.registerMacro(new ConstraintMacro(DefaultValueResolution.class), registry);
		this.registerMacro(new ConstraintMacro(WhiteListConstraint.class), registry);
		this.registerMacro(new ConstraintMacro(BlackListConstraint.class), registry);
		this.registerMacro(new ConstraintMacro(IllegalCharacterConstraint.class), registry);
		this.registerMacro(new ConstraintMacro(TypeConstraint.class) {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.sopremo.cleansing.ConstraintMacro#process(eu.stratosphere.sopremo.expressions.
			 * EvaluationExpression[])
			 */
			@Override
			protected EvaluationExpression process(EvaluationExpression[] params) {
				final Class<? extends IJsonNode> type =
					TypeConstraint.AvailableTypes.get(params[0].evaluate(MissingNode.getInstance()).toString());
				return new TypeConstraint(type);
			}
		}, registry);
	}

	private void registerMacro(final ConstraintMacro macro, final IFunctionRegistry registry) {
		registry.put(macro.getConstraintClass().getAnnotation(Name.class), macro);
	}

	private void registerConstant(final EvaluationExpression constant, final IConstantRegistry registry) {
		registry.put(constant.getClass().getAnnotation(Name.class), constant);
	}

	@Name(verb = "generateId")
	public static final SopremoFunction GENERATE_ID = new SopremoFunction1<TextNode>() {
		private transient TextNode resultId = new TextNode();

		private transient int id = 0;

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.cleansing.CleansFunctions.LONGEST#call(eu
		 * .stratosphere.sopremo.type.IArrayNode)
		 */
		@Override
		public IJsonNode call(final TextNode prefix) {
			this.resultId.clear();
			this.resultId.append(prefix);
			this.resultId.append(this.id++);
			return this.resultId;
		}
	};

	@Name(noun = "soundex")
	public static final SopremoFunction SOUNDEX = new SopremoFunction1<TextNode>() {
		private transient TextNode soundex = new TextNode();

		@Override
		public IJsonNode call(final TextNode input) {
			this.soundex.clear();
			try {
				eu.stratosphere.sopremo.cleansing.similarity.SoundEx.generateSoundExInto(input, this.soundex);
			} catch (final IOException e) {
			}
			return this.soundex;
		}
	};

	// 0.2compability
	// @Name(verb = "removeVowels")
	// public static final SopremoFunction REMOVE_VOWELS =
	// CoreFunctions.REPLACE.bind(TextNode.valueOf("(?i)[aeiou]"),
	// TextNode.valueOf(""));
	
	@Name(adjective = "longest")
	public static SopremoFunction Longest = new SopremoFunction1<IArrayNode<IJsonNode>>() {
		private transient NodeCache nodeCache = new NodeCache();

		private transient CachingArrayNode<IJsonNode> result = new CachingArrayNode<IJsonNode>();

		private transient IntList sizes = new IntArrayList();

		@Override
		public IJsonNode call(final IArrayNode<IJsonNode> values) {
			this.result.setSize(0);
			if (values.isEmpty())
				return this.result;

			if (values.size() == 1) {
				this.result.addClone(values.get(0));
				return this.result;
			}

			this.sizes.clear();
			for (final IJsonNode value : values)
				this.sizes.add(TypeCoercer.INSTANCE.coerce(value,
					this.nodeCache, TextNode.class).length());
			int maxSize = this.sizes.getInt(0);
			for (int index = 1; index < this.sizes.size(); index++)
				maxSize = Math.max(index, maxSize);
			for (int index = 0; index < this.sizes.size(); index++)
				if (maxSize == this.sizes.getInt(index))
					this.result.addClone(values.get(index));
			return this.result;
		}
	};

	/**
	 * @author Arvid Heise
	 */
	static class SimilarityMacro extends MacroBase {
		private final Similarity<?> similarity;

		/**
		 * Initializes SimmetricMacro.
		 * 
		 * @param similarity
		 */
		public SimilarityMacro(final Similarity<?> similarity) {
			super(1, 2);
			this.similarity = similarity;
		}
		
		/**
		 * Initializes CleansFunctions.SimilarityMacro.
		 *
		 */
		SimilarityMacro() {
			this(null);
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable
		 * )
		 */
		@Override
		public void appendAsString(final Appendable appendable) throws IOException {
			this.similarity.appendAsString(appendable);
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.sopremo.function.MacroBase#process(eu.stratosphere.sopremo.expressions.EvaluationExpression
		 * [])
		 */
		@SuppressWarnings("unchecked")
		@Override
		protected EvaluationExpression process(EvaluationExpression[] params) {
			for (final EvaluationExpression evaluationExpression : params)
				if (!(evaluationExpression instanceof PathSegmentExpression))
					throw new IllegalArgumentException(
						"Can only expand simple path expressions");

			Similarity<IJsonNode> similarity;
			if (params.length > 1) {
				final InputSelection input0 = params[0].findFirst(InputSelection.class);
				final InputSelection input1 = params[1].findFirst(InputSelection.class);
				if (input0 == null || input1 == null)
					throw new IllegalArgumentException("Paths are incomplete (source is unknown) " +
						Arrays.asList(params));
				if (input0.getIndex() == input1.getIndex())
					throw new IllegalArgumentException("Paths are using the same source " + Arrays.asList(params));
				final PathSegmentExpression left =
					(PathSegmentExpression) (input0.getIndex() == 0 ? params[0] : params[1]).remove(InputSelection.class);
				final PathSegmentExpression right =
					(PathSegmentExpression) (input1.getIndex() == 0 ? params[0] : params[1]).remove(InputSelection.class);

				similarity =
					(Similarity<IJsonNode>) SimilarityFactory.INSTANCE.create(this.similarity, left, right, true);
			} else {
				final PathSegmentExpression path = (PathSegmentExpression) params[0].remove(InputSelection.class);
				similarity =
					(Similarity<IJsonNode>) SimilarityFactory.INSTANCE.create(this.similarity, path, path, true);
			}
			return new SimilarityExpression(similarity);
		}
	}

	/**
	 * This correction is a fix for {@link RangeRule}. To solve a violation this
	 * correction simply chooses the nearest bound (lower bound if the actual
	 * value was lower than the lower bound, upper bound if the actual value was
	 * higher than the upper bound). To specify this correction for a {@link RangeRule} use the following syntax:
	 * <code><pre>
	 * ...
	 * $persons_scrubbed = scrub $persons_sample with rules {
	 * 	...
	 * 	age: range(0, 100) ?: chooseNearestBound,
	 * 	...
	 * };
	 * ...
	 * </pre></code>
	 */
	public static final ValueCorrection CHOOSE_NEAREST_BOUND = new ValueCorrection() {
		private Object readResolve() {
			return CHOOSE_NEAREST_BOUND;
		}

		@Override
		public IJsonNode fix(final IJsonNode value, final ValidationRule violatedRule) {
			if (violatedRule instanceof RangeConstraint) {
				final RangeConstraint that = (RangeConstraint) violatedRule;
				if (that.getMin().compareTo(value) > 0)
					return that.getMin();
				return that.getMax();
			}
			return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
		}
	};

	public static final ValueCorrection TRY_COERCING_TO_TYPE = new ValueCorrection() {
		private Object readResolve() {
			return TRY_COERCING_TO_TYPE;
		}

		@Override
		public IJsonNode fix(final IJsonNode value, final ValidationRule violatedRule) {
			if (violatedRule instanceof TypeConstraint) {
				final Class<? extends IJsonNode> type = ((TypeConstraint) violatedRule)
					.getType();
				final NodeCache cache = ((TypeConstraint) violatedRule)
					.getNodeCache();
				try {
					if (value instanceof TextNode)
						return LenientParser.INSTANCE.parse((TextNode) value,
							type, LenientParser.ELIMINATE_NOISE);
					return TypeCoercer.INSTANCE.coerce(value, cache, type);
				} catch (final Exception e) {
					return UnresolvableCorrection.INSTANCE.fix(value,
						violatedRule);
				}
			}
			return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
		}
	};

	/**
	 * This correction is a fix for {@link WhiteListRule}. To solve a violation
	 * this correction simply chooses the first allowed value from the white
	 * list. To specify this correction for a {@link WhiteListRule} use the
	 * following syntax: <code><pre>
	 * ...
	 * $persons_scrubbed = scrub $persons_sample with rules {
	 * 	...
	 * 	person_type: containedIn(["customer", "employee", "founder"]) ?: chooseFirstFromList,
	 * 	...
	 * };
	 * ...
	 * </pre></code>
	 */
	public static final ValueCorrection CHOOSE_FIRST_FROM_LIST = new ValueCorrection() {
		private Object readResolve() {
			return CHOOSE_FIRST_FROM_LIST;
		}

		@Override
		public IJsonNode fix(final IJsonNode value, final ValidationRule violatedRule) {
			if (violatedRule instanceof WhiteListConstraint) {
				final WhiteListConstraint that = (WhiteListConstraint) violatedRule;
				return that.getPossibleValues().get(0);
			}
			return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
		}
	};

	/**
	 * This correction is a fix for {@link IllegalCharacterRule}. To solve a
	 * violation this correction simply removes all violating characters from
	 * the value. To specify this correction for a {@link IllegalCharacterRule} use the following syntax: <code><pre>
	 * ...
	 * $persons_scrubbed = scrub $persons_sample with rules {
	 * 	...
	 * 	name: illegalCharacters("%", "$", "!", "[", "]") ?: removeIllegalCharacters,
	 * 	...
	 * };
	 * ...
	 * </pre></code>
	 */
	public static final ValueCorrection REMOVE_ILLEGAL_CHARACTERS = new ValueCorrection() {
		private Object readResolve() {
			return REMOVE_ILLEGAL_CHARACTERS;
		}

		@Override
		public IJsonNode fix(final IJsonNode value, final ValidationRule violatedRule) {
			if (violatedRule instanceof IllegalCharacterConstraint) {
				final IllegalCharacterConstraint that = (IllegalCharacterConstraint) violatedRule;
				final TextNode fixedValue = new TextNode();
				final char[] illegalCharacters = that.getIllegalCharacters();
				boolean append = true;
				for (final char c : value.toString().toCharArray()) {
					for (final char ic : illegalCharacters)
						if (c == ic) {
							append = false;
							break;
						}
					if (append)
						fixedValue.append(c);
					append = true;
				}
				return fixedValue;
			}
			return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
		}
	};

	// 0.2compability
	// private static class VoteMacro extends MacroBase {
	// /**
	// * Initializes CleansFunctions.VoteMacro.
	// */
	// public VoteMacro() {
	// }
	//
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object,
	// java.lang.Object,
	// * eu.stratosphere.sopremo.EvaluationContext)
	// */
	// @Override
	// public EvaluationExpression call(EvaluationExpression[] params) {
	// for (int index = 0; index < params.length; index++)
	// if (params[index] instanceof MethodPointerExpression) {
	// final String functionName = ((MethodPointerExpression)
	// params[index]).getFunctionName();
	// params[index] = new FunctionCall(functionName,
	// SopremoEnvironment.getInstance()
	// .getEvaluationContext(), EvaluationExpression.VALUE);
	// }
	// return new BeliefResolution(params);
	// }
	//
	// /*
	// * (non-Javadoc)
	// * @see
	// eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	// */
	// @Override
	// public void appendAsString(Appendable appendable) throws IOException {
	// appendable.append("vote");
	// }
	//
	// }

	//
	// @Name(verb = "satisfies")
	// public static class Filter extends
	// SopremoFunction2<IArrayNode<IJsonNode>, FunctionNode> {
	// Filter() {
	// super("satisfies");
	// }
	//
	// @Override
	// protected IJsonNode call(IArrayNode<IJsonNode> input, final FunctionNode
	// mapExpression) {
	// SopremoUtil.assertArguments(mapExpression.getFunction(), 1);
	//
	// this.result.clear();
	// final FunctionCache calls = this.caches.get(mapExpression.getFunction());
	// for (int index = 0; index < input.size(); index++) {
	// this.parameters.set(0, input.get(index));
	// this.result.add(calls.get(index).call(this.parameters));
	// }
	// return this.result;
	// }
	// };
}
