package eu.stratosphere.sopremo.cleansing;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.cleansing.fusion.ChooseRandomResolution;
import eu.stratosphere.sopremo.cleansing.fusion.DefaultValueResolution;
import eu.stratosphere.sopremo.cleansing.fusion.MergeDistinctResolution;
import eu.stratosphere.sopremo.cleansing.fusion.MostFrequentResolution;
import eu.stratosphere.sopremo.cleansing.scrubbing.BlackListConstraint;
import eu.stratosphere.sopremo.cleansing.scrubbing.DefaultValueCorrection;
import eu.stratosphere.sopremo.cleansing.scrubbing.IllegalCharacterConstraint;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullConstraint;
import eu.stratosphere.sopremo.cleansing.scrubbing.PatternValidationConstraint;
import eu.stratosphere.sopremo.cleansing.scrubbing.RangeConstraint;
import eu.stratosphere.sopremo.cleansing.scrubbing.UnresolvableCorrection;
import eu.stratosphere.sopremo.cleansing.scrubbing.ValidationRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.ValueCorrection;
import eu.stratosphere.sopremo.cleansing.scrubbing.WhiteListConstraint;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityExpression;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityFactory;
import eu.stratosphere.sopremo.cleansing.similarity.set.JaccardSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroWinklerSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.LevenshteinSimilarity;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.packages.ConstantRegistryCallback;
import eu.stratosphere.sopremo.packages.FunctionRegistryCallback;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;
//0.2compability
//import eu.stratosphere.sopremo.SopremoEnvironment;

public class CleansFunctions implements BuiltinProvider,
		ConstantRegistryCallback, FunctionRegistryCallback {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.packages.ConstantRegistryCallback#registerConstants
	 * (eu.stratosphere.sopremo.packages. IConstantRegistry)
	 */
	@Override
	public void registerConstants(IConstantRegistry constantRegistry) {

		constantRegistry.put("chooseNearestBound", CHOOSE_NEAREST_BOUND);
		constantRegistry.put("chooseFirst", CHOOSE_FIRST_FROM_LIST);
		constantRegistry.put("removeIllegalCharacters",
				REMOVE_ILLEGAL_CHARACTERS);

		this.registerConstant(new NonNullConstraint(), constantRegistry);
		this.registerConstant(new MostFrequentResolution(), constantRegistry);
		this.registerConstant(MergeDistinctResolution.INSTANCE,
				constantRegistry);
		this.registerConstant(ChooseRandomResolution.INSTANCE, constantRegistry);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.packages.FunctionRegistryCallback#registerFunctions
	 * (eu.stratosphere.sopremo.packages. IFunctionRegistry)
	 */
	@Override
	public void registerFunctions(IFunctionRegistry registry) {
		registry.put("jaccard", new SimilarityMacro(new JaccardSimilarity()));
		registry.put("levenshtein", new SimilarityMacro(new LevenshteinSimilarity()));
		registry.put("jaroWinkler", new SimilarityMacro(
				new JaroWinklerSimilarity()));

		this.registerMacro(new PatternValidationRuleMacro(), registry);
		this.registerMacro(new RangeRuleMacro(), registry);
		this.registerMacro(new DefaultValueCorrectionMacro(), registry);
		this.registerMacro(new WhiteListRuleMacro(), registry);
		this.registerMacro(new BlackListRuleMacro(), registry);
		this.registerMacro(new IllegalCharacterRuleMacro(), registry);

		this.registerMacro(new DefaultValueResolutionMacro(), registry);

		// 0.2compability
		// registry.put("vote", new VoteMacro());
	}

	private void registerMacro(MacroBase macro, IFunctionRegistry registry) {
		registry.put(this.getScriptName(((CleansingMacro) macro)
				.getWrappedExpressionClass()), macro);
	}

	private void registerConstant(EvaluationExpression constant,
			IConstantRegistry registry) {
		registry.put(this.getScriptName(constant.getClass()), constant);
	}

	private String getScriptName(Class<? extends EvaluationExpression> expr) {
		Name name = expr.getAnnotation(Name.class);
		if (name == null) {
			SopremoUtil.LOG.warn("No name for " + expr + " found, using '"
					+ expr.getSimpleName() + "'.");
			return expr.getSimpleName();
		}
		if (name.noun().length != 0)
			return name.noun()[0];
		if (name.verb().length != 0)
			return name.verb()[0];
		if (name.adjective().length != 0)
			return name.adjective()[0];
		if (name.preposition().length != 0)
			return name.preposition()[0];
		SopremoUtil.LOG.warn("Name found for " + expr
				+ " but non of the properties are set, using '"
				+ expr.getSimpleName() + "'");
		return expr.getSimpleName();
	}

	public static final SopremoFunction GENERATE_ID = new GenerateId();

	@Name(verb = "generateId")
	public static class GenerateId extends SopremoFunction1<TextNode> {
		GenerateId() {
			super("generateId");
		}

		private transient TextNode resultId = new TextNode();

		private transient int id = 0;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.cleansing.CleansFunctions.LONGEST#call(eu
		 * .stratosphere.sopremo.type.IArrayNode)
		 */
		@Override
		public IJsonNode call(TextNode prefix) {
			this.resultId.clear();
			this.resultId.append(prefix);
			this.resultId.append(this.id++);
			return this.resultId;
		}
	};

	public static final SopremoFunction SOUNDEX = new SoundEx();

	@Name(noun = "soundex")
	public static class SoundEx extends SopremoFunction1<TextNode> {
		SoundEx() {
			super("soundex");
		}

		private transient TextNode soundex = new TextNode();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.cleansing.CleansFunctions.LONGEST#call(eu
		 * .stratosphere.sopremo.type.IArrayNode)
		 */
		@Override
		public IJsonNode call(TextNode input) {
			this.soundex.clear();
			try {
				eu.stratosphere.sopremo.cleansing.SoundEx
					.generateSoundExInto(input, this.soundex);
			} catch (IOException e) {
			}
			return this.soundex;
		}
	};

	// 0.2compability
	// @Name(verb = "removeVowels")
	// public static final SopremoFunction REMOVE_VOWELS =
	// CoreFunctions.REPLACE.bind(TextNode.valueOf("(?i)[aeiou]"),
	// TextNode.valueOf(""));

	public static final SopremoFunction LONGEST = new Longest();

	@Name(adjective = "longest")
	public static class Longest extends SopremoFunction1<IArrayNode<IJsonNode>> {
		Longest() {
			super("longest");
		}

		private transient NodeCache nodeCache = new NodeCache();

		private transient CachingArrayNode<IJsonNode> result = new CachingArrayNode<IJsonNode>();

		private transient IntList sizes = new IntArrayList();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.cleansing.CleansFunctions.LONGEST#call(eu
		 * .stratosphere.sopremo.type.IArrayNode)
		 */
		@Override
		public IJsonNode call(IArrayNode<IJsonNode> values) {
			this.result.setSize(0);
			if (values.isEmpty())
				return this.result;

			if (values.size() == 1) {
				this.result.addClone(values.get(0));
				return this.result;
			}

			this.sizes.clear();
			for (IJsonNode value : values)
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

	private static class PatternValidationRuleMacro extends CleansingMacro {

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object)
		 */
		@Override
		public PatternValidationConstraint call(EvaluationExpression[] params) {

			if (params.length == 1)
				return new PatternValidationConstraint(
						Pattern.compile(params[0].evaluate(
								NullNode.getInstance()).toString()));
			else
				throw new IllegalArgumentException("Wrong number of arguments.");

		}

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public Class<? extends EvaluationExpression> getWrappedExpressionClass() {
			return PatternValidationConstraint.class;
		}
	}

	private static class RangeRuleMacro extends CleansingMacro {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length == 2) {
				return new RangeConstraint(params[0].evaluate(NullNode
						.getInstance()), params[1].evaluate(NullNode
						.getInstance()));
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

		@Override
		public Class<? extends EvaluationExpression> getWrappedExpressionClass() {
			return RangeConstraint.class;
		}

	}

	private static class WhiteListRuleMacro extends CleansingMacro {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length > 0) {
				List<IJsonNode> possibleValues = new LinkedList<IJsonNode>();
				for (EvaluationExpression expr : params) {
					convertToList(expr, possibleValues);
				}
				return new WhiteListConstraint(possibleValues);
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

		@SuppressWarnings("unchecked")
		private void convertToList(EvaluationExpression value,
				List<IJsonNode> possibleValues) {
			if (value.evaluate(NullNode.getInstance()) instanceof IArrayNode) {
				for (IJsonNode node : (IArrayNode<IJsonNode>) value
						.evaluate(NullNode.getInstance())) {
					possibleValues.add(node);
				}
			} else {
				possibleValues.add(value.evaluate(NullNode.getInstance()));
			}
		}

		@Override
		public Class<? extends EvaluationExpression> getWrappedExpressionClass() {
			return WhiteListConstraint.class;
		}
	}

	private static class BlackListRuleMacro extends CleansingMacro {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length > 0) {
				List<IJsonNode> forbiddenValues = new LinkedList<IJsonNode>();
				for (EvaluationExpression expr : params) {
					convertToList(expr, forbiddenValues);
				}
				return new BlackListConstraint(forbiddenValues);
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

		@SuppressWarnings("unchecked")
		private void convertToList(EvaluationExpression value,
				List<IJsonNode> forbiddenValues) {
			if (value.evaluate(NullNode.getInstance()) instanceof IArrayNode) {
				for (IJsonNode node : (IArrayNode<IJsonNode>) value
						.evaluate(NullNode.getInstance())) {
					forbiddenValues.add(node);
				}
			} else {
				forbiddenValues.add(value.evaluate(NullNode.getInstance()));
			}
		}

		@Override
		public Class<? extends EvaluationExpression> getWrappedExpressionClass() {
			return BlackListConstraint.class;
		}
	}

	private static class IllegalCharacterRuleMacro extends CleansingMacro {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length > 0) {
				TextNode illegalCharacters = new TextNode();
				for (EvaluationExpression expr : params) {
					illegalCharacters.append((TextNode) expr.evaluate(NullNode
							.getInstance()));
				}
				return new IllegalCharacterConstraint(illegalCharacters);
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

		@Override
		public Class<? extends EvaluationExpression> getWrappedExpressionClass() {
			return IllegalCharacterConstraint.class;
		}
	}

	private static class DefaultValueCorrectionMacro extends CleansingMacro {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub
		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length == 1) {
				return new DefaultValueCorrection(params[0].evaluate(NullNode
						.getInstance()));
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

		@Override
		public Class<? extends EvaluationExpression> getWrappedExpressionClass() {
			return DefaultValueCorrection.class;
		}

	}

	private static class DefaultValueResolutionMacro extends CleansingMacro {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub
		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length == 1) {
				return new DefaultValueResolution(params[0].evaluate(NullNode
						.getInstance()));
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

		@Override
		public Class<? extends EvaluationExpression> getWrappedExpressionClass() {
			return DefaultValueResolution.class;
		}
	}

	/**
	 * @author Arvid Heise
	 */
	private static class SimilarityMacro extends MacroBase {
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
		 * 
		 * @see
		 * eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable
		 * )
		 */
		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			this.similarity.appendAsString(appendable);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			for (EvaluationExpression evaluationExpression : params)
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
				PathSegmentExpression left =
					(PathSegmentExpression) (input0.getIndex() == 0 ? params[0] : params[1]).remove(InputSelection.class);
				PathSegmentExpression right =
					(PathSegmentExpression) (input1.getIndex() == 0 ? params[0] : params[1]).remove(InputSelection.class);

				similarity =
					(Similarity<IJsonNode>) SimilarityFactory.INSTANCE.create(this.similarity, left, right, true);
			}
			else {
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
		public IJsonNode fix(IJsonNode value, ValidationRule violatedRule) {
			if (violatedRule instanceof RangeConstraint) {
				final RangeConstraint that = (RangeConstraint) violatedRule;
				if (that.getMin().compareTo(value) > 0)
					return that.getMin();
				return that.getMax();
			} else {
				return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
			}
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
		public IJsonNode fix(IJsonNode value, ValidationRule violatedRule) {
			if (violatedRule instanceof WhiteListConstraint) {
				final WhiteListConstraint that = (WhiteListConstraint) violatedRule;
				return that.getPossibleValues().get(0);
			} else {
				return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
			}
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
		public IJsonNode fix(IJsonNode value, ValidationRule violatedRule) {
			if (violatedRule instanceof IllegalCharacterConstraint) {
				final IllegalCharacterConstraint that = (IllegalCharacterConstraint) violatedRule;
				TextNode fixedValue = new TextNode();
				char[] illegalCharacters = that.getIllegalCharacters();
				boolean append = true;
				for (char c : value.toString().toCharArray()) {
					for (char ic : illegalCharacters) {
						if (c == ic) {
							append = false;
							break;
						}
					}
					if (append)
						fixedValue.append(c);
					append = true;
				}
				return fixedValue;
			} else {
				return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
			}
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
