package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

@DefaultSerializer(value = PatternValidationRule.PatternValidationRuleSerializer.class)
public class PatternValidationRule extends ValidationRule {
	public static class PatternValidationRuleSerializer extends Serializer<PatternValidationRule>{

		@Override
		public void write(Kryo kryo, Output output, PatternValidationRule object) {
			kryo.writeClassAndObject(output, object.pattern.pattern());
		}

		@Override
		public PatternValidationRule read(Kryo kryo, Input input, Class<PatternValidationRule> type) {
			String pattern = (String)kryo.readClassAndObject(input);
			PatternValidationRule deserialized = new PatternValidationRule(Pattern.compile(pattern));
			return deserialized;
		}

		@Override
		public PatternValidationRule copy(Kryo kryo, PatternValidationRule original) {
			return new PatternValidationRule(original.pattern);
		}
	}

	private final Pattern pattern;

	public PatternValidationRule(final Pattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Initializes PatternValidationRule.
	 */
	PatternValidationRule() {
		this.pattern = null;
	}

	private transient NodeCache nodeCache = new NodeCache();

	@Override
	protected boolean validate(final IJsonNode node) {
		return this.pattern.matcher(TypeCoercer.INSTANCE.coerce(node, this.nodeCache, TextNode.class)).matches();
	}
}
