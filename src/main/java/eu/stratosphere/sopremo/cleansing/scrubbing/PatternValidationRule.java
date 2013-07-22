package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import eu.stratosphere.sopremo.Immutable;
import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;

@DefaultSerializer(value = PatternValidationRule.PatternValidationRuleSerializer.class)
public class PatternValidationRule extends ValidationRule {
	public static class PatternValidationRuleSerializer extends Serializer<PatternValidationRule>{
		
		FieldSerializer<PatternValidationRule> fieldSerializer;
		
		public PatternValidationRuleSerializer(Kryo kryo, Class<PatternValidationRuleSerializer> type) {
			fieldSerializer = new FieldSerializer<PatternValidationRule>(kryo, type);
		}
		
		@Override
		public void write(Kryo kryo,Output output, PatternValidationRule object) {
			fieldSerializer.write(kryo, output, object);
			kryo.writeObject(output, object.pattern.pattern());
		}

		@Override
		public PatternValidationRule read(Kryo kryo, Input input, Class<PatternValidationRule> type) {
			PatternValidationRule object =  fieldSerializer.read(kryo, input, type);
			String pattern = kryo.readObject(input, String.class);
			object.pattern = Pattern.compile(pattern);
			return object;
		}

		@Override
		public PatternValidationRule copy(Kryo kryo, PatternValidationRule original) {
			return new PatternValidationRule(original.pattern);
		}
	}
	@Immutable
	private transient Pattern pattern;
	
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
	public boolean validate(final IJsonNode node) {
		return this.pattern.matcher(TypeCoercer.INSTANCE.coerce(node, this.nodeCache, TextNode.class)).matches();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		PatternValidationRule other = (PatternValidationRule) obj;
		if (pattern == null) {
			if (other.pattern != null)
				return false;
		} else if (!pattern.pattern().equals(other.pattern.pattern()))
			return false;
		return true;
	}
}
