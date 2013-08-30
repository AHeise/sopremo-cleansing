package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.regex.Pattern;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;
import eu.stratosphere.util.Immutable;

/**
 * This class provides the functionality to validate the structure of values.
 * The desired structure is specified via a regular expression.
 * The following example shows the usage of this rule in a meteor-script:
 * 
 * <code><pre>
 * ...
 * $persons_scrubbed = scrub $persons_sample with rules {
 *	...
 *	format: hasPattern("\d{4}-\d{4}"),
 *	...
 * };
 * ...
 * </pre></code>
 * 
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
@Name(verb="hasPattern")
@DefaultSerializer(value = PatternValidationConstraint.PatternValidationRuleSerializer.class)
public class PatternValidationConstraint extends ValidationRule {
	public static class PatternValidationRuleSerializer extends
			Serializer<PatternValidationConstraint> {

		FieldSerializer<PatternValidationConstraint> fieldSerializer;

		public PatternValidationRuleSerializer(Kryo kryo,
				Class<PatternValidationRuleSerializer> type) {
			fieldSerializer = new FieldSerializer<PatternValidationConstraint>(kryo,
					type);
		}

		@Override
		public void write(Kryo kryo, Output output, PatternValidationConstraint object) {
			fieldSerializer.write(kryo, output, object);
			kryo.writeObject(output, object.pattern.pattern());
		}

		@Override
		public PatternValidationConstraint read(Kryo kryo, Input input,
				Class<PatternValidationConstraint> type) {
			PatternValidationConstraint object = fieldSerializer.read(kryo, input,
					type);
			String pattern = kryo.readObject(input, String.class);
			object.pattern = Pattern.compile(pattern);
			return object;
		}

		@Override
		public PatternValidationConstraint copy(Kryo kryo,
				PatternValidationConstraint original) {
			PatternValidationConstraint copy = fieldSerializer.copy(kryo, original);
			copy.pattern = original.pattern;
			return copy;
		}
	}

	@Immutable
	private transient Pattern pattern;

	public PatternValidationConstraint(final Pattern pattern) {
		this.pattern = pattern;
	}

	/**
	 * Initializes PatternValidationRule.
	 */
	PatternValidationConstraint() {
		this.pattern = null;
	}

	private transient NodeCache nodeCache = new NodeCache();

	@Override
	public boolean validate(final IJsonNode node) {
		return this.pattern.matcher(
				TypeCoercer.INSTANCE.coerce(node, this.nodeCache,
						TextNode.class)).matches();
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
		PatternValidationConstraint other = (PatternValidationConstraint) obj;
		if (pattern == null) {
			if (other.pattern != null)
				return false;
		} else if (!pattern.pattern().equals(other.pattern.pattern()))
			return false;
		return true;
	}
}
