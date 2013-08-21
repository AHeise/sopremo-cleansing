package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.NullNode;

/**
 * This class provides the functionality to check the presence of values. When
 * using this rule for a field only non null and non missing values are allowed.
 * The following example shows the usage of this rule in a meteor-script:
 * 
 * <code><pre>
 * ...
 * $persons_scrubbed = scrub $persons_sample with rules {
 * 	...
 * 	id: required,
 * 	...
 * };
 * ...
 * </pre></code>
 * 
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
public class NonNullConstraint extends ValidationRule implements StatefulConstant {

	public NonNullConstraint() {
	}

	public NonNullConstraint(IJsonNode defaultValue) {
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	@Override
	public boolean validate(IJsonNode value) {
		return value != NullNode.getInstance()
				&& value != MissingNode.getInstance();
	}
}
