package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.cleansing.CleansFunctions;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This class provides the functionality to specify the allowed range of values
 * for a record-field. The evaluation of this rule is symmetric to the
 * compareTo-method. Both, the lower and upper bound, are treated ase the
 * lowest/highest allowed value for the field. The following example shows the
 * usage of this rule in a meteor-script:
 * 
 * <code><pre>
 * ...
 * $persons_scrubbed = scrub $persons_sample with rules {
 * 	...
 * 	age: range(0, 100),
 *  code: range("A", "Z"),
 * 	...
 * };
 * ...
 * </pre></code>
 * 
 * implemented corrections: <br/>
 * - {@link CleansFunctions#CHOOSE_NEAREST_BOUND}
 * 
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
public class RangeRule extends ValidationRule {
	private IJsonNode min, max;

	public RangeRule(final IJsonNode min, final IJsonNode max) {
		this.min = min;
		this.max = max;
	}

	/**
	 * Initializes RangeRule.
	 * 
	 */
	RangeRule() {
	}

	@Override
	public boolean validate(final IJsonNode value) {
		return this.min.compareTo(value) <= 0 && value.compareTo(this.max) <= 0;
	}

	public IJsonNode getMin() {
		return min;
	}

	public IJsonNode getMax() {
		return max;
	}
}
