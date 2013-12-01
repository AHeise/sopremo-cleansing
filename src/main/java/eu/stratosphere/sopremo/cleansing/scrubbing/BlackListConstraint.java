package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This class provides the functionality to specify a certain number of
 * forbidden values for a specific record-field. Only values that a not included
 * in this 'black list' are allowed as values for the result. The following
 * example shows the usage of this rule in a meteor-script:
 * 
 * <code><pre>
 * ...
 * $persons_scrubbed = scrub $persons_sample with rules {
 *	...
 *	works_for: notContainedIn(["NOT_SPECIFIED", "UNKNOWN", "DEFAULT"]),
 *	...
 * };
 * ...
 * </pre></code>
 * 
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
@Name(verb="notContainedIn")
public class BlackListConstraint extends ValidationRule {
	private final List<IJsonNode> blacklistedValues;

	@SuppressWarnings("unchecked")
	public BlackListConstraint(List<? extends IJsonNode> blacklistedValues) {
		this.blacklistedValues = (List<IJsonNode>) blacklistedValues;
	}

	@SuppressWarnings("unchecked")
	public BlackListConstraint(List<? extends IJsonNode> blacklistedValues,
			IJsonNode defaultValue) {
		this.blacklistedValues = (List<IJsonNode>) blacklistedValues;
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}
	
	public BlackListConstraint(ArrayNode<IJsonNode> blacklistedValues) {
		this.blacklistedValues = new LinkedList<IJsonNode>();
		for (IJsonNode node : blacklistedValues) {
			this.blacklistedValues.add(node);
		}
	}

	BlackListConstraint() {
		this.blacklistedValues = null;
	}

	@Override
	public boolean validate(IJsonNode value) {
		return !this.blacklistedValues.contains(value);
	}

}
