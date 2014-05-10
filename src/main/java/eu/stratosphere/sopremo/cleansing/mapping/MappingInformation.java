package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;

/**
 * This class holds all relevant mapping information defined in a meteor script
 * to create spicy specific data objects of them inside the {@link SpicyMappingTransformation}
 * 
 * @author Fabian Tschirschnitz
 */

public class MappingInformation extends AbstractSopremoType {
	private List<ComparativeExpression> sourceJoinConditions = new ArrayList<ComparativeExpression>();

	private List<ComparativeExpression> targetJoinConditions = new ArrayList<ComparativeExpression>();

	private MappingSchema sourceSchema = new MappingSchema();

	private MappingDataSource target = new MappingDataSource();

	private Set<MappingValueCorrespondence> valueCorrespondences = new HashSet<MappingValueCorrespondence>();

	MappingInformation() {

	}

	public List<ComparativeExpression> getSourceJoinConditions() {
		return this.sourceJoinConditions;
	}

	public void setSourceJoinCondition(final List<ComparativeExpression> sourceJoinCondition) {
		this.sourceJoinConditions = sourceJoinCondition;
	}

	public MappingSchema getSourceSchema() {
		return this.sourceSchema;
	}

	public void setSourceSchema(final MappingSchema sourceSchema) {
		this.sourceSchema = sourceSchema;
	}

	public MappingDataSource getTarget() {
		return this.target;
	}

	public void setTarget(final MappingDataSource target) {
		this.target = target;
	}

	public List<ComparativeExpression> getTargetJoinConditions() {
		return this.targetJoinConditions;
	}

	public void setTargetJoinConditions(
			final List<ComparativeExpression> targetJoinConditions) {
		this.targetJoinConditions = targetJoinConditions;
	}

	public Set<MappingValueCorrespondence> getValueCorrespondences() {
		return this.valueCorrespondences;
	}

	public List<ValueCorrespondence> getValueCorrespondencesAsSpicyTypes() {
		final List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();
		for (final MappingValueCorrespondence mvc : this.getValueCorrespondences())
			valueCorrespondences.add(mvc.generateSpicyType());
		return valueCorrespondences;
	}

	public void setValueCorrespondences(
			final Set<MappingValueCorrespondence> valueCorrespondences) {
		this.valueCorrespondences = valueCorrespondences;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		appendable.append("MappingInformation [sourceJoinCondition=");
		this.append(appendable, this.sourceJoinConditions, ",");
		appendable.append(", targetJoinConditions=");
		this.append(appendable, this.targetJoinConditions, ",");
		appendable.append(", sourceSchema=");
		this.sourceSchema.appendAsString(appendable);
		appendable.append(", target=");
		this.target.appendAsString(appendable);
		appendable.append(", valueCorrespondences=");
		this.append(appendable, this.valueCorrespondences, ",");
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.sourceJoinConditions == null ? 0 : this.sourceJoinConditions.hashCode());
		result = prime * result + (this.sourceSchema == null ? 0 : this.sourceSchema.hashCode());
		result = prime * result + (this.target == null ? 0 : this.target.hashCode());
		result = prime * result + (this.targetJoinConditions == null ? 0 : this.targetJoinConditions.hashCode());
		result = prime * result + (this.valueCorrespondences == null ? 0 : this.valueCorrespondences.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		final MappingInformation other = (MappingInformation) obj;
		if (this.sourceJoinConditions == null) {
			if (other.sourceJoinConditions != null)
				return false;
		} else if (!this.sourceJoinConditions.equals(other.sourceJoinConditions))
			return false;
		if (this.sourceSchema == null) {
			if (other.sourceSchema != null)
				return false;
		} else if (!this.sourceSchema.equals(other.sourceSchema))
			return false;
		if (this.target == null) {
			if (other.target != null)
				return false;
		} else if (!this.target.equals(other.target))
			return false;
		if (this.targetJoinConditions == null) {
			if (other.targetJoinConditions != null)
				return false;
		} else if (!this.targetJoinConditions.equals(other.targetJoinConditions))
			return false;
		if (this.valueCorrespondences == null) {
			if (other.valueCorrespondences != null)
				return false;
		} else if (!this.valueCorrespondences.equals(other.valueCorrespondences))
			return false;
		return true;
	}

}
