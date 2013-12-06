package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;

/**
 * This class holds all relevant mapping information defined in a meteor script
 * to create spicy specific data objects of them inside the {@link SpicyMappingTransformation}
 * 
 * @author Fabian Tschirschnitz
 */

public class MappingInformation extends AbstractSopremoType {
	private MappingJoinCondition sourceJoinCondition;

	private List<MappingJoinCondition> targetJoinConditions = new ArrayList<MappingJoinCondition>();

	private MappingSchema sourceSchema = new MappingSchema();

	private MappingDataSource target = new MappingDataSource();

	private List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();

	MappingInformation() {

	}

	public MappingJoinCondition getSourceJoinCondition() {
		return this.sourceJoinCondition;
	}

	public void setSourceJoinCondition(MappingJoinCondition sourceJoinCondition) {
		this.sourceJoinCondition = sourceJoinCondition;
	}

	public MappingSchema getSourceSchema() {
		return this.sourceSchema;
	}

	public void setSourceSchema(MappingSchema sourceSchema) {
		this.sourceSchema = sourceSchema;
	}

	public MappingDataSource getTarget() {
		return this.target;
	}

	public void setTarget(MappingDataSource target) {
		this.target = target;
	}

	public List<MappingJoinCondition> getTargetJoinConditions() {
		return this.targetJoinConditions;
	}

	public void setTargetJoinConditions(
			List<MappingJoinCondition> targetJoinConditions) {
		this.targetJoinConditions = targetJoinConditions;
	}

	public List<MappingValueCorrespondence> getValueCorrespondences() {
		return this.valueCorrespondences;
	}

	public List<ValueCorrespondence> getValueCorrespondencesAsSpicyTypes() {
		List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();
		for (MappingValueCorrespondence mvc : this.getValueCorrespondences()) {
			valueCorrespondences.add(mvc.generateSpicyType());
		}
		return valueCorrespondences;
	}

	public void setValueCorrespondences(
			List<MappingValueCorrespondence> valueCorrespondences) {
		this.valueCorrespondences = valueCorrespondences;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("MappingInformation [sourceJoinCondition=");
		if (this.sourceJoinCondition != null)
			this.sourceJoinCondition.appendAsString(appendable);
		else
			appendable.append(null);
		appendable.append(", targetJoinConditions=");
		append(appendable, this.targetJoinConditions, ",");
		appendable.append(", sourceSchema=");
		this.sourceSchema.appendAsString(appendable);
		appendable.append(", target=");
		this.target.appendAsString(appendable);
		appendable.append(", valueCorrespondences=");
		append(appendable, this.valueCorrespondences, ",");
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.sourceJoinCondition == null) ? 0 : this.sourceJoinCondition.hashCode());
		result = prime * result + ((this.sourceSchema == null) ? 0 : this.sourceSchema.hashCode());
		result = prime * result + ((this.target == null) ? 0 : this.target.hashCode());
		result = prime * result + ((this.targetJoinConditions == null) ? 0 : this.targetJoinConditions.hashCode());
		result = prime * result + ((this.valueCorrespondences == null) ? 0 : this.valueCorrespondences.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MappingInformation other = (MappingInformation) obj;
		if (this.sourceJoinCondition == null) {
			if (other.sourceJoinCondition != null)
				return false;
		} else if (!this.sourceJoinCondition.equals(other.sourceJoinCondition))
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
