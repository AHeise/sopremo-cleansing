package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

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

	private Set<MappingValueCorrespondence> valueCorrespondences = new HashSet<MappingValueCorrespondence>();

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

	public Set<MappingValueCorrespondence> getValueCorrespondences() {
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
			Set<MappingValueCorrespondence> valueCorrespondences) {
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
		result = prime * result + ((sourceJoinCondition == null) ? 0 : sourceJoinCondition.hashCode());
		result = prime * result + ((sourceSchema == null) ? 0 : sourceSchema.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
		result = prime * result + ((targetJoinConditions == null) ? 0 : targetJoinConditions.hashCode());
		result = prime * result + ((valueCorrespondences == null) ? 0 : valueCorrespondences.hashCode());
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
		if (sourceJoinCondition == null) {
			if (other.sourceJoinCondition != null)
				return false;
		} else if (!sourceJoinCondition.equals(other.sourceJoinCondition))
			return false;
		if (sourceSchema == null) {
			if (other.sourceSchema != null)
				return false;
		} else if (!sourceSchema.equals(other.sourceSchema))
			return false;
		if (target == null) {
			if (other.target != null)
				return false;
		} else if (!target.equals(other.target))
			return false;
		if (targetJoinConditions == null) {
			if (other.targetJoinConditions != null)
				return false;
		} else if (!targetJoinConditions.equals(other.targetJoinConditions))
			return false;
		if (valueCorrespondences == null) {
			if (other.valueCorrespondences != null)
				return false;
		} else if (!valueCorrespondences.equals(other.valueCorrespondences))
			return false;
		return true;
	}
	
}
