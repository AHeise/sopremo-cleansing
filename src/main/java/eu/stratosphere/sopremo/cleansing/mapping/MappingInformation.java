package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;

import java.util.ArrayList;
import java.util.List;

/**
 * This class holds all relevant mapping information defined in a meteor script
 * to create spicy specific data objects of them inside the
 * {@link SpicyMappingTransformation}
 * 
 * @author Fabian Tschirschnitz
 * 
 */

public class MappingInformation {
	private MappingJoinCondition sourceJoinCondition;
	private List<MappingJoinCondition> targetJoinConditions = new ArrayList<MappingJoinCondition>();

	private MappingSchema sourceSchema;

	private MappingDataSource target = new MappingDataSource();

	private List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();
	
	MappingInformation(){
		
	}

	public MappingJoinCondition getSourceJoinCondition() {
		return sourceJoinCondition;
	}

	public void setSourceJoinCondition(MappingJoinCondition sourceJoinCondition) {
		this.sourceJoinCondition = sourceJoinCondition;
	}

	public MappingSchema getSourceSchema() {
		return sourceSchema;
	}

	public void setSourceSchema(MappingSchema sourceSchema) {
		this.sourceSchema = sourceSchema;
	}

	public MappingDataSource getTarget() {
		return target;
	}

	public void setTarget(MappingDataSource target) {
		this.target = target;
	}

	public List<MappingJoinCondition> getTargetJoinConditions() {
		return targetJoinConditions;
	}

	public void setTargetJoinConditions(
			List<MappingJoinCondition> targetJoinConditions) {
		this.targetJoinConditions = targetJoinConditions;
	}

	public List<MappingValueCorrespondence> getValueCorrespondences() {
		return valueCorrespondences;
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
}
