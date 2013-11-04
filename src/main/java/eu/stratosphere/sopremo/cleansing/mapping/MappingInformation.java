package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.datasource.DataSource;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.SequenceNode;

import java.util.ArrayList;
import java.util.List;

/**
 * This class holds all relevant mapping information defined in a meteor script to create spicy specific data objects of them inside the {@link SpicyMappingTransformation}
 * @author Fabian Tschirschnitz
 *
 */


public class MappingInformation {
	private MappingJoinCondition sourceJoinCondition;
	private List<MappingJoinCondition> targetJoinConditions = new ArrayList<MappingJoinCondition>();
	
	private INode sourceSchema = new SequenceNode(EntityMapping.sourceStr);
	private INode targetSchema = new SequenceNode(EntityMapping.targetStr);

	private INode sourceEntity;
	private INode targetEntity;
	
	private DataSource target = new DataSource(EntityMapping.type, targetSchema);
	
	private List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();

	public MappingJoinCondition getSourceJoinCondition() {
		return sourceJoinCondition;
	}

	public void setSourceJoinCondition(MappingJoinCondition sourceJoinCondition) {
		this.sourceJoinCondition = sourceJoinCondition;
	}

	public INode getSourceSchema() {
		return sourceSchema;
	}

	public void setSourceSchema(INode sourceSchema) {
		this.sourceSchema = sourceSchema;
	}

	public INode getTargetSchema() {
		return targetSchema;
	}

	public void setTargetSchema(INode targetSchema) {
		this.targetSchema = targetSchema;
	}

	public INode getSourceEntity() {
		return sourceEntity;
	}

	public void setSourceEntity(INode sourceEntity) {
		this.sourceEntity = sourceEntity;
	}

	public INode getTargetEntity() {
		return targetEntity;
	}

	public void setTargetEntity(INode targetEntity) {
		this.targetEntity = targetEntity;
	}

	public DataSource getTarget() {
		return target;
	}

	public void setTarget(DataSource target) {
		this.target = target;
	}

	public List<MappingJoinCondition> getTargetJoinConditions() {
		return targetJoinConditions;
	}

	public void setTargetJoinConditions(List<MappingJoinCondition> targetJoinConditions) {
		this.targetJoinConditions = targetJoinConditions;
	}

	public List<ValueCorrespondence> getValueCorrespondences() {
		return valueCorrespondences;
	}

	public void setValueCorrespondences(List<ValueCorrespondence> valueCorrespondences) {
		this.valueCorrespondences = valueCorrespondences;
	}
}
