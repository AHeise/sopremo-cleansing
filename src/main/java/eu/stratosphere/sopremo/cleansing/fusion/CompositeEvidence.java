package eu.stratosphere.sopremo.cleansing.fusion;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.type.DecimalNode;

public class CompositeEvidence extends AbstractSopremoType implements Iterable<Entry<String, CompositeEvidence>> {

	private DecimalNode baseEvidence;
	private Map<String, CompositeEvidence> subEvidences;
	
	public CompositeEvidence(){
		
	}

	public CompositeEvidence(DecimalNode baseEvidence) {
		this.baseEvidence = baseEvidence;
		this.subEvidences = new HashMap<String, CompositeEvidence>();
	}

	public DecimalNode getBaseEvidence() {
		return baseEvidence;
	}

	public void setBaseEvidence(DecimalNode baseEvidence) {
		this.baseEvidence = baseEvidence;
	}

	public CompositeEvidence getEvidence(String fieldName) {
		return this.subEvidences.get(fieldName);
	}

	public void putEvidence(String fieldName, CompositeEvidence evidence) {
		this.subEvidences.put(fieldName, evidence);
	}

	@Override
	public Iterator<Entry<String, CompositeEvidence>> iterator() {
		return this.subEvidences.entrySet().iterator();
	}

	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("{ baseEvidence: ").append(this.baseEvidence.toString()).append(", subEvidences: ").append(this.subEvidences.toString()).append(" }");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((baseEvidence == null) ? 0 : baseEvidence.hashCode());
		result = prime * result + ((subEvidences == null) ? 0 : subEvidences.hashCode());
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
		CompositeEvidence other = (CompositeEvidence) obj;
		if (baseEvidence == null) {
			if (other.baseEvidence != null)
				return false;
		} else if (!baseEvidence.equals(other.baseEvidence))
			return false;
		if (subEvidences == null) {
			if (other.subEvidences != null)
				return false;
		} else if (!subEvidences.equals(other.subEvidences))
			return false;
		return true;
	}

}
