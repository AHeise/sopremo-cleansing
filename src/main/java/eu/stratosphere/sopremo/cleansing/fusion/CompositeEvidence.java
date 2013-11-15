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

	public CompositeEvidence() {

	}

	public CompositeEvidence(final DecimalNode baseEvidence) {
		this.baseEvidence = baseEvidence;
		this.subEvidences = new HashMap<String, CompositeEvidence>();
	}

	public DecimalNode getBaseEvidence() {
		return this.baseEvidence;
	}

	public void setBaseEvidence(final DecimalNode baseEvidence) {
		this.baseEvidence = baseEvidence;
	}

	public CompositeEvidence getEvidence(final String fieldName) {
		return this.subEvidences.get(fieldName);
	}

	public void putEvidence(final String fieldName, final CompositeEvidence evidence) {
		this.subEvidences.put(fieldName, evidence);
	}

	@Override
	public Iterator<Entry<String, CompositeEvidence>> iterator() {
		return this.subEvidences.entrySet().iterator();
	}

	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		appendable.append("{ baseEvidence: ").append(this.baseEvidence.toString()).append(", subEvidences: ").append(
			this.subEvidences.toString()).append(" }");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.baseEvidence == null ? 0 : this.baseEvidence.hashCode());
		result = prime * result + (this.subEvidences == null ? 0 : this.subEvidences.hashCode());
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
		final CompositeEvidence other = (CompositeEvidence) obj;
		if (this.baseEvidence == null) {
			if (other.baseEvidence != null)
				return false;
		} else if (!this.baseEvidence.equals(other.baseEvidence))
			return false;
		if (this.subEvidences == null) {
			if (other.subEvidences != null)
				return false;
		} else if (!this.subEvidences.equals(other.subEvidences))
			return false;
		return true;
	}

}
