package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;

import java.io.IOException;

import eu.stratosphere.sopremo.AbstractSopremoType;

public class MappingValueCorrespondence extends AbstractSopremoType {
	private SpicyPathExpression sourcePath;

	private SpicyPathExpression targetPath;

	MappingValueCorrespondence() {
	}

	public MappingValueCorrespondence(SpicyPathExpression sourcePath,
			SpicyPathExpression targetPath) {
		this.sourcePath = sourcePath;
		this.targetPath = targetPath;
	}

	public ValueCorrespondence generateSpicyType() {
		return new ValueCorrespondence(this.sourcePath.getPathExpression(), this.targetPath.getPathExpression());
	}

	public SpicyPathExpression getSourcePath() {
		return this.sourcePath;
	}

	public SpicyPathExpression getTargetPath() {
		return this.targetPath;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("MappingValueCorrespondence [");
		if (this.sourcePath != null) {
			appendable.append("sourcePath=");
			this.sourcePath.appendAsString(appendable);
			appendable.append(", ");
		}
		if (this.targetPath != null) {
			appendable.append("targetPath=");
			this.targetPath.appendAsString(appendable);
		}
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.sourcePath.hashCode();
		result = prime * result + this.targetPath.hashCode();
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
		MappingValueCorrespondence other = (MappingValueCorrespondence) obj;
		return this.sourcePath.equals(other.sourcePath) && this.targetPath.equals(other.targetPath);
	}

}
