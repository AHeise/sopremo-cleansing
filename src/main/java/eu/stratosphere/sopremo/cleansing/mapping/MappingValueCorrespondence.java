package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.paths.PathExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.FunctionCall;

public class MappingValueCorrespondence extends AbstractSopremoType {
	private List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();

	private SpicyPathExpression targetPath;

	private FunctionCall function;

	MappingValueCorrespondence() {
	}

	public MappingValueCorrespondence(SpicyPathExpression sourcePath, SpicyPathExpression targetPath) {
		this.sourcePaths.add(sourcePath);
		this.targetPath = targetPath;
	}

	public MappingValueCorrespondence(List<SpicyPathExpression> sourcePaths, SpicyPathExpression targetPath, FunctionCall function) {
		this.sourcePaths = sourcePaths;
		this.targetPath = targetPath;
		this.function = function;
	}

	public ValueCorrespondence generateSpicyType() {
		List<PathExpression> spicyTypeSourcesPathes = new ArrayList<PathExpression>();
		for (SpicyPathExpression spe : this.sourcePaths) {
			spicyTypeSourcesPathes.add(spe.getPathExpression());
		}
		if (this.function == null) {
			return new ValueCorrespondence(spicyTypeSourcesPathes.get(0), this.targetPath.getPathExpression());
		} else {
			return new ValueCorrespondence(spicyTypeSourcesPathes, this.targetPath.getPathExpression(), new SopremoFunctionExpression(this.function));
		}
	}

	public List<SpicyPathExpression> getSourcePaths() {
		return this.sourcePaths;
	}

	public SpicyPathExpression getTargetPath() {
		return this.targetPath;
	}

	public FunctionCall getFunction() {
		return function;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		appendable.append("MappingValueCorrespondence [");
		if (this.sourcePaths != null) {
			appendable.append("sourcePath=");
			for (SpicyPathExpression spe : this.sourcePaths) {
				spe.appendAsString(appendable);
				appendable.append(", ");
			}

			appendable.append(", ");
		}
		if (this.targetPath != null) {
			appendable.append("targetPath=");
			this.targetPath.appendAsString(appendable);
		}
		if (this.function != null) {
			appendable.append(", function=");
			this.function.appendAsString(appendable);
		}
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((function == null) ? 0 : function.hashCode());
		result = prime * result + ((sourcePaths == null) ? 0 : sourcePaths.hashCode());
		result = prime * result + ((targetPath == null) ? 0 : targetPath.hashCode());
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
		if (function == null) {
			if (other.function != null)
				return false;
		} else if (!function.equals(other.function))
			return false;
		if (sourcePaths == null) {
			if (other.sourcePaths != null)
				return false;
		} else if (!sourcePaths.equals(other.sourcePaths))
			return false;
		if (targetPath == null) {
			if (other.targetPath != null)
				return false;
		} else if (!targetPath.equals(other.targetPath))
			return false;
		return true;
	}

}
