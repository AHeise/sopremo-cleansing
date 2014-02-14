package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.paths.PathExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class MappingValueCorrespondence extends AbstractSopremoType {
	private List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();

	private SpicyPathExpression targetPath;

	private EvaluationExpression expr;
	
	private boolean takeAllValuesOfGrouping;

	MappingValueCorrespondence() {
	}

	public MappingValueCorrespondence(SpicyPathExpression sourcePath, SpicyPathExpression targetPath) {
		this.sourcePaths.add(sourcePath);
		this.targetPath = targetPath;
	}

	public MappingValueCorrespondence(List<SpicyPathExpression> sourcePaths, SpicyPathExpression targetPath, EvaluationExpression expr) {
		this.sourcePaths = sourcePaths;
		this.targetPath = targetPath;
		this.expr = expr;
	}

	public ValueCorrespondence generateSpicyType() {
		List<PathExpression> spicyTypeSourcesPathes = new ArrayList<PathExpression>();
		for (SpicyPathExpression spe : this.sourcePaths) {
			spicyTypeSourcesPathes.add(spe.getPathExpression());
		}
		if (this.expr == null) {
			return new ValueCorrespondence(spicyTypeSourcesPathes.get(0), this.targetPath.getPathExpression());
		} else {
			return new ValueCorrespondence(spicyTypeSourcesPathes, this.targetPath.getPathExpression(), new SopremoFunctionExpression(this.expr));
		}
	}

	public List<SpicyPathExpression> getSourcePaths() {
		return this.sourcePaths;
	}

	public SpicyPathExpression getTargetPath() {
		return this.targetPath;
	}

	public EvaluationExpression getExpression() {
		return expr;
	}
	
	public void setTakeAllValuesOfGrouping(boolean takeAllValuesOfGrouping) {
		this.takeAllValuesOfGrouping = takeAllValuesOfGrouping;
	}

	public boolean isTakeAllValuesOfGrouping() {
		return takeAllValuesOfGrouping;
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
		if (this.expr != null) {
			appendable.append(", function=");
			this.expr.appendAsString(appendable);
		}
		appendable.append(", takeAllValuesOfGrouping="+takeAllValuesOfGrouping);
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((expr == null) ? 0 : expr.hashCode());
		result = prime * result + ((sourcePaths == null) ? 0 : sourcePaths.hashCode());
		result = prime * result + (takeAllValuesOfGrouping ? 1231 : 1237);
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
		if (expr == null) {
			if (other.expr != null)
				return false;
		} else if (!expr.equals(other.expr))
			return false;
		if (sourcePaths == null) {
			if (other.sourcePaths != null)
				return false;
		} else if (!sourcePaths.equals(other.sourcePaths))
			return false;
		if (takeAllValuesOfGrouping != other.takeAllValuesOfGrouping)
			return false;
		if (targetPath == null) {
			if (other.targetPath != null)
				return false;
		} else if (!targetPath.equals(other.targetPath))
			return false;
		return true;
	}

}
