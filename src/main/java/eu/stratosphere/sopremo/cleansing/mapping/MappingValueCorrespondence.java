package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.paths.PathExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class MappingValueCorrespondence extends AbstractSopremoType {
	private List<EvaluationExpression> sourcePaths = new ArrayList<EvaluationExpression>();

	private EvaluationExpression targetPath;

	private EvaluationExpression expr;

	private boolean takeAllValuesOfGrouping;

	MappingValueCorrespondence() {
	}

	public MappingValueCorrespondence(final EvaluationExpression sourcePath, final EvaluationExpression targetPath) {
		this.sourcePaths.add(sourcePath);
		this.targetPath = targetPath;
	}

	public MappingValueCorrespondence(final List<EvaluationExpression> sourcePaths,
			final EvaluationExpression targetPath, final EvaluationExpression expr) {
		this.sourcePaths = sourcePaths;
		this.targetPath = targetPath;
		this.expr = expr;
	}

	public ValueCorrespondence generateSpicyType() {
		final List<PathExpression> spicyTypeSourcesPathes = new ArrayList<PathExpression>();
		for (final EvaluationExpression spe : this.sourcePaths)
			spicyTypeSourcesPathes.add(SpicyUtil.toSpicy(spe));
		if (this.expr == null)
			return new ValueCorrespondence(spicyTypeSourcesPathes.get(0), SpicyUtil.toSpicy(this.targetPath));
		return new ValueCorrespondence(spicyTypeSourcesPathes, SpicyUtil.toSpicy(this.targetPath),
			new SopremoFunctionExpression(this.expr));
	}

	public List<EvaluationExpression> getSourcePaths() {
		return this.sourcePaths;
	}

	public EvaluationExpression getTargetPath() {
		return this.targetPath;
	}

	public EvaluationExpression getExpression() {
		return this.expr;
	}

	public void setTakeAllValuesOfGrouping(final boolean takeAllValuesOfGrouping) {
		this.takeAllValuesOfGrouping = takeAllValuesOfGrouping;
	}

	public boolean isTakeAllValuesOfGrouping() {
		return this.takeAllValuesOfGrouping;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.IAppending#appendAsString(java.lang.Appendable)
	 */
	@Override
	public void appendAsString(final Appendable appendable) throws IOException {
		appendable.append("MappingValueCorrespondence [");
		if (this.sourcePaths != null) {
			appendable.append("sourcePath=");
			for (final EvaluationExpression spe : this.sourcePaths) {
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
		appendable.append(", takeAllValuesOfGrouping=" + this.takeAllValuesOfGrouping);
		appendable.append("]");
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.expr == null ? 0 : this.expr.hashCode());
		result = prime * result + (this.sourcePaths == null ? 0 : this.sourcePaths.hashCode());
		result = prime * result + (this.takeAllValuesOfGrouping ? 1231 : 1237);
		result = prime * result + (this.targetPath == null ? 0 : this.targetPath.hashCode());
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
		final MappingValueCorrespondence other = (MappingValueCorrespondence) obj;
		if (this.expr == null) {
			if (other.expr != null)
				return false;
		} else if (!this.expr.equals(other.expr))
			return false;
		if (this.sourcePaths == null) {
			if (other.sourcePaths != null)
				return false;
		} else if (!this.sourcePaths.equals(other.sourcePaths))
			return false;
		if (this.takeAllValuesOfGrouping != other.takeAllValuesOfGrouping)
			return false;
		if (this.targetPath == null) {
			if (other.targetPath != null)
				return false;
		} else if (!this.targetPath.equals(other.targetPath))
			return false;
		return true;
	}

}
