package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.paths.PathExpression;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;

public class MappingValueCorrespondence extends AbstractSopremoType {
	private List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();

	private SpicyPathExpression targetPath;
	
	private String expression;

	MappingValueCorrespondence() {
	}

	public MappingValueCorrespondence(SpicyPathExpression sourcePath,
			SpicyPathExpression targetPath) {
		this.sourcePaths.add(sourcePath);
		this.targetPath = targetPath;
	}
	
	public MappingValueCorrespondence(List<SpicyPathExpression> sourcePath,
			SpicyPathExpression targetPath, String transformationFunctionExpression) {
		this.sourcePaths = sourcePath;
		this.targetPath = targetPath;
		this.expression = transformationFunctionExpression;
	}

	public ValueCorrespondence generateSpicyType() {
		if(this.sourcePaths.size()==1){
			return new ValueCorrespondence(this.sourcePaths.get(0).getPathExpression(), this.targetPath.getPathExpression());
		} else{
			List<PathExpression> spicyTypeSourcesPathes = new ArrayList<PathExpression>();
			for(SpicyPathExpression spe : this.sourcePaths){
				spicyTypeSourcesPathes.add(spe.getPathExpression());
			}
			return new ValueCorrespondence(spicyTypeSourcesPathes, this.targetPath.getPathExpression(), new Expression(this.expression));
		}
		
	}

	public List<SpicyPathExpression> getSourcePaths() {
		return this.sourcePaths;
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
		if (this.sourcePaths != null) {
			appendable.append("sourcePath=");
			for(SpicyPathExpression spe : this.sourcePaths){
				spe.appendAsString(appendable);
			}
			
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
		result = prime * result + this.sourcePaths.hashCode();
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
		return this.sourcePaths.equals(other.sourcePaths) && this.targetPath.equals(other.targetPath);
	}

}
