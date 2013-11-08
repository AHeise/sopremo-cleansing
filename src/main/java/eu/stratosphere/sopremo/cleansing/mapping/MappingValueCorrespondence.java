package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.correspondence.ValueCorrespondence;

import java.util.List;

public class MappingValueCorrespondence {
	private List<String> sourcePath;
	private List<String> targetPath;

	MappingValueCorrespondence() {
	}

	public MappingValueCorrespondence(List<String> sourcePath,
			List<String> targetPath) {
		this.sourcePath = sourcePath;
		this.targetPath = targetPath;
	}

	public ValueCorrespondence generateSpicyType() {
		return new ValueCorrespondence(
				MappingUtil.createPathExpression(this.sourcePath),
				MappingUtil.createPathExpression(this.targetPath));
	}

	public List<String> getSourcePath() {
		return sourcePath;
	}

	public List<String> getTargetPath() {
		return targetPath;
	}
}
