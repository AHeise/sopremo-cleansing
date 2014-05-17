package eu.stratosphere.sopremo.cleansing.mapping;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.expressions.*;
import eu.stratosphere.sopremo.expressions.ObjectCreation.SymbolicAssignment;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.type.JsonUtil;

class SpicyMappingTransformationFactory {

	boolean createConcat = false;

	boolean createNesting = false;

	boolean createSubstring = false;

	boolean createSum = false;

	boolean createJoinWithConcat = false;

	boolean createTargetJoinSwitch = false;

	boolean createSourceJoinSwitch = false;

	public boolean isCreateNesting() {
		return this.createNesting;
	}

	public void setCreateJoinWithConcat(boolean createJoinWithConcat) {
		this.createJoinWithConcat = createJoinWithConcat;
	}

	public void setCreateNesting(boolean createNesting) {
		this.createNesting = createNesting;
	}

	public void setCreateSubstring(boolean createSubstring) {
		this.createSubstring = createSubstring;
	}

	public boolean isCreateSubstring() {
		return this.createSubstring;
	}

	public boolean isCreateConcat() {
		return this.createConcat;
	}

	public void setCreateConcat(boolean createConcat) {
		this.createConcat = createConcat;
	}

	public boolean isCreateSum() {
		return this.createSum;
	}

	public void setCreateSum(boolean createSum) {
		this.createSum = createSum;
	}

	public void setCreateTargetJoinSwitch(boolean createTargetJoinSwitch) {
		this.createTargetJoinSwitch = createTargetJoinSwitch;
	}

	public void setCreateSourceJoinSwitch(boolean createSourceJoinSwitch) {
		this.createSourceJoinSwitch = createSourceJoinSwitch;
	}

	public SpicyMappingTransformation create() {
		SpicyMappingTransformation transformation = new SpicyMappingTransformation();
		// force two inputs and two outputs
		transformation.setInputs(null, null);
		transformation.getOutput(1);

		// ### soure and target
		transformation.setSourceSchema(createSourceSchema());
		transformation.setTargetSchema(this.createNesting ? createNestedTargetSchema() : createTargetSchema());

		// ### key contraints
		List<PathSegmentExpression> sourcePKs = new ArrayList<PathSegmentExpression>();
		sourcePKs.add(JsonUtil.createPath("0", "id"));
		sourcePKs.add(JsonUtil.createPath("1", "biographyId"));
		transformation.setSourceKeys(sourcePKs);

		List<PathSegmentExpression> targetPKs = new ArrayList<PathSegmentExpression>();
		targetPKs.add(JsonUtil.createPath("0", "id"));
		targetPKs.add(JsonUtil.createPath("1", "id"));
		transformation.setTargetKeys(targetPKs);

		// ### foreign key contraints
		Set<SymbolicAssignment> sourceFKs = new HashSet<ObjectCreation.SymbolicAssignment>();
		if (this.createSourceJoinSwitch)
			sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "biography"),
				JsonUtil.createPath("1", "biographyId")));
		else
			sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("1", "biographyId"),
				JsonUtil.createPath("0", "biography")));
		transformation.setTargetFKs(sourceFKs);

		Set<SymbolicAssignment> targetFKs = new HashSet<ObjectCreation.SymbolicAssignment>();
		if (this.createTargetJoinSwitch)
			targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor"), JsonUtil.createPath("1", "id")));
		else
			targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("1", "id"), JsonUtil.createPath("0", "worksFor")));
		transformation.setTargetFKs(targetFKs);

		// ### value correspondences
		transformation.setSourceToValueCorrespondences(createValueCorrespondences());

		return transformation;
	}

	private List<ObjectCreation> createSourceSchema() {
		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("name", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("biography", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("incomes", new ArrayCreation());
		sourceSchema.get(1).addMapping("biographyId", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor", EvaluationExpression.VALUE);
		return sourceSchema;
	}

	private List<ObjectCreation> createTargetSchema() {
		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("name", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("income", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name", EvaluationExpression.VALUE);
		return targetSchema;
	}

	private List<ObjectCreation> createNestedTargetSchema() {
		final ObjectCreation nestedName = new ObjectCreation();
		nestedName.addMapping("fullName", EvaluationExpression.VALUE);

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("nestedName", nestedName);
		targetSchema.get(0).addMapping("worksFor", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("income", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name", EvaluationExpression.VALUE);
		return targetSchema;
	}

	private Set<SymbolicAssignment> createValueCorrespondences() {
		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();

		// draw 5 arrows
		if (!this.createJoinWithConcat) {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "id"),
				JsonUtil.createPath("1", "worksFor")));
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor"),
				JsonUtil.createPath("1", "worksFor")));
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name"),
				JsonUtil.createPath("1", "worksFor")));
		} else {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "id"),
				createConcat(JsonUtil.createPath("1", "worksFor"))));
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor"),
				createConcat(JsonUtil.createPath("1", "worksFor"))));
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name"),
				JsonUtil.createPath("1", "worksFor")));
		}

		if (this.createNesting) {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "fullName", "nestedName"),
				JsonUtil.createPath("0", "name")));
		} else if (this.createSubstring) {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name"),
				FunctionUtil.createFunctionCall(CoreFunctions.SUBSTRING, JsonUtil.createPath("0", "name"),
					new ConstantExpression(2))));
		} else {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name"),
				JsonUtil.createPath("0", "name")));
		}

		if (this.createConcat) {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "id"),
				FunctionUtil.createFunctionCall(CoreFunctions.CONCAT, JsonUtil.createPath("0", "id"),
					new ConstantExpression("---"), JsonUtil.createPath("0", "name"))));
		} else {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "id"),
				JsonUtil.createPath("0", "id")));
		}

		if (this.createSum) {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "income"),
				FunctionUtil.createFunctionCall(CoreFunctions.SUM, JsonUtil.createPath("0", "name"))));
		} else {
			valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "income"),
				JsonUtil.createPath("0", "incomes")));
		}

		return valueCorrespondences;
	}

	private EvaluationExpression createConcat(PathSegmentExpression sourcePath) {
		return FunctionUtil.createFunctionCall(CoreFunctions.CONCAT, sourcePath, new ConstantExpression("---"));
	}

}