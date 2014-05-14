package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.algebra.IAlgebraOperator;
import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.datasource.*;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.datasource.nodes.TupleNode;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.PathExpression;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.pact.SopremoUtil;

public class SpicyMappingFactory {

	boolean createConcat = false;
	boolean createNesting = false;
	boolean createSubstring = false;
	boolean createSum = false;
	boolean createJoinWithConcat = false;
	boolean createTargetJoinSwitch = false;
	boolean createSourceJoinSwitch = false;

	boolean targetJoinMandatory = true;
	boolean sourceJoinMandatory = true;

	public static void main(String[] args) {
		SpicyMappingFactory factory = new SpicyMappingFactory();
		factory.setCreateConcat(true);
		factory.create();
	}

	public boolean isCreateNesting() {
		return createNesting;
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
		return createConcat;
	}

	public void setCreateConcat(boolean createConcat) {
		this.createConcat = createConcat;
	}

	public boolean isCreateSum() {
		return createSum;
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

	public MappingTask create() {

		MappingTask task = createMappingTaskFromMeteorScript();
		// System.out.println(task);

		task.getMappingData();

		IAlgebraOperator tree = task.getMappingData().getAlgebraTree();
		SopremoUtil.LOG.debug("Generated Spicy Tree:\n" + tree);

		return task;
	}

	private MappingTask createMappingTaskFromMeteorScript() {

		// ### soure and target

		INode sourceSchema = createSourceSchema();
		INode targetSchema = createNesting ? createNestedTargetSchema() : createTargetSchema();
		String type = null; // "XML";

		DataSource source = new DataSource(type, sourceSchema);
		DataSource target = new DataSource(type, targetSchema);

		// ### key contraints

		// KeyConstraint sourceKeyConstraint1 =
		// createKeyConstraints("usCongress.usCongressBiographies.usCongressBiography.biographyId");
		// KeyConstraint sourceKeyConstraint2 =
		// createKeyConstraints("usCongress.usCongressMembers.usCongressMember.id");
		// source.addKeyConstraint(sourceKeyConstraint1);
		// source.addKeyConstraint(sourceKeyConstraint2);

		KeyConstraint targetKeyConstraint1 = createKeyConstraints("usCongress.persons.person.id");
		KeyConstraint targetKeyConstraint2 = createKeyConstraints("usCongress.legalEntities.legalEntity.id");
		target.addKeyConstraint(targetKeyConstraint1);
		target.addKeyConstraint(targetKeyConstraint2);

		// ### foreign key contraints //TODO waren nicht drin

		// ForeignKeyConstraint foreignKeyConstraint1 =
		// createForeignKeyConstraint("usCongress.usCongressMembers.usCongressMember.biography",
		// sourceKeyConstraint1);
		// source.addForeignKeyConstraint(foreignKeyConstraint1);
		//
		ForeignKeyConstraint foreignKeyConstraint2 = createForeignKeyConstraint("usCongress.persons.person.worksFor", targetKeyConstraint2);
		target.addForeignKeyConstraint(foreignKeyConstraint2);

		// ### value correspondences

		List<ValueCorrespondence> valueCorrespondences = createValueCorrespondences();

		// ### task

		MappingTask task = new MappingTask(source, target, valueCorrespondences);
		createJoinConditions(task);

		// System.out.println("###\n" + sourceSchema.toShortString());
		// System.out.println("###\n" + targetSchema.toShortString());
		// System.out.println("###\n" + valueCorrespondences);
		// System.out.println("###\n" + task);

		return task;
	}

	private void createJoinConditions(MappingTask task) {
		List<String> list1 = new ArrayList<String>();
		list1.add("usCongress.usCongressMembers.usCongressMember.biography");

		List<PathExpression> slp1 = new ArrayList<PathExpression>();
		slp1.add(new PathExpression(list1));

		List<String> list2 = new ArrayList<String>();
		list2.add("usCongress.usCongressBiographies.usCongressBiography.biographyId");

		List<PathExpression> slp2 = new ArrayList<PathExpression>();
		slp2.add(new PathExpression(list2));

		JoinCondition sourceJoinCondition;
		if (!createSourceJoinSwitch) {
			sourceJoinCondition = new JoinCondition(slp1, slp2, true);
		} else {
			sourceJoinCondition = new JoinCondition(slp2, slp1, true);
		}
		sourceJoinCondition.setMandatory(sourceJoinMandatory);
		sourceJoinCondition.setMonodirectional(true);

		List<String> list3 = new ArrayList<String>();
		list3.add("usCongress.persons.person.worksFor");

		List<PathExpression> tlp1 = new ArrayList<PathExpression>();
		tlp1.add(new PathExpression(list3));

		List<String> list4 = new ArrayList<String>();
		list4.add("usCongress.legalEntities.legalEntity.id");

		List<PathExpression> tlp2 = new ArrayList<PathExpression>();
		tlp2.add(new PathExpression(list4));

		JoinCondition targetJoinCondition;
		if (!createTargetJoinSwitch) {
			targetJoinCondition = new JoinCondition(tlp1, tlp2, true);
		} else {
			targetJoinCondition = new JoinCondition(tlp2, tlp1, true);
		}
		targetJoinCondition.setMandatory(targetJoinMandatory);
		targetJoinCondition.setMonodirectional(true);

		task.getSourceProxy().addJoinCondition(sourceJoinCondition);
		task.getTargetProxy().addJoinCondition(targetJoinCondition);

	}

	private INode createSourceSchema() {

		INode dummy = new LeafNode("dummy");

		// usCongressMembers

		INode id = new AttributeNode("id");
		id.addChild(dummy);
		INode name = new AttributeNode("name");
		name.addChild(dummy);
		INode biography = new AttributeNode("biography");
		biography.addChild(dummy);
		INode incomes = new AttributeNode("incomes");
		incomes.addChild(new SetNode("incomes"));

		INode usCongressMember = new TupleNode("usCongressMember");
		usCongressMember.addChild(id);
		usCongressMember.addChild(name);
		usCongressMember.addChild(incomes);
		usCongressMember.addChild(biography);

		INode usCongressMembers = new SetNode("usCongressMembers");
		usCongressMembers.addChild(usCongressMember);

		// usCongressBiographies

		INode biographyId = new AttributeNode("biographyId");
		biographyId.addChild(dummy);
		INode worksFor = new AttributeNode("worksFor");
		worksFor.addChild(dummy);

		INode usCongressBiography = new TupleNode("usCongressBiography");
		usCongressBiography.addChild(biographyId);
		usCongressBiography.addChild(worksFor);

		INode usCongressBiographies = new SetNode("usCongressBiographies");
		usCongressBiographies.addChild(usCongressBiography);

		INode usCongress = new TupleNode("usCongress");
		usCongress.addChild(usCongressMembers);
		usCongress.addChild(usCongressBiographies);
		usCongress.setRoot(true);

		return usCongress;
	}

	private INode createTargetSchema() {

		INode dummy = new LeafNode("stringDummy");

		// person

		INode id = new AttributeNode("id");
		id.addChild(dummy);

		INode name = new AttributeNode("name");
		name.addChild(dummy);

		INode worksFor = new AttributeNode("worksFor");
		worksFor.addChild(dummy);

		INode incomes = new AttributeNode("income");
		incomes.addChild(new LeafNode("intDummy"));

		INode person = new TupleNode("person");
		person.addChild(id);
		person.addChild(name);
		person.addChild(worksFor);
		person.addChild(incomes);

		INode persons = new SetNode("persons");
		persons.addChild(person);

		// legalEntity

		INode id2 = new AttributeNode("id");
		id2.addChild(dummy);
		id2.setRequired(true);
		INode name2 = new AttributeNode("name");
		name2.addChild(dummy);

		INode legalEntity = new TupleNode("legalEntity");
		legalEntity.addChild(id2);
		legalEntity.addChild(name2);

		INode legalEntities = new SetNode("legalEntities");
		legalEntities.addChild(legalEntity);

		INode usCongress = new TupleNode("usCongress");
		usCongress.addChild(persons);
		usCongress.addChild(legalEntities);
		usCongress.setRoot(true);

		return usCongress;
	}

	private INode createNestedTargetSchema() {

		INode dummy = new LeafNode("string");

		// person

		INode id = new AttributeNode("id");
		id.addChild(dummy);
		id.setRequired(true);

		INode name = new AttributeNode("nestedName"); // nesting
		name.addChild(dummy);
		INode fullName = new TupleNode("fullName");
		fullName.addChild(name);

		INode worksFor = new AttributeNode("worksFor");
		worksFor.addChild(dummy);

		INode income = new AttributeNode("income");
		income.addChild(new LeafNode("intDummy"));

		INode person = new TupleNode("person");
		person.addChild(id);
		person.addChild(fullName); // nesting
		person.addChild(worksFor);
		person.addChild(income);

		INode persons = new SetNode("persons");
		persons.addChild(person);

		// legalEntity

		INode id2 = new AttributeNode("id");
		id2.addChild(dummy);
		id2.setRequired(true);
		INode name2 = new AttributeNode("name");
		name2.addChild(dummy);

		INode legalEntity = new TupleNode("legalEntity");
		legalEntity.addChild(id2);
		legalEntity.addChild(name2);

		INode legalEntities = new SetNode("legalEntities");
		legalEntities.addChild(legalEntity);

		INode usCongress = new TupleNode("usCongress");
		usCongress.addChild(persons);
		usCongress.addChild(legalEntities);
		usCongress.setRoot(true);

		return usCongress;
	}

	private KeyConstraint createKeyConstraints(String str) {

		List<String> list = new ArrayList<String>();
		list.add(str);

		PathExpression key = new PathExpression(list);

		List<PathExpression> keyPath = new ArrayList<PathExpression>();
		keyPath.add(key);

		KeyConstraint keyConstraint = new KeyConstraint(keyPath, true); // TODO:
																		// do we
																		// need
																		// information
																		// if
																		// primary
																		// key?

		return keyConstraint;
	}

	private ForeignKeyConstraint createForeignKeyConstraint(String foreignKey, KeyConstraint key) {

		List<String> list = new ArrayList<String>();
		list.add(foreignKey);

		PathExpression path = new PathExpression(list);

		List<PathExpression> fk = new ArrayList<PathExpression>();
		fk.add(path);

		ForeignKeyConstraint foreignKeyConstraint = new ForeignKeyConstraint(key, fk);

		return foreignKeyConstraint;
	}

	private List<ValueCorrespondence> createValueCorrespondences() {

		// draw 5 arrows
		ValueCorrespondence wfCorrespondence1, wfCorrespondence2, wfCorrespondence3;
		if (!createJoinWithConcat) {
			wfCorrespondence1 = createValueCorrespondence("usCongress.usCongressBiographies.usCongressBiography.worksFor",
					"usCongress.legalEntities.legalEntity.id");
			wfCorrespondence2 = createValueCorrespondence("usCongress.usCongressBiographies.usCongressBiography.worksFor",
					"usCongress.legalEntities.legalEntity.name");
			wfCorrespondence3 = createValueCorrespondence("usCongress.usCongressBiographies.usCongressBiography.worksFor", "usCongress.persons.person.worksFor");
		} else {
			wfCorrespondence1 = createValueCorrespondenceWithConcats("usCongress.usCongressBiographies.usCongressBiography.worksFor",
					"usCongress.legalEntities.legalEntity.id");
			wfCorrespondence3 = createValueCorrespondenceWithConcats("usCongress.usCongressBiographies.usCongressBiography.worksFor",
					"usCongress.persons.person.worksFor");
			wfCorrespondence2 = createValueCorrespondence("usCongress.usCongressBiographies.usCongressBiography.worksFor",
					"usCongress.legalEntities.legalEntity.name");
		}

		ValueCorrespondence nameCorrespondence;
		if (createNesting) {
			nameCorrespondence = createValueCorrespondence("usCongress.usCongressMembers.usCongressMember.name",
					"usCongress.persons.person.fullName.nestedName");
		} else if (createSubstring) {
			nameCorrespondence = createValueCorrespondenceWithSubstring("usCongress.usCongressMembers.usCongressMember.name", "usCongress.persons.person.name");
		} else {
			nameCorrespondence = createValueCorrespondence("usCongress.usCongressMembers.usCongressMember.name", "usCongress.persons.person.name");
		}

		ValueCorrespondence idCorrespondence;
		
		if (createConcat) {
			idCorrespondence = createValueCorrespondenceWithConcats("usCongress.usCongressMembers.usCongressMember.id",
					"usCongress.usCongressMembers.usCongressMember.name", "usCongress.persons.person.id");
		}else{
			idCorrespondence = createValueCorrespondence("usCongress.usCongressMembers.usCongressMember.id", "usCongress.persons.person.id");
		}
		
		ValueCorrespondence incomeCorrespondence;
		if (createSum) {
			incomeCorrespondence = this.createValueCorrespondenceWithSum("usCongress.usCongressMembers.usCongressMember.incomes",
					"usCongress.persons.person.income");
		} else {
			incomeCorrespondence = this.createValueCorrespondence("usCongress.usCongressMembers.usCongressMember.incomes", "usCongress.persons.person.income");
		}

		List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();
		valueCorrespondences.add(wfCorrespondence1);
		valueCorrespondences.add(wfCorrespondence2);
		valueCorrespondences.add(wfCorrespondence3);
		valueCorrespondences.add(idCorrespondence);
		valueCorrespondences.add(nameCorrespondence);
		valueCorrespondences.add(incomeCorrespondence);

		return valueCorrespondences;
	}

	private ValueCorrespondence createValueCorrespondence(String str1, String str2) {

		List<String> sourcePathSteps = new ArrayList<String>();
		List<String> targetPathSteps = new ArrayList<String>();

		sourcePathSteps.add(str1);
		targetPathSteps.add(str2);

		PathExpression sourcePath = new PathExpression(sourcePathSteps);
		PathExpression targetPath = new PathExpression(targetPathSteps);

		ValueCorrespondence corr = new ValueCorrespondence(sourcePath, targetPath);

		return corr;
	}

	private ValueCorrespondence createValueCorrespondenceWithConcats(String str1a, String str1b, String str2) {

		List<String> sourcePathStepsA = new ArrayList<String>();
		List<String> sourcePathStepsB = new ArrayList<String>();
		sourcePathStepsA.add(str1a);
		sourcePathStepsB.add(str1b);
		PathExpression sourcePathA = new PathExpression(sourcePathStepsA);
		PathExpression sourcePathB = new PathExpression(sourcePathStepsB);
		List<PathExpression> sourcePaths = new ArrayList<PathExpression>(2);
		sourcePaths.add(sourcePathA);
		sourcePaths.add(sourcePathB);

		List<String> targetPathSteps = new ArrayList<String>();
		targetPathSteps.add(str2);
		PathExpression targetPath = new PathExpression(targetPathSteps);
		
		//TODO allow functions with more than two parameters
		//String dashes = "\"---\"";
		Expression exp = new Expression(str1a + " + " + str1b);
		ValueCorrespondence corr = new ValueCorrespondence(sourcePaths, targetPath, exp); // 2
																							// source
																							// paths,
																							// 1
																							// target
																							// path

		return corr;
	}

	private ValueCorrespondence createValueCorrespondenceWithConcats(String str1a, String str2) {

		List<String> sourcePathStepsA = new ArrayList<String>();
		sourcePathStepsA.add(str1a);
		PathExpression sourcePathA = new PathExpression(sourcePathStepsA);
		List<PathExpression> sourcePaths = new ArrayList<PathExpression>(1);
		sourcePaths.add(sourcePathA);

		List<String> targetPathSteps = new ArrayList<String>();
		targetPathSteps.add(str2);
		PathExpression targetPath = new PathExpression(targetPathSteps);

		String dashes = "\"---\"";
		Expression exp = new Expression(str1a + " +" + dashes);
		ValueCorrespondence corr = new ValueCorrespondence(sourcePaths, targetPath, exp); // 1
																							// source
																							// path
																							// +
																							// "---",
																							// 1
																							// target
																							// path

		return corr;
	}

	private ValueCorrespondence createValueCorrespondenceWithSubstring(String source, String target) {
		List<String> sourcePathStepsA = new ArrayList<String>();
		sourcePathStepsA.add(source);
		PathExpression sourcePathA = new PathExpression(sourcePathStepsA);
		List<PathExpression> sourcePaths = new ArrayList<PathExpression>(1);
		sourcePaths.add(sourcePathA);

		List<String> targetPathSteps = new ArrayList<String>();
		targetPathSteps.add(target);
		PathExpression targetPath = new PathExpression(targetPathSteps);

		Expression exp = new Expression("substring(" + source + ", 2)");
		ValueCorrespondence corr = new ValueCorrespondence(sourcePaths, targetPath, exp); // 2
																							// source
																							// paths,
																							// 1
																							// target
																							// path

		return corr;
	}

	private ValueCorrespondence createValueCorrespondenceWithSum(String source, String target) {
		List<String> sourcePathStepsA = new ArrayList<String>();
		sourcePathStepsA.add(source);
		PathExpression sourcePathA = new PathExpression(sourcePathStepsA);
		List<PathExpression> sourcePaths = new ArrayList<PathExpression>(1);
		sourcePaths.add(sourcePathA);

		List<String> targetPathSteps = new ArrayList<String>();
		targetPathSteps.add(target);
		PathExpression targetPath = new PathExpression(targetPathSteps);

		Expression exp = new Expression("sum(" + source + ")");
		ValueCorrespondence corr = new ValueCorrespondence(sourcePaths, targetPath, exp); // 2
																							// source
																							// paths,
																							// 1
																							// target
																							// path

		return corr;
	}

	public void setTargetJoinMandatory(boolean targetJoinMandatory) {
		this.targetJoinMandatory = targetJoinMandatory;
	}

	public void setSourceJoinMandatory(boolean sourceJoinMandatory) {
		this.sourceJoinMandatory = sourceJoinMandatory;
	}
}