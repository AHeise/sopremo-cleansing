package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.algebra.IAlgebraOperator;
import it.unibas.spicy.model.correspondence.ValueCorrespondence;
import it.unibas.spicy.model.datasource.DataSource;
import it.unibas.spicy.model.datasource.ForeignKeyConstraint;
import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.JoinCondition;
import it.unibas.spicy.model.datasource.KeyConstraint;
import it.unibas.spicy.model.datasource.nodes.AttributeNode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;
import it.unibas.spicy.model.datasource.nodes.SetNode;
import it.unibas.spicy.model.datasource.nodes.TupleNode;
import it.unibas.spicy.model.expressions.Expression;
import it.unibas.spicy.model.mapping.MappingData;
import it.unibas.spicy.model.mapping.MappingTask;
import it.unibas.spicy.model.paths.PathExpression;
import it.unibas.spicy.persistence.DAOException;
import it.unibas.spicy.persistence.DAOMappingTaskTgds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import eu.stratosphere.sopremo.cleansing.mapping.GeneratedSchemaMapping;
import eu.stratosphere.sopremo.operator.Operator;

public class MappingTaskFactory {

	boolean createConcat = false;
	
	public boolean isCreateConcat() {
		return createConcat;
	}

	public void setCreateConcat(boolean createConcat) {
		this.createConcat = createConcat;
	}

	public static void main(String[] args) {
		new MappingTaskFactory().create();
	}

	public MappingTask create() {

		MappingTask task = createMappingTaskFromMeteorScript();
		System.out.println(task);
		
		MappingData data = task.getMappingData();
		System.out.println(data);
		IAlgebraOperator tree = task.getMappingData().getAlgebraTree();
		System.out.println(tree);	
		
		return task;
	}
	
	private MappingTask createMappingTaskFromMeteorScript() {
		
//		### soure and target
		
		INode sourceSchema = createSourceSchema();
		INode targetSchema = createTargetSchema();
		String type = null; // "XML";
		
		DataSource source = new DataSource(type, sourceSchema);
		DataSource target = new DataSource(type, targetSchema);	
		
//		### key contraints
		
		//TODO waren nicht drin for source 
//		KeyConstraint sourceKeyConstraint1 = createKeyConstraints("usCongress.usCongressBiographies.usCongressBiography.biographyId");
//		KeyConstraint sourceKeyConstraint2 = createKeyConstraints("usCongress.usCongressMembers.usCongressMember.id");
//		source.addKeyConstraint(sourceKeyConstraint1);
//		source.addKeyConstraint(sourceKeyConstraint2);
		
		KeyConstraint targetKeyConstraint1 = createKeyConstraints("usCongress.persons.person.id");
		KeyConstraint targetKeyConstraint2 = createKeyConstraints("usCongress.legalEntities.legalEntity.id");
		target.addKeyConstraint(targetKeyConstraint1);
		target.addKeyConstraint(targetKeyConstraint2);
		
//		### foreign key contraints //TODO waren nicht drin
		
//		ForeignKeyConstraint foreignKeyConstraint1 = createForeignKeyConstraint("usCongress.usCongressMembers.usCongressMember.biography", sourceKeyConstraint1);
//		source.addForeignKeyConstraint(foreignKeyConstraint1);
//
		ForeignKeyConstraint foreignKeyConstraint2 = createForeignKeyConstraint("usCongress.persons.person.worksFor", targetKeyConstraint2);
		target.addForeignKeyConstraint(foreignKeyConstraint2);

//		### join conditions
		
		List<String> list1 = new ArrayList<String>();
		list1.add("usCongress.usCongressMembers.usCongressMember.biography");
		PathExpression fromPath1 = new PathExpression(list1);
		List<PathExpression> slp1 = new ArrayList<PathExpression>();
		slp1.add(fromPath1);
		
		List<String> list2 = new ArrayList<String>();
		list2.add("usCongress.usCongressBiographies.usCongressBiography.biographyId");
		PathExpression toPath1 = new PathExpression(list2);
		List<PathExpression> slp2 = new ArrayList<PathExpression>();
		slp2.add(toPath1);
		
//		JoinCondition sourceJoinCondition = new JoinCondition(fromPath1, toPath1);
//		JoinCondition sourceJoinCondition = new JoinCondition(slp1, slp2, true, true);
		JoinCondition sourceJoinCondition = new JoinCondition(slp1, slp2, true);
		sourceJoinCondition.setMandatory(true);
		sourceJoinCondition.setMonodirectional(true);
		
		List<String> list3 = new ArrayList<String>();
		list3.add("usCongress.persons.person.worksFor");
		PathExpression fromPath2 = new PathExpression(list3);
		List<PathExpression> tlp1 = new ArrayList<PathExpression>();
		tlp1.add(fromPath2);
		
		List<String> list4 = new ArrayList<String>();
		list4.add("usCongress.legalEntities.legalEntity.id");
		PathExpression toPath2 = new PathExpression(list4);
		List<PathExpression> tlp2 = new ArrayList<PathExpression>();
		tlp2.add(toPath2);
		
//		JoinCondition targetJoinCondition = new JoinCondition(fromPath2, toPath2);
//		JoinCondition targetJoinCondition = new JoinCondition(tlp1, tlp2, true, true);
		JoinCondition targetJoinCondition = new JoinCondition(tlp1, tlp2, true);
		targetJoinCondition.setMandatory(true);
		targetJoinCondition.setMonodirectional(true);
		
//		### value correspondences
		
		List<ValueCorrespondence> valueCorrespondences = createValueCorrespondences();
		
//		### task

		MappingTask task = new MappingTask(source, target, valueCorrespondences);
		task.getSourceProxy().addJoinCondition(sourceJoinCondition);
		task.getTargetProxy().addJoinCondition(targetJoinCondition);
		
		System.out.println("###\n" + sourceSchema.toShortString());
		System.out.println("###\n" + targetSchema.toShortString());
		System.out.println("###\n" + valueCorrespondences);
		System.out.println("###\n" + task);
		
		return task;
	}
	
	private INode createSourceSchema() {
		
		INode dummy = new LeafNode("dummy");
		
//		usCongressMembers
		
		INode id = new AttributeNode("id");
		id.addChild(dummy);
		INode name = new AttributeNode("name");
		name.addChild(dummy);
		INode biography = new AttributeNode("biography");
		biography.addChild(dummy);
		
		INode usCongressMember = new TupleNode("usCongressMember");
		usCongressMember.addChild(id);
		usCongressMember.addChild(name);
		usCongressMember.addChild(biography);
		
		INode usCongressMembers = new SetNode("usCongressMembers");
		usCongressMembers.addChild(usCongressMember);
		
//		usCongressBiographies
		
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
		
		INode dummy = new LeafNode("string");
		
//		person
		
		INode id = new AttributeNode("id");
		id.addChild(dummy);
		id.setRequired(true); //TODO new
		
		INode name = new AttributeNode("nestedName"); //TODO nesting
		name.addChild(dummy);
		INode fullName = new TupleNode("fullName");
		fullName.addChild(name);
		
		INode worksFor = new AttributeNode("worksFor");
		worksFor.addChild(dummy);
		 
		INode person = new TupleNode("person");
		person.addChild(id);
		person.addChild(fullName); //TODO nesting
		person.addChild(worksFor);
		
		INode persons = new SetNode("persons");
		persons.addChild(person);
		
//		legalEntity
		
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
		
		KeyConstraint keyConstraint = new KeyConstraint(keyPath, true);			// TODO: do we need information if primary key?
		
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
		
		ValueCorrespondence c0 = createValueCorrespondence("usCongress.usCongressMembers.usCongressMember.id", "usCongress.persons.person.id");
		ValueCorrespondence c1 = createValueCorrespondence("usCongress.usCongressMembers.usCongressMember.name", "usCongress.persons.person.fullName.nestedName"); 
		ValueCorrespondence c2 = createValueCorrespondence("usCongress.usCongressBiographies.usCongressBiography.worksFor", "usCongress.legalEntities.legalEntity.id");
		ValueCorrespondence c3 = createValueCorrespondence("usCongress.usCongressBiographies.usCongressBiography.worksFor", "usCongress.legalEntities.legalEntity.name");
		ValueCorrespondence c4 = createValueCorrespondence("usCongress.usCongressBiographies.usCongressBiography.worksFor", "usCongress.persons.person.worksFor");

		
		List<ValueCorrespondence> valueCorrespondences = new ArrayList<ValueCorrespondence>();
		valueCorrespondences.add(c0);
		valueCorrespondences.add(c1); 
		valueCorrespondences.add(c2);
		valueCorrespondences.add(c3);
		valueCorrespondences.add(c4);

		return valueCorrespondences;
	}
	
	private ValueCorrespondence createValueCorrespondence(String str1, String str2) {
		
		List<String> sourcePathSteps = new ArrayList<String>();
		List<String> targetPathSteps = new ArrayList<String>();
		
		sourcePathSteps.add(str1);
		targetPathSteps.add(str2);
		
		PathExpression sourcePath = new PathExpression(sourcePathSteps);
		PathExpression targetPath = new PathExpression(targetPathSteps);
		
		ValueCorrespondence corr;
		if(createConcat && str1.equals("usCongress.usCongressBiographies.usCongressBiography.worksFor")) {
			//	Expression exp = new Expression("right(\""+str1+"\",3)");
			Expression exp = new Expression(str1 + " + \"---\"");

			//TODO read source paths from exp
			List<PathExpression> sourcePaths = new ArrayList<PathExpression>(1);
			sourcePaths.add(sourcePath);
			corr = new ValueCorrespondence(sourcePaths, targetPath, exp);
		} else {		
			corr = new ValueCorrespondence(sourcePath, targetPath);
		}
		
		return corr;
	}
}