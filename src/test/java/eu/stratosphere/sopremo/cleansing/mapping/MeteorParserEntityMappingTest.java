/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.cleansing.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.meteor.QueryParser;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.query.AdditionalInfoResolver;
import eu.stratosphere.sopremo.query.IConfObjectRegistry;

/**
 * 
 */
public class MeteorParserEntityMappingTest extends MeteorParseTest {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.meteor.MeteorParseTest#initParser(eu.stratosphere.meteor
	 * .QueryParser)
	 */
	@Override
	protected void initParser(QueryParser queryParser) {
		final IConfObjectRegistry<Operator<?>> operatorRegistry = queryParser
				.getPackageManager().getOperatorRegistry();
		operatorRegistry.put(EntityMapping.class,
				new AdditionalInfoResolver.None());
		super.initParser(queryParser);
	}

	private SopremoPlan getExpectedPlanForDefaultInputOutput() {

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file://usCongressMembers.json");
		final Source input2 = new Source("file://usCongressBiographies.json");
		final EntityMapping extract = new EntityMapping().withInputs(input1,
				input2);
		final Sink output1 = new Sink("file://person.json").withInputs(extract
				.getOutput(0));
		final Sink output2 = new Sink("file://legalEntity.json")
				.withInputs(extract.getOutput(1));
		expectedPlan.setSinks(output1, output2);

		return expectedPlan;
	}

	private SopremoPlan getExpectedPlanForDefaultInputOutput(
			MappingInformation mappingInformation) {

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file://usCongressMembers.json");
		final Source input2 = new Source("file://usCongressBiographies.json");
		final EntityMapping extract = new EntityMapping().withInputs(input1,
				input2);
		extract.getSpicyMappingTransformation().setMappingInformation(
				mappingInformation);
		final Sink output1 = new Sink("file://person.json").withInputs(extract
				.getOutput(0));
		final Sink output2 = new Sink("file://legalEntity.json")
				.withInputs(extract.getOutput(1));
		expectedPlan.setSinks(output1, output2);

		return expectedPlan;
	}

	@Test
	public void testMinimalSchemaMapping() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $usCongressMembers.id_o"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);

		MappingInformation info = new MappingInformation();

		MappingSchema schema = new MappingSchema(2, "source");
		schema.addKeyToInput("in1", "worksFor_o");
		schema.addKeyToInput("in0", "id_o");
		schema.addKeyToInput("in0", "name_o");
		info.setSourceSchema(schema);

		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));
		MappingSchema targetSchema = new MappingSchema(2, "target");
		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");
		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");
		target.setTargetSchema(targetSchema);
		info.setTarget(target);

		List<MappingValueCorrespondence> valueCorrespondences = new LinkedList<MappingValueCorrespondence>();
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));
		info.setValueCorrespondences(valueCorrespondences);

		final SopremoPlan expectedPlan = getExpectedPlanForDefaultInputOutput(info);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testFinalSchemaMapping() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "where ($usCongressMembers.biography_o[0:1] == $usCongressBiographies.biographyId_o[1:1])\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+
				// "    biography_p: $usCongressBiographies.biographyId_o,\n" +
				"    worksFor_p: $legalEntity.id"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);
		MappingInformation info = new MappingInformation();

		List<SpicyPathExpression> sourcePaths = new LinkedList<SpicyPathExpression>();
		sourcePaths.add(new SpicyPathExpression(
				"source.entities_in0.entity_in0", "biography_o"));
		List<SpicyPathExpression> targetPaths = new LinkedList<SpicyPathExpression>();
		targetPaths.add(new SpicyPathExpression(
				"source.entities_in1.entity_in1", "biographyId_o"));
		MappingJoinCondition sourceJoinCondition = new MappingJoinCondition(
				sourcePaths, targetPaths, true, true);
		info.setSourceJoinCondition(sourceJoinCondition);

		List<SpicyPathExpression> sourcePaths2 = new LinkedList<SpicyPathExpression>();
		sourcePaths2.add(new SpicyPathExpression(
				"target.entities_in0.entity_in0", "worksFor_p"));
		List<SpicyPathExpression> targetPaths2 = new LinkedList<SpicyPathExpression>();
		targetPaths2.add(new SpicyPathExpression(
				"target.entities_in1.entity_in1", "id"));
		MappingJoinCondition targetJoinCondition = new MappingJoinCondition(
				sourcePaths2, targetPaths2, true, true);
		List<MappingJoinCondition> targetJoinConditions = new LinkedList<MappingJoinCondition>();
		targetJoinConditions.add(targetJoinCondition);
		info.setTargetJoinConditions(targetJoinConditions);

		MappingSchema schema = new MappingSchema(2, "source");
		schema.addKeyToInput("in1", "worksFor_o");
		schema.addKeyToInput("in1", "biographyId_o");
		schema.addKeyToInput("in0", "id_o");
		schema.addKeyToInput("in0", "name_o");
		schema.addKeyToInput("in0", "biography_o");
		info.setSourceSchema(schema);

		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));
		MappingSchema targetSchema = new MappingSchema(2, "target");
		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");
		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");
		target.setTargetSchema(targetSchema);
		info.setTarget(target);

		List<MappingValueCorrespondence> valueCorrespondences = new LinkedList<MappingValueCorrespondence>();
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));
		info.setValueCorrespondences(valueCorrespondences);

		final SopremoPlan expectedPlan = getExpectedPlanForDefaultInputOutput(info);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testRenamedOperator() { // map entities from ... as ...
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $usCongressMembers.id_o"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);

		MappingInformation mappingInformation = new MappingInformation();

		// sourceSchema
		MappingSchema sourceSchema = new MappingSchema(2, "source");

		mappingInformation.setSourceSchema(sourceSchema);

		sourceSchema.addKeyToInput("in1", "worksFor_o");

		sourceSchema.addKeyToInput("in0", "id_o");
		sourceSchema.addKeyToInput("in0", "name_o");

		// target
		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));

		MappingSchema targetSchema = new MappingSchema(2, "target");

		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");

		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");

		target.setTargetSchema(targetSchema);

		mappingInformation.setTarget(target);

		// valueCorrespondences
		List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));

		mappingInformation.setValueCorrespondences(valueCorrespondences);

		final SopremoPlan expectedPlan = getExpectedPlanForDefaultInputOutput(mappingInformation);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testWhereClause() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "where ($usCongressMembers.biography[0:1] == $usCongressBiographies.biographyId[1:1])\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $usCongressMembers.id_o"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);

		MappingInformation mappingInformation = new MappingInformation();

		// sourceJoinCondition
		List<SpicyPathExpression> sourceJoinConditionSourcePaths = Collections
				.singletonList(new SpicyPathExpression(
						"source.entities_in0.entity_in0", "biography"));
		List<SpicyPathExpression> targetJoinConditionSourcePaths = Collections
				.singletonList(new SpicyPathExpression(
						"source.entities_in1.entity_in1", "biographyId"));

		MappingJoinCondition sourceJoinCondition = new MappingJoinCondition(
				sourceJoinConditionSourcePaths, targetJoinConditionSourcePaths,
				true, true);

		mappingInformation.setSourceJoinCondition(sourceJoinCondition);

		// sourceSchema
		MappingSchema sourceSchema = new MappingSchema(2, "source");

		mappingInformation.setSourceSchema(sourceSchema);

		sourceSchema.addKeyToInput("in1", "biographyId");
		sourceSchema.addKeyToInput("in1", "worksFor_o");

		sourceSchema.addKeyToInput("in0", "id_o");
		sourceSchema.addKeyToInput("in0", "name_o");
		sourceSchema.addKeyToInput("in0", "biography");

		// target
		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));

		MappingSchema targetSchema = new MappingSchema(2, "target");

		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");

		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");

		target.setTargetSchema(targetSchema);

		mappingInformation.setTarget(target);

		// valueCorrespondences
		List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));

		mappingInformation.setValueCorrespondences(valueCorrespondences);

		final SopremoPlan expectedPlan = getExpectedPlanForDefaultInputOutput(mappingInformation);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testMultipleSourcesPerGroupBy() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    biography_p: $usCongressBiographies.biographyId_o,\n"
				+ "    worksFor_p: $legalEntity.id"
				+ // included
				"  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);

		MappingInformation info = new MappingInformation();

		List<SpicyPathExpression> sourcePaths2 = new LinkedList<SpicyPathExpression>();
		sourcePaths2.add(new SpicyPathExpression(
				"target.entities_in0.entity_in0", "worksFor_p"));
		List<SpicyPathExpression> targetPaths2 = new LinkedList<SpicyPathExpression>();
		targetPaths2.add(new SpicyPathExpression(
				"target.entities_in1.entity_in1", "id"));
		MappingJoinCondition targetJoinCondition = new MappingJoinCondition(
				sourcePaths2, targetPaths2, true, true);
		List<MappingJoinCondition> targetJoinConditions = new LinkedList<MappingJoinCondition>();
		targetJoinConditions.add(targetJoinCondition);
		info.setTargetJoinConditions(targetJoinConditions);

		MappingSchema schema = new MappingSchema(2, "source");
		schema.addKeyToInput("in1", "worksFor_o");
		schema.addKeyToInput("in1", "biographyId_o");
		schema.addKeyToInput("in0", "id_o");
		schema.addKeyToInput("in0", "name_o");
		info.setSourceSchema(schema);

		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));
		MappingSchema targetSchema = new MappingSchema(2, "target");
		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");
		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");
		targetSchema.addKeyToInput("in0", "biography_p");
		target.setTargetSchema(targetSchema);
		info.setTarget(target);

		List<MappingValueCorrespondence> valueCorrespondences = new LinkedList<MappingValueCorrespondence>();
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"biographyId_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "biography_p")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));
		info.setValueCorrespondences(valueCorrespondences);

		final SopremoPlan expectedPlan = getExpectedPlanForDefaultInputOutput(info);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testOneInputMultipleOutput() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $legalEntity.id"
				+ "  },"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_l: $usCongressMembers.id_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);
		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input = new Source("file://usCongressMembers.json");
		final EntityMapping extract = new EntityMapping().withInputs(input);
		final Sink output1 = new Sink("file://person.json").withInputs(extract
				.getOutput(0));
		final Sink output2 = new Sink("file://legalEntity.json")
				.withInputs(extract.getOutput(1));
		expectedPlan.setSinks(output1, output2);

		MappingInformation mappingInformation = new MappingInformation();

		// targetJoinCondition
		List<SpicyPathExpression> sourceJoinConditionSourcePaths = Collections
				.singletonList(new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p"));
		List<SpicyPathExpression> targetJoinConditionSourcePaths = Collections
				.singletonList(new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id"));

		MappingJoinCondition targetJoinCondition = new MappingJoinCondition(
				sourceJoinConditionSourcePaths, targetJoinConditionSourcePaths,
				true, true);

		List<MappingJoinCondition> targetJoinConditions = new ArrayList<MappingJoinCondition>();
		targetJoinConditions.add(targetJoinCondition);
		mappingInformation.setTargetJoinConditions(targetJoinConditions);

		// sourceSchema
		MappingSchema sourceSchema = new MappingSchema(1, "source");

		mappingInformation.setSourceSchema(sourceSchema);

		sourceSchema.addKeyToInput("in0", "id_o");
		sourceSchema.addKeyToInput("in0", "name_o");

		// target
		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));

		MappingSchema targetSchema = new MappingSchema(2, "target");

		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");

		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");

		target.setTargetSchema(targetSchema);

		mappingInformation.setTarget(target);

		// valueCorrespondences
		List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));

		mappingInformation.setValueCorrespondences(valueCorrespondences);

		extract.getSpicyMappingTransformation().setMappingInformation(
				mappingInformation);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testSwitchedOutputs() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$legalEntity, $person = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "where ($usCongressMembers.biography_o[0:1] == $usCongressBiographies.biographyId_o[1:1])\n"
				+ "as [\n"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ // switched output order and group by
				"    name_l: $usCongressBiographies.worksFor_o"
				+ "  },"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $legalEntity.id"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);
		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file://usCongressMembers.json");
		final Source input2 = new Source("file://usCongressBiographies.json");
		final EntityMapping extract = new EntityMapping().withInputs(input1,
				input2);
		final Sink output2 = new Sink("file://legalEntity.json")
				.withInputs(extract.getOutput(0));
		final Sink output1 = new Sink("file://person.json").withInputs(extract
				.getOutput(1));
		expectedPlan.setSinks(output1, output2);

		MappingInformation info = new MappingInformation();

		List<SpicyPathExpression> sourcePaths = new LinkedList<SpicyPathExpression>();
		sourcePaths.add(new SpicyPathExpression(
				"source.entities_in0.entity_in0", "biography_o"));
		List<SpicyPathExpression> targetPaths = new LinkedList<SpicyPathExpression>();
		targetPaths.add(new SpicyPathExpression(
				"source.entities_in1.entity_in1", "biographyId_o"));
		MappingJoinCondition sourceJoinCondition = new MappingJoinCondition(
				sourcePaths, targetPaths, true, true);
		info.setSourceJoinCondition(sourceJoinCondition);

		List<SpicyPathExpression> sourcePaths2 = new LinkedList<SpicyPathExpression>();
		sourcePaths2.add(new SpicyPathExpression(
				"target.entities_in1.entity_in1", "worksFor_p"));
		List<SpicyPathExpression> targetPaths2 = new LinkedList<SpicyPathExpression>();
		targetPaths2.add(new SpicyPathExpression(
				"target.entities_in0.entity_in0", "id"));
		MappingJoinCondition targetJoinCondition = new MappingJoinCondition(
				sourcePaths2, targetPaths2, true, true);
		List<MappingJoinCondition> targetJoinConditions = new LinkedList<MappingJoinCondition>();
		targetJoinConditions.add(targetJoinCondition);
		info.setTargetJoinConditions(targetJoinConditions);

		MappingSchema schema = new MappingSchema(2, "source");
		schema.addKeyToInput("in1", "worksFor_o");
		schema.addKeyToInput("in1", "biographyId_o");
		schema.addKeyToInput("in0", "id_o");
		schema.addKeyToInput("in0", "name_o");
		schema.addKeyToInput("in0", "biography_o");
		info.setSourceSchema(schema);

		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));
		MappingSchema targetSchema = new MappingSchema(2, "target");
		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "worksFor_p");
		targetSchema.addKeyToInput("in1", "name_p");
		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "name_l");
		target.setTargetSchema(targetSchema);
		info.setTarget(target);

		List<MappingValueCorrespondence> valueCorrespondences = new LinkedList<MappingValueCorrespondence>();
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_l")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_p")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "worksFor_p")));
		info.setValueCorrespondences(valueCorrespondences);

		extract.getSpicyMappingTransformation().setMappingInformation(info);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testSwitchedJoinOrder() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "where ($usCongressBiographies.biographyId_o[1:1] == $usCongressMembers.biography_o[0:1])\n"
				+ // if switched, spicy creates foreign keys on source in
					// reversed order
				"as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $legalEntity.id"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);
		MappingInformation mappingInformation = new MappingInformation();

		// sourceSchema
		MappingSchema sourceSchema = new MappingSchema(2, "source");

		mappingInformation.setSourceSchema(sourceSchema);

		sourceSchema.addKeyToInput("in1", "worksFor_o");

		sourceSchema.addKeyToInput("in0", "id_o");
		sourceSchema.addKeyToInput("in0", "name_o");
		sourceSchema.addKeyToInput("in0", "biography");

		// target
		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));

		MappingSchema targetSchema = new MappingSchema(2, "target");

		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");

		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");

		target.setTargetSchema(targetSchema);

		mappingInformation.setTarget(target);

		// valueCorrespondences
		List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));

		mappingInformation.setValueCorrespondences(valueCorrespondences);

		final SopremoPlan expectedPlan = getExpectedPlanForDefaultInputOutput(mappingInformation);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testThreeInputs() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$states = read from 'file://states.json';\n"
				+

				"$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies, $states\n"
				+ "where ($usCongressMembers.biography_o[0:1] == $usCongressBiographies.biographyId_o[1:1]) "
				+ "		and ($usCongressMembers.state[0:1] == $states.letterCode[1:1])\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $legalEntity.id,\n"
				+ "	 state_p: $states.name"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);
		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file://usCongressMembers.json");
		final Source input2 = new Source("file://usCongressBiographies.json");
		final Source input3 = new Source("file://states.json");
		final EntityMapping extract = new EntityMapping().withInputs(input1,
				input2, input3);
		final Sink output1 = new Sink("file://person.json").withInputs(extract
				.getOutput(0));
		final Sink output2 = new Sink("file://legalEntity.json")
				.withInputs(extract.getOutput(1));
		expectedPlan.setSinks(output1, output2);

		MappingInformation mappingInformation = new MappingInformation();

		// sourceJoinCondition
		List<SpicyPathExpression> sourceJoinConditionSourcePaths1 = Collections
				.singletonList(new SpicyPathExpression(
						"source.entities_in0.entity_in0", "state"));
		List<SpicyPathExpression> targetJoinConditionSourcePaths1 = Collections
				.singletonList(new SpicyPathExpression(
						"source.entities_in1.entity_in1", "biographyId_o"));

		MappingJoinCondition sourceJoinCondition = new MappingJoinCondition(
				sourceJoinConditionSourcePaths1,
				targetJoinConditionSourcePaths1, true, true);

		mappingInformation.setSourceJoinCondition(sourceJoinCondition);

		// targetJoinCondition
		List<SpicyPathExpression> sourceJoinConditionSourcePaths2 = Collections
				.singletonList(new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p"));
		List<SpicyPathExpression> targetJoinConditionSourcePaths2 = Collections
				.singletonList(new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id"));

		MappingJoinCondition targetJoinCondition = new MappingJoinCondition(
				sourceJoinConditionSourcePaths2,
				targetJoinConditionSourcePaths2, true, true);

		List<MappingJoinCondition> targetJoinConditions = new ArrayList<MappingJoinCondition>();
		targetJoinConditions.add(targetJoinCondition);
		mappingInformation.setTargetJoinConditions(targetJoinConditions);

		// sourceSchema
		MappingSchema sourceSchema = new MappingSchema(3, "source");

		mappingInformation.setSourceSchema(sourceSchema);

		sourceSchema.addKeyToInput("in2", "name");
		sourceSchema.addKeyToInput("in1", "worksFor_o");
		sourceSchema.addKeyToInput("in1", "biographyId_o");
		sourceSchema.addKeyToInput("in0", "id_o");
		sourceSchema.addKeyToInput("in0", "state");
		sourceSchema.addKeyToInput("in0", "name_o");

		// target
		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));

		MappingSchema targetSchema = new MappingSchema(2, "target");

		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");

		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");
		targetSchema.addKeyToInput("in0", "state_p");

		target.setTargetSchema(targetSchema);

		mappingInformation.setTarget(target);

		// valueCorrespondences
		List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in2.entity_in2",
						"name"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "state_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));

		mappingInformation.setValueCorrespondences(valueCorrespondences);

		extract.getSpicyMappingTransformation().setMappingInformation(
				mappingInformation);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testThreeOutputs() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity, $personNames = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "where ($usCongressMembers.biography_o[0:1] == $usCongressBiographies.biographyId_o[1:1])\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $legalEntity.id"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  },"
				+ "  group $usCongressMembers by $usCongressMembers.name_o into {"
				+ // Only include id field
				"  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';\n"
				+ "write $personNames to 'file://personNames.json';";

		final SopremoPlan actualPlan = parseScript(query);
		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file://usCongressMembers.json");
		final Source input2 = new Source("file://usCongressBiographies.json");
		final EntityMapping extract = new EntityMapping().withInputs(input1,
				input2);
		final Sink output1 = new Sink("file://person.json").withInputs(extract
				.getOutput(0));
		final Sink output2 = new Sink("file://legalEntity.json")
				.withInputs(extract.getOutput(1));
		final Sink output3 = new Sink("file://personNames.json")
				.withInputs(extract.getOutput(2));
		expectedPlan.setSinks(output1, output2, output3);

		MappingInformation info = new MappingInformation();

		List<SpicyPathExpression> sourcePaths = new LinkedList<SpicyPathExpression>();
		sourcePaths.add(new SpicyPathExpression(
				"source.entities_in0.entity_in0", "biography_o"));
		List<SpicyPathExpression> targetPaths = new LinkedList<SpicyPathExpression>();
		targetPaths.add(new SpicyPathExpression(
				"source.entities_in1.entity_in1", "biographyId_o"));
		MappingJoinCondition sourceJoinCondition = new MappingJoinCondition(
				sourcePaths, targetPaths, true, true);
		info.setSourceJoinCondition(sourceJoinCondition);

		List<SpicyPathExpression> sourcePaths2 = new LinkedList<SpicyPathExpression>();
		sourcePaths2.add(new SpicyPathExpression(
				"target.entities_in0.entity_in0", "worksFor_p"));
		List<SpicyPathExpression> targetPaths2 = new LinkedList<SpicyPathExpression>();
		targetPaths2.add(new SpicyPathExpression(
				"target.entities_in1.entity_in1", "id"));
		MappingJoinCondition targetJoinCondition = new MappingJoinCondition(
				sourcePaths2, targetPaths2, true, true);
		List<MappingJoinCondition> targetJoinConditions = new LinkedList<MappingJoinCondition>();
		targetJoinConditions.add(targetJoinCondition);
		info.setTargetJoinConditions(targetJoinConditions);

		MappingSchema schema = new MappingSchema(2, "source");
		schema.addKeyToInput("in1", "worksFor_o");
		schema.addKeyToInput("in1", "biographyId_o");
		schema.addKeyToInput("in0", "id_o");
		schema.addKeyToInput("in0", "name_o");
		schema.addKeyToInput("in0", "biography_o");
		info.setSourceSchema(schema);

		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in2.entity_in2", "id"));
		MappingSchema targetSchema = new MappingSchema(3, "target");
		targetSchema.addKeyToInput("in2", "id");
		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");
		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");
		target.setTargetSchema(targetSchema);
		info.setTarget(target);

		List<MappingValueCorrespondence> valueCorrespondences = new LinkedList<MappingValueCorrespondence>();
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in2.entity_in2", "id")));
		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));
		info.setValueCorrespondences(valueCorrespondences);

		extract.getSpicyMappingTransformation().setMappingInformation(info);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	@Ignore
	public void testMinimalSchemaMappingWithFunction() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: concat([$usCongressMembers.name_o, \"---\"]),\n"
				+ "    worksFor_p: $usCongressMembers.id_o"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);

		MappingInformation mappingInformation = new MappingInformation();

		// sourceSchema
		MappingSchema sourceSchema = new MappingSchema(2, "source");

		mappingInformation.setSourceSchema(sourceSchema);

		sourceSchema.addKeyToInput("in1", "worksFor_o");

		sourceSchema.addKeyToInput("in0", "id_o");
		sourceSchema.addKeyToInput("in0", "name_o");
		sourceSchema.addKeyToInput("in0", "biography");

		// target
		MappingDataSource target = new MappingDataSource();
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in0.entity_in0", "id"));
		target.addKeyConstraint(new MappingKeyConstraint(
				"target.entities_in1.entity_in1", "id"));

		MappingSchema targetSchema = new MappingSchema(2, "target");

		targetSchema.addKeyToInput("in1", "id");
		targetSchema.addKeyToInput("in1", "name_l");

		targetSchema.addKeyToInput("in0", "id");
		targetSchema.addKeyToInput("in0", "worksFor_p");
		targetSchema.addKeyToInput("in0", "name_p");

		target.setTargetSchema(targetSchema);

		mappingInformation.setTarget(target);

		// valueCorrespondences
		List<MappingValueCorrespondence> valueCorrespondences = new ArrayList<MappingValueCorrespondence>();

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"name_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "name_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in0.entity_in0",
						"id_o"), new SpicyPathExpression(
						"target.entities_in0.entity_in0", "worksFor_p")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "id")));

		valueCorrespondences.add(new MappingValueCorrespondence(
				new SpicyPathExpression("source.entities_in1.entity_in1",
						"worksFor_o"), new SpicyPathExpression(
						"target.entities_in1.entity_in1", "name_l")));

		mappingInformation.setValueCorrespondences(valueCorrespondences);

		final SopremoPlan expectedPlan = getExpectedPlanForDefaultInputOutput(mappingInformation);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	@Test
	public void testMappingTaskEquals() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
				+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
				+ "$person, $legalEntity = map entities from $usCongressMembers, $usCongressBiographies\n"
				+ "as [\n"
				+ "  group $usCongressMembers by $usCongressMembers.id_o into {"
				+ "    name_p: $usCongressMembers.name_o,\n"
				+ "    worksFor_p: $usCongressMembers.id_o"
				+ "  },"
				+ "  group $usCongressBiographies by $usCongressBiographies.worksFor_o into {"
				+ "    name_l: $usCongressBiographies.worksFor_o"
				+ "  }"
				+ "];\n"
				+ "write $person to 'file://person.json';\n"
				+ "write $legalEntity to 'file://legalEntity.json';";

		final SopremoPlan actualPlan = parseScript(query);

		EntityMapping mapping1 = null;
		EntityMapping mapping2 = null;
		for (Operator<?> operator : actualPlan.getContainedOperators()) {
			if (operator instanceof EntityMapping) {
				mapping1 = (EntityMapping) operator;
				mapping2 = ((EntityMapping) operator).copy();
				break;
			}
		}
		Assert.assertEquals(mapping1, mapping2);
	}
}