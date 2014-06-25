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

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.cleansing.DataTransformation;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.ObjectCreation.SymbolicAssignment;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.testing.SopremoTestUtil;
import eu.stratosphere.sopremo.type.JsonUtil;
import eu.stratosphere.util.SopremoKryo;

/**
 * 
 */
public class MeteorParserEntityMappingTest extends MeteorParseTest {

	private DataTransformation parseAndGetOperator(String query) {
		SopremoPlan actualPlan = parseScript(query);
		actualPlan = SopremoTestUtil.transferToClassManager(actualPlan, new SopremoKryo());
		return (DataTransformation) Iterables.find(actualPlan.getContainedOperators(),
			Predicates.instanceOf(DataTransformation.class));
	}

	@Test
	public void testMinimalSchemaMapping() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $usCongressMembers.id_o"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("id_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("0", "id_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testForeignKey() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "where ($usCongressMembers.biography_o == $usCongressBiographies.biographyId_o)\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $legalEntity.id"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("biography_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("biographyId_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Set<SymbolicAssignment> sourceFKs = new HashSet<SymbolicAssignment>();
		sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "biography_o"),
			JsonUtil.createPath("1", "biographyId_o")));
		Assert.assertEquals(sourceFKs, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Set<SymbolicAssignment> targetFKs = new HashSet<SymbolicAssignment>();
		targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("1", "id")));
		Assert.assertEquals(targetFKs, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testRenamedOperator() { // transform records ... as ...
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $usCongressMembers.id_o"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("id_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getTargetFKs());

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("0", "id_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testWhereClause() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "where ($usCongressMembers.biography == $usCongressBiographies.biographyId)\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $usCongressMembers.id_o"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("id_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("biography", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("biographyId", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Set<SymbolicAssignment> sourceFKs = new HashSet<SymbolicAssignment>();
		sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "biography"),
			JsonUtil.createPath("1", "biographyId")));
		Assert.assertEquals(sourceFKs, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getTargetFKs());

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("0", "id_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testMultipleSourcesPeridentifiedBy() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    biography_p: $usCongressBiographies.biographyId_o,\n"
			+ "    worksFor_p: $legalEntity.id"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("biographyId_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("biography_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Set<SymbolicAssignment> targetFKs = new HashSet<SymbolicAssignment>();
		targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("1", "id")));
		Assert.assertEquals(targetFKs, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "biography_p"),
			JsonUtil.createPath("1", "biographyId_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testOneInputMultipleOutput() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $legalEntity.id"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressMembers.id_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.<ObjectCreation> newArrayList(new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("id_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Set<SymbolicAssignment> targetFKs = new HashSet<SymbolicAssignment>();
		targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("1", "id")));
		Assert.assertEquals(targetFKs, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("0", "id_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testSwitchedOutputs() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "where ($usCongressMembers.biography_o == $usCongressBiographies.biographyId_o)\n"
			+ "into [\n"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ // switched output order and identified by
			"    name_l: $usCongressBiographies.worksFor_o"
			+ "  },"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $legalEntity.id"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("biography_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("biographyId_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Set<SymbolicAssignment> sourceFKs = new HashSet<SymbolicAssignment>();
		sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "biography_o"),
			JsonUtil.createPath("1", "biographyId_o")));
		Assert.assertEquals(sourceFKs, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Set<SymbolicAssignment> targetFKs = new HashSet<SymbolicAssignment>();
		targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("1", "id")));
		Assert.assertEquals(targetFKs, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testSwitchedJoinOrder() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "where ($usCongressBiographies.biographyId_o == $usCongressMembers.biography_o)\n"
			+ // if switched, spicy creates foreign keys on source in
				// reversed order
			"into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $legalEntity.id"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("biography_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("biographyId_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Set<SymbolicAssignment> sourceFKs = new HashSet<SymbolicAssignment>();
		sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("1", "biographyId_o"), JsonUtil.createPath("0",
			"biography_o")));
		Assert.assertEquals(sourceFKs, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Set<SymbolicAssignment> targetFKs = new HashSet<SymbolicAssignment>();
		targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("1", "id")));
		Assert.assertEquals(targetFKs, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	private static void assertSchemaEquals(List<? extends EvaluationExpression> expected, List<EvaluationExpression> actual) {
		sortMappings(expected);
		sortMappings(actual);
		Assert.assertEquals(expected, actual);
	}

	/**
	 * @param expected
	 */
	private static void sortMappings(List<? extends EvaluationExpression> expressions) {
		for (EvaluationExpression expression : expressions)
			for (ObjectCreation oc : expression.findAll(ObjectCreation.class))
				Collections.sort(oc.getMappings(), new Comparator<Mapping<?>>() {
					@Override
					public int compare(Mapping<?> o1, Mapping<?> o2) {
						return ((String) o1.getTarget()).compareTo((String) o2.getTarget());
					}
				});
	}

	@Test
	public void testThreeInputs() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$states = read from 'file://states.json';\n" +

			"$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies, $states\n"
			+ "where ($usCongressMembers.biography_o == $usCongressBiographies.biographyId_o) "
			+ "		and ($usCongressMembers.state == $states.letterCode)\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $legalEntity.id,\n"
			+ "    state_p: $states.name"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema =
			Lists.newArrayList(new ObjectCreation(), new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("biography_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("state", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("biographyId_o", EvaluationExpression.VALUE);
		sourceSchema.get(2).addMapping("name", EvaluationExpression.VALUE);
		sourceSchema.get(2).addMapping("letterCode", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Set<SymbolicAssignment> sourceFKs = new HashSet<SymbolicAssignment>();
		sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "biography_o"),
			JsonUtil.createPath("1", "biographyId_o")));
		sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "state"),
			JsonUtil.createPath("2", "letterCode")));
		Assert.assertEquals(sourceFKs, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("state_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Set<SymbolicAssignment> targetFKs = new HashSet<SymbolicAssignment>();
		targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("1", "id")));
		Assert.assertEquals(targetFKs, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "state_p"),
			JsonUtil.createPath("2", "name")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testThreeOutputs() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity, $personNames = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "where ($usCongressMembers.biography_o == $usCongressBiographies.biographyId_o)\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $legalEntity.id"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  },"
			+ "  entity $personNames with {"
			+ // Only include id field
			"  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';\n"
			+ "write $personNames to 'file://personNames.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("biography_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("biographyId_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Set<SymbolicAssignment> sourceFKs = new HashSet<SymbolicAssignment>();
		sourceFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "biography_o"),
			JsonUtil.createPath("1", "biographyId_o")));
		Assert.assertEquals(sourceFKs, em.getSourceFKs());

		List<ObjectCreation> targetSchema =
			Lists.newArrayList(new ObjectCreation(), new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(2).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Set<SymbolicAssignment> targetFKs = new HashSet<SymbolicAssignment>();
		targetFKs.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("1", "id")));
		Assert.assertEquals(targetFKs, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		targetKeys.add(JsonUtil.createPath("2", "id"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			JsonUtil.createPath("0", "name_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testMinimalSchemaMappingWithFunction() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: concat($usCongressMembers.name_o, '---', $usCongressBiographies.worksFor_o),\n"
			+ "    worksFor_p: $usCongressMembers.id_o"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);

		List<ObjectCreation> sourceSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		sourceSchema.get(0).addMapping("name_o", EvaluationExpression.VALUE);
		sourceSchema.get(0).addMapping("id_o", EvaluationExpression.VALUE);
		sourceSchema.get(1).addMapping("worksFor_o", EvaluationExpression.VALUE);
		assertSchemaEquals(sourceSchema, em.getSourceSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getSourceFKs());

		List<ObjectCreation> targetSchema = Lists.newArrayList(new ObjectCreation(), new ObjectCreation());
		targetSchema.get(0).addMapping("name_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("worksFor_p", EvaluationExpression.VALUE);
		targetSchema.get(0).addMapping("id", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("name_l", EvaluationExpression.VALUE);
		targetSchema.get(1).addMapping("id", EvaluationExpression.VALUE);
		assertSchemaEquals(targetSchema, em.getTargetSchema());
		Assert.assertEquals(Collections.EMPTY_SET, em.getTargetFKs());
		
		Set<PathSegmentExpression> targetKeys = new HashSet<PathSegmentExpression>();
		targetKeys.add(JsonUtil.createPath("0", "id"));
		targetKeys.add(JsonUtil.createPath("0", "name_p"));
		targetKeys.add(JsonUtil.createPath("1", "id"));
		targetKeys.add(JsonUtil.createPath("1", "name_l"));
		Assert.assertEquals(targetKeys, new HashSet<PathSegmentExpression>(em.getTargetKeys()));

		Set<SymbolicAssignment> valueCorrespondences = new HashSet<SymbolicAssignment>();
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "name_p"),
			FunctionUtil.createFunctionCall(CoreFunctions.CONCAT, JsonUtil.createPath("0", "name_o"),
				new ConstantExpression("---"), JsonUtil.createPath("1", "worksFor_o"))));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("0", "worksFor_p"),
			JsonUtil.createPath("0", "id_o")));
		valueCorrespondences.add(new SymbolicAssignment(JsonUtil.createPath("1", "name_l"),
			JsonUtil.createPath("1", "worksFor_o")));
		Assert.assertEquals(valueCorrespondences, em.getSourceToValueCorrespondences());
	}

	@Test
	public void testMappingTaskEquals() {
		String query = "$usCongressMembers = read from 'file://usCongressMembers.json';\n"
			+ "$usCongressBiographies = read from 'file://usCongressBiographies.json';\n"
			+ "$person, $legalEntity = transform records $usCongressMembers, $usCongressBiographies\n"
			+ "into [\n"
			+ "  entity $person identified by $person.name_p with {"
			+ "    name_p: $usCongressMembers.name_o,\n"
			+ "    worksFor_p: $usCongressMembers.id_o"
			+ "  },"
			+ "  entity $legalEntity identified by $legalEntity.name_l with {"
			+ "    name_l: $usCongressBiographies.worksFor_o"
			+ "  }"
			+ "];\n"
			+ "write $person to 'file://person.json';\n"
			+ "write $legalEntity to 'file://legalEntity.json';";

		DataTransformation em = parseAndGetOperator(query);
		DataTransformation em2 = em.copy();
		Assert.assertEquals(em, em2);
	}
}