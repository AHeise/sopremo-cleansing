package eu.stratosphere.sopremo.cleansing.mapping;

import static eu.stratosphere.sopremo.type.JsonUtil.*;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.meteor.MeteorIT;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.function.FunctionUtil;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * This test shall test mapping from one source to two entities.
 * 
 * @author fabian
 */

public class TransformRecordsTakeAllValuesTest extends MeteorIT {
	private File originalPersons, companies;

	@Before
	public void createFiles() throws IOException {
		this.originalPersons = this.testServer.createFile("originalPersons.json",
			createObjectNode("id", 1, "name", "Arvid", "worksFor", "HPI"),
			createObjectNode("id", 2, "name", "Tommy", "worksFor", "Arvid Inc."),
			createObjectNode("id", 4, "name", "Foobar", "worksFor", "Arvid Inc."),
			createObjectNode("id", 3, "name", "Fabian", "worksFor", "SAP"));
		this.companies = this.testServer.getOutputFile("companies.json");
	}

	@Test
	public void testTakeFirstValue() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor,\n" +
			"		   employee: $originalPersons.name\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		SopremoUtil.trace();
		this.client.submit(plan, null, true);

		this.testServer.checkContentsOf("companies.json",
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employee", "Tommy"),
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employee", "Foobar"),
			createObjectNode("id", "HPI", "name", "HPI", "employee", "Arvid"),
			createObjectNode("id", "SAP", "name", "SAP", "employee", "Fabian"));
	}

	@Test
	public void testTakeAllValues() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor,\n" +
			"		   employees: [$originalPersons.name]\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		SopremoUtil.trace();
		this.client.submit(plan, null, true);

		ObjectCreation canonicalizer = new ObjectCreation();
		canonicalizer.addMapping(new ObjectCreation.CopyFields(EvaluationExpression.VALUE));
		canonicalizer.addMapping("employees",
			FunctionUtil.createFunctionCall(CoreFunctions.SORT, new ObjectAccess("employees")));
		this.testServer.checkContentsOf("companies.json", canonicalizer,
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employees",
				JsonUtil.createArrayNode("Foobar", "Tommy")),
			createObjectNode("id", "HPI", "name", "HPI", "employees", JsonUtil.createArrayNode("Arvid")),
			createObjectNode("id", "SAP", "name", "SAP", "employees", JsonUtil.createArrayNode("Fabian")));
	}

	@Test
	public void testTakeAllObjects() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor,\n" +
			"		   employees: [{name: $originalPersons.name}]\n" +
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		SopremoUtil.trace();
		this.client.submit(plan, null, true);

		ObjectCreation canonicalizer = new ObjectCreation();
		canonicalizer.addMapping(new ObjectCreation.CopyFields(EvaluationExpression.VALUE));
		canonicalizer.addMapping("employees",
			FunctionUtil.createFunctionCall(CoreFunctions.SORT, new ObjectAccess("employees")));
		this.testServer.checkContentsOf("companies.json", canonicalizer,
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employees",
				createArrayNode(createObjectNode("name", "Foobar"), createObjectNode("name", "Tommy"))),
			createObjectNode("id", "HPI", "name", "HPI", "employees",
				JsonUtil.createArrayNode(createObjectNode("name", "Arvid"))),
			createObjectNode("id", "SAP", "name", "SAP", "employees",
				JsonUtil.createArrayNode(createObjectNode("name", "Fabian"))));
	}
	
	@Test
	public void testTakeAllObjectsInFunctions() throws IOException {
		String query = "using cleansing;\n" +
			"\n" +
			"$originalPersons = read from '" + this.originalPersons.toURI() + "';\n" +
			"\n" +
			"$companies  = transform records $originalPersons\n" +
			"	into [\n" +
			"	    entity $companies identified by $companies.name with {\n" +
			"	       name: $originalPersons.worksFor,\n" +
			"		   employees: [{name: $originalPersons.name}],\n" +
			"		   highestId: max([$originalPersons.id])"+
			"	    }\n" +
			"	];\n" +
			"\n" +
			"write $companies to '" + this.companies.toURI() + "';\n";

		final SopremoPlan plan = parseScript(query);

		SopremoUtil.trace();
		this.client.submit(plan, null, true);

		ObjectCreation canonicalizer = new ObjectCreation();
		canonicalizer.addMapping(new ObjectCreation.CopyFields(EvaluationExpression.VALUE));
		canonicalizer.addMapping("employees",
			FunctionUtil.createFunctionCall(CoreFunctions.SORT, new ObjectAccess("employees")));
		this.testServer.checkContentsOf("companies.json", canonicalizer,
			createObjectNode("id", "Arvid Inc.", "name", "Arvid Inc.", "employees",
				createArrayNode(createObjectNode("name", "Foobar"), createObjectNode("name", "Tommy")), "highestId", 4),
			createObjectNode("id", "HPI", "name", "HPI", "employees",
				JsonUtil.createArrayNode(createObjectNode("name", "Arvid")), "highestId", 1),
			createObjectNode("id", "SAP", "name", "SAP", "employees",
				JsonUtil.createArrayNode(createObjectNode("name", "Fabian")), "highestId", 3));
	}
}
