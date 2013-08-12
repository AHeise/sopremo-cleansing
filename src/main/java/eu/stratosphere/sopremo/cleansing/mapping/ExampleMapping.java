/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import static eu.stratosphere.sopremo.type.JsonUtil.createPath;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.base.TwoSourceJoin;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression.BinaryOperator;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * @author Arvid Heise
 */
@InputCardinality(2)
@OutputCardinality(2) //TODO
public class ExampleMapping extends CompositeOperator<ExampleMapping> {

	private DuplicateDetectionImplementation implementation;
	
	/**
	 * Returns the implementation.
	 * 
	 * @return the implementation
	 */
	public DuplicateDetectionImplementation getImplementation() {
		return this.implementation;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.operator.CompositeOperator#asModule(eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) { 
		//TODO input is list of FORule
		//for now: input 0 = members, input 1 = biographies
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		/*	Join(sourceMem, sourceBio) on FK
		 *	-> joinMemBio (defines names according to sources)
		 */
		ObjectCreation joinTransformation = new ObjectCreation().
				addMapping("uscongressMember", new ObjectCreation().
					addMapping("id", createPath("0","id")).  //TODO any way to keep all values of current entry in input(0) and input(1)?
					addMapping("name", createPath("0","name")). 
					addMapping("biography", createPath("0","biography")) ).
				addMapping("uscongressBiography", new ObjectCreation().
					addMapping("biographyId", createPath("1","biographyId")). 
					addMapping("worksFor", createPath("1","worksFor"))
				);
		TwoSourceJoin joinMemBio = new TwoSourceJoin().
				withCondition(new ComparativeExpression(createPath("0", "biography"), BinaryOperator.EQUAL, createPath("1", "biographyId"))).
				withInputs(module.getInputs()). //not needed if embedded in module
				withResultProjection(joinTransformation); //automatically all fields from both sources, hopefully different names, what if duplicates?

		/////////////////////////////////////////////////////////////////////////////////////////////////////
		/*	AntiJoin (sourceBio, joinMemBio)
			(for each b in biography try to find join partner in JoinedMemBio,
			if no partner found: return b)
			-> Map to fill other attributes with NULL values, define target names
			-> remainingBio 
		*/
		final ObjectCreation transformation = new ObjectCreation();
				transformation.addMapping("id", ConstantExpression.NULL );
				transformation.addMapping("name", createPath("0", "worksFor"));
		TwoSourceJoin remainingFromBio = new TwoSourceJoin().
				withCondition( new ElementInSetExpression(createPath("0", "biographyId"), Quantor.EXISTS_NOT_IN, createPath("1", "uscongressMember", "biography"))).
				withResultProjection(transformation).
				withInputs(module.getInput(1), joinMemBio);	//input 0 = sourceB and input 1 = join

		/////////////////////////////////////////////////////////////////////////////////////////////////////
		/*	joinMemBio
			-> define target names and skolemFunctions for FK/PK in person.worksFor and LE.id
			-> tgdWithFK
		 */
		ObjectCreation tgdWithFKSchema = new ObjectCreation().
				addMapping("person", new ObjectCreation().
					addMapping("id", createPath("0", "uscongressMember", "id")). 
					addMapping("name", createPath("0", "uscongressMember", "name")). 
					addMapping("worksFor", skolemWorksFor() )
							).
				addMapping("legalEntity", new ObjectCreation().
					addMapping("id", skolemWorksFor() ). 
					addMapping("name", createPath("0", "uscongressBiography", "worksFor"))
							);
		Projection tgdWithFK = new Projection().
				withResultProjection(tgdWithFKSchema).				
				withInputs(joinMemBio);

		/////////////////////////////////////////////////////////////////////////////////////////////////////
		/*
		tgdWithFK 
		(-> skolemFunctions for tupleId, setId)
		-> removeDup 
		-> sinkP
		*/
		
		ObjectCreation personSchemaCreation = new ObjectCreation().addMapping(new ObjectCreation.CopyFields(createPath("person")));			
		Projection personSchema = new Projection().
				withInputs(tgdWithFK).
				withResultProjection(personSchemaCreation);
		Union person = new Union().   //Union removes exact duplicates TODO use Unique later
				withInputs(personSchema);	
		
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		/*
		Union(
				remainingBio -> SkolemFunctions for tupleId, setId,
				tgdWithFK -> SkolemFunctions for tupleId, setId) 
		-> removeDup
		-> sinkLE
		 */
		
		Projection leSchema1 = new Projection().
				withInputs(remainingFromBio);
		
		ObjectCreation leSchemaCreation = new ObjectCreation().addMapping(new ObjectCreation.CopyFields(createPath("legalEntity")));			
		Projection leSchema2 = new Projection().
				withInputs(tgdWithFK).
				withResultProjection(leSchemaCreation);
		
		Union legalEntity = new Union().  
				withInputs(leSchema1, leSchema2);	
				
		/////////////////////////////////////////////////////////////////////////////////////////////////////
		
//		module.getOutput(0).setInput(0, joinMemBio);
//		module.getOutput(0).setInput(0, remainingFromBio);
//		module.getOutput(0).setInput(0, tgdWithFK);
		
//		module.getOutput(0).setInput(0, person);
//		module.getOutput(0).setInput(0, legalEntity);
		
		module.getOutput(0).setInput(0, person);
		module.getOutput(1).setInput(0, legalEntity);
	}

	private EvaluationExpression skolemWorksFor() {
		return createPath("0", "uscongressMember", "biography"); //TODO
	}

}
