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
import it.unibas.spicy.model.paths.SetAlias;
import it.unibas.spicy.model.paths.VariablePathExpression;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.sopremo.expressions.PathSegmentExpression;


/**
 * @author Andrina Mascher, Arvid Heise
 *
 */
public class EntityMappingUtil {

	public static PathSegmentExpression convertSpicyPath(String inputIndex, VariablePathExpression spicyPath) {
		/* e.g. spicyPath 					= 	v0.usCongressMember.biography
		 * spicyPath.getAbsolutePath()	 	= 	usCongress.usCongressMembers.usCongressMember.biography
		 * spicyPath.getPathSteps()			=	[usCongressMember, biography]
		 * spicyPath.getStartingVariable()	=	v0 in usCongress.usCongressMembers
		 */

		// create steps such as [0, v0, biography]
		List<String> pathSteps = new ArrayList<String>(); 
		if( inputIndex!=null ) {
			pathSteps.add( inputIndex ); //needed for multiple input operator e.g. join input 0 or 1
		} else {
			pathSteps.add( "0" ); 
		}
		
		pathSteps.addAll(getRelevantPathSteps(spicyPath));
		
		return createPath(pathSteps); 
	}
	
	public static List<String> getRelevantPathSteps(VariablePathExpression spicyPath) {
		List<String> steps = new ArrayList<String>();
		steps.add( "["+String.valueOf(spicyPath.getStartingVariable().getId()) +"]");
		for(int i=1; i<spicyPath.getPathSteps().size(); i++) { //always ignore [0], is replaced by sourceId v0
			steps.add( spicyPath.getPathSteps().get(i) );			
		}
		return steps;
	}
	
	public static List<String> getRelevantPathStepsWithoutInput(VariablePathExpression spicyPath) {
		List<String> steps = new ArrayList<String>();
		steps.add(String.valueOf(spicyPath.getStartingVariable().getId()));
		for(int i=1; i<spicyPath.getPathSteps().size(); i++) { //always ignore [0], is replaced by sourceId v0
			steps.add( spicyPath.getPathSteps().get(i) );			
		}
		return steps;
	}
	
	public static int getSourceIdForArrayAccess(SetAlias setAlias) { 
		return setAlias.getId(); //e.g. "v0" as used by spicy
	}
}
