package eu.stratosphere.sopremo.cleansing;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * Input elements are either
 * <ul>
 * <li>Array of records resulting from record linkage without transitive closure. [r1, r2, r3] with r<sub>i</sub>
 * <li>Array of record clusters resulting from record linkage with transitive closure
 * </ul>
 * 
 * @author Arvid Heise
 */
@Name(verb = "fuse")
public class Fusion extends CompositeOperator<Fusion> {

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(eu.stratosphere.sopremo.operator.SopremoModule
	 * , eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module, EvaluationContext context) {
	}

}
