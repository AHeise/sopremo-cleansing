package eu.stratosphere.sopremo.cleansing.fusion;

import java.util.Map;
import java.util.Random;

import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

@Name(noun = "chooseRandom")
@SingleOutputResolution
public class ChooseRandomResolution extends ConflictResolution {

	public final static ChooseRandomResolution INSTANCE = new ChooseRandomResolution();
	private transient final Random rnd = new Random();

	@Override
	public void fuse(final IArrayNode<IJsonNode> values,
			final Map<String, CompositeEvidence> weights) {
		int rndIndex = rnd.nextInt(values.size());
		IJsonNode randomlyChosenValue = getValueFromSourceTaggedObject(values
				.get(rndIndex));
		values.clear();
		values.add(randomlyChosenValue);
	}
}
