package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.paths.PathExpression;

import java.util.ArrayList;
import java.util.List;

public class MappingUtil {
	public static List<SpicyPathExpression> extractPathFrom(List<PathExpression> fromPaths) {
		List<SpicyPathExpression> paths = new ArrayList<SpicyPathExpression>(fromPaths.size());
		for (PathExpression currentPath : fromPaths)
			paths.add(new SpicyPathExpression(currentPath));
		return paths;
	}

	public static List<PathExpression> createPaths(List<SpicyPathExpression> paths) {
		List<PathExpression> spicyPaths = new ArrayList<PathExpression>(paths.size());
		for (SpicyPathExpression currentPath : paths) {
			spicyPaths.add(currentPath.getPathExpression());
		}
		return spicyPaths;
	}
}
