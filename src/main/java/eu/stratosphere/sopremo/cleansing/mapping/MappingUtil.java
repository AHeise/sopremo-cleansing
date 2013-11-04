package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.paths.PathExpression;

import java.util.LinkedList;
import java.util.List;

public class MappingUtil {
	public static List<List<String>> extractPathFrom(List<PathExpression> fromPaths) {
		List<List<String>> paths = new LinkedList<List<String>>();
		for (PathExpression currentPath : fromPaths) {
			List<String> pathSteps = currentPath.getPathSteps();
			paths.add(pathSteps);
		}
		return paths;
	}

	public static List<PathExpression> createPaths(List<List<String>> paths) {
		List<PathExpression> spicyPaths = new LinkedList<PathExpression>();
		for (List<String> currentPath : paths) {
			spicyPaths.add(new PathExpression(currentPath));
		}
		return spicyPaths;
	}

	public static PathExpression createPathExpression(List<String> sourcePath) {
		return new PathExpression(sourcePath);
	}
}
