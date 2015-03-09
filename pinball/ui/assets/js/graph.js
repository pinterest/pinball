/**
 * Construct a graph from the svg topology.
 * @return {Array} An associative array mapping node name to a structure with
 *     a lists of children of that node.
 */
function graphFromSvg() {
    var graph = {};
    $("#graph svg g.node").each(function() {
        graph[$(this).attr("id")] = {
            children: []
        };
    });

    $("#graph svg g.edge").each(function() {
        var edge = $(this).attr("id").split("/");
        var parent = edge[0];
        var child = edge[1];
        graph[parent].children.push(child);
    });
    return graph;
}

/**
 * Traverse the graph in dfs fashion starting at a given node.
 * @param {string} current The next node to traverse.
 * @param {string[]} visited The list of visited nodes.
 * @param {Array} graph The graph topology. 
 */
function dfs(current, visited, graph) {
    if (visited.indexOf(current) === -1) {
        visited.push(current);
        var children = graph[current].children;
        for (var i = 0; i < children.length; ++i) {
            var childName = children[i];
            dfs(childName, visited, graph);
        }
    }
}

/**
 * Find direct and indirect dependants of a given root node.
 * @param {string} root The root node whose dependents should be retrieved.
 * @param {Array} graph The graph topology to traverse.
 * @return {string[]} The list of dependents of the root node.
 */
function findDependents(root, graph) {
    var visited = [];
    dfs(root, visited, graph);
    return visited;
}
