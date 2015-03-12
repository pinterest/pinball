/**
 * Reset the default style of edges in the svg.
 */
function resetEdgeColors() {
    var path = $("#graph svg g.edge path");
    path.attr("stroke", "black");
    path.removeAttr("stroke-width");
}

/**
 * Highlight incoming and outgoing edges for the selected node.
 * 
 * @param {string} node The name of the node whose edges should be highlighted.
 */
function highlightEdges(node) {
    return function() {
        var edge_name = $(this).attr("id");
        if (startsWith(edge_name, node + "/")) {
            $(this).children("path").attr("stroke", "green");
            $(this).children("path").attr("stroke-width", "3");
        } else if (endsWith(edge_name, "/" + node)) {
            $(this).children("path").attr("stroke", "blue");
            $(this).children("path").attr("stroke-width", "3");
        }
    };
}

/**
 * Highlight a node with a given name.
 * 
 * @param {string} node The name of the node to highlight.
 */
function highlightNode(node) {
    var shape = $("#" + node).children("ellipse, polygon");
    shape.attr("stroke-width", "3");
    shape.attr("stroke", "red");
}

/**
 * Remove highlight from a node with a given name.
 * 
 * @param {string} node The name of the node to remove highlight from.
 */
function resetNode(node) {
    var shape = $("#" + node).children("ellipse, polygon");
    shape.removeAttr("stroke-width");
    shape.attr("stroke", "black");
}

/**
 * Reverse the highlight of a node with a given name.
 * 
 * @param {string} node The name of the node to reverse highlight on.
 */
function flipNodeSelection(node) {
    var nodeObject = $("#" + node);
    var selected;
    if (nodeObject.children("ellipse, polygon").attr("stroke-width")) {
        resetNode(node);
        selected = false;
    } else {
        highlightNode(node);
        selected = true;
    }
    resetEdgeColors();
    if (selected) {
        $("#graph svg g.edge").each(highlightEdges(node));
        if ($('#parents').length > 0) {
            // For parent selection, concat text of new selected node.
            if ($("#parents").val() === '') {
                $("#parents").val(node);
            } else {
                parents = $("#parents").val().split(', ');
                parents.push(node);
                $("#parents").val(parents.join(', '));
            }
        }
    } else if ($('#parents').length > 0) {
        // For parent selection, remove text of last selected node.
        parents = $("#parents").val().split(", ");
        parents = $.grep(parents, function(value) {
            return value != node;
        });
        $("#parents").val(parents.join(", "));
    }
}

/**
 * Handler for the svg node click event. Flip the selection of the clicked node.
 */
function nodeClick() {
    flipNodeSelection($(this).attr("id"));
}

/**
 * Ignore an event and prevent it from bubbling.
 * 
 * @param {Event} e The event to ignore.
 */
function ignore(e) {
    e.stopPropagation();
}

function centerElement(text, fullGraph) {
    var workflowGraph = $("#workflow_graph");
    var workflowGraphDom = workflowGraph[0].getBBox();
    var graphWidth = workflowGraphDom.width;
    var graphHeight = workflowGraphDom.height;

    var element = $("#" + text)[0];
    var eWidth = element.getBBox().width;
    var eHeight = element.getBBox().height;
    var eX = element.getBBox().x;
    var eY = element.getBBox().y;
    var eCenterX = eWidth / 2 + eX;
    var eCenterY = eHeight / 2 + eY;

    var graph = $("#graph");
    var widthCenter = graph.width() / 2;
    var heightCenter = graph.height() / 2;

    var scale = 60 / eHeight;
    if (fullGraph) {
        scale = computeScale(eWidth, eHeight, $('#graph')
        .width(), $('#graph').height());
        var singleElementScale = 60 / $('#graph g').last()[0].getBBox().height;
        if (singleElementScale < scale) {
            scale = singleElementScale;
        }
    }
    horizontal = widthCenter - eCenterX * scale;
    vertical = heightCenter - eCenterY * scale;
    translate = " translate(" + horizontal + "," + vertical + ")";
    scale = " scale(" + scale + ", " + scale + ")";
    workflowGraph.animate({
        "svgTransform": translate + scale
    }, 1000);
}

/**
 * Compute svg resizing scale.
 * 
 * @param {number} original_width The width of the original object.
 * @param {number} original_height The height of the original object.
 * @param {number} resized_width The width of the resized object.
 * @param {number} resized_height The height of the resized object.
 * @return {number} The scale between scaling factor of resized vs original
 *         value minimized across width and height.
 */
function computeScale(original_width, original_height, resized_width,
resized_height) {
    var width_scale = resized_width / original_width;
    var height_scale = resized_height / original_height;
    return Math.min(width_scale, height_scale);
}

/**
 * Callback run after the svg loaded.
 */
function graphLoaded() {
    var graph = $("#graph svg");
    // graph.width() and graph.height() don't return the right thing.
    var original_width = parseInt(graph.attr("width"), 10);
    var original_height = parseInt(graph.attr("height"), 10);
    graph.removeAttr("viewBox");
    graph.width("100%");
    graph.height("500px");
    centerElement($('#graph g:first-child').attr('id'), true);

    $("#graph svg g.node").click(nodeClick);

    // Strange things happen when the svg is dragged by the nodes so we disable
    // this by default.
    $("#graph svg g.node").bind("mousedown", ignore);

    // Graph id should be in sync with the value defined in
    // workflow_graph.py
    $("#graph svg").svgPan("workflow_graph");

    addContextMenus();
    addSearchBox();
}

/**
 * Retrieve selected svg node names representing jobs.
 * 
 * @return {string[]} Names of selected jobs.
 */
function getSelectedJobs() {
    var result = [];
    $("#graph svg g.node").each(function() {
        if ($(this).children("ellipse").attr("stroke-width")) {
            result.push($(this).attr("id"));
        }
    });
    return result;
}

/**
 * Show selected job names inside an element.
 */
function showJobs() {
    $("span[id$='-jobs']").html(getSelectedJobs().join(", "));
}

/**
 * Remove leading and trailing whitespace from a string.
 * 
 * @param {string} s The string to trim.
 * @return {string} The trimmed string.
 */
function trim(s) {
    return s.replace(/^\s+|\s+$/g, '');
}

/**
 * Show a message and thrown an exception if the condition is false.
 * 
 * @param {Boolean} condition The condition to check.
 * @param {string} message The message to show if condition is false.
 */
function assert(condition, message) {
    if (!condition) {
        throw message || "Assertion failed";
    }
}

/**
 * Handle context menu selection.
 * 
 * @param {string} node The name of the node where the menu is attached to.
 * @param {Array} graph The graph describing dependencies between nodes.
 */
function contextMenu(node, graph) {
    return function(e, item) {
        var text = trim(item.text());
        assert(text === "Go to job" || text === "Select with dependents"
        || text === "Unselect with dependents");
        if (text === "Go to job") {
            var instance = null;
            var location_regex = location.search.match(/instance=([0-9A-z_]*)/);
            if (location_regex === null) {
                return null;
            }
            if (location_regex !== null) {
                instance = location_regex[1];
            }
            var workflow = location.search.match(/workflow=([0-9A-z_]*)/)[1];
            location.href = "/executions/?workflow=" + workflow + "&instance="
            + instance + "&job=" + node;
        }
        var dependents = findDependents(node, graph);
        if (text !== "Select with dependents"
        && text !== "Unselect with dependents") {
            return;
        }
        var select = (text === "Select with dependents");
        for ( var i = 0; i < dependents.length; ++i) {
            if (select) {
                highlightNode(dependents[i]);
            } else {
                resetNode(dependents[i]);
            }
        }
    };
}

/**
 * Add context menus to all svg node elements.
 */
function addContextMenus() {
    var graph = graphFromSvg();
    $("#graph svg g.node").each(function() {
        var nodeId = $(this).attr("id");
        var menu = $("#job-context-menu-template").clone();
        var menuId = "job-context-menu-" + nodeId;
        menu.attr("id", menuId);
        menu.appendTo("#context-menus");
        $(this).contextmenu({
            target: "#" + menuId,
            onItem: contextMenu($(this).attr("id"), graph)
        });
    });
}

/**
 * Configure search box for the SVG graph.
 */
function addSearchBox() {
    var data = [];
    _.each($("#graph ellipse").siblings("title"), function(title, id) {
        data.push({
            id: 'graph-search-' + id,
            text: $(title).text()
        });
    });
    // Need to slice because the first polygon is the canvas.
    _.each($("#graph polygon").siblings("title").slice(1), function(title, id) {
        data.push({
            id: 'graph-search-' + id,
            text: $(title).text()
        });
    });
    $("#graph-search-box").select2({
        data: data,
    });
    $("#graph-search-box").on("select2-selecting", function(e) {
        centerElement(e.object.text, false);
    });
}
