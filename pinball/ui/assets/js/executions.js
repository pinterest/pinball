function getClassType(exit_code) {
    if (typeof exit_code !== 'number') {
        return "text-info";
    } else if (exit_code === 0) {
        return "text-success";
    } else {
        return "text-danger";
    }
}

function addLogsLinks(showInstance) { return function(nRow, aData,
                                                      iDataIndex) {
    var offset = 0;
    if (showInstance) {
        offset = 1;
        var instanceHtml = '<a href="/jobs/?' +
                           'workflow=' + aData["workflow"] + '&' +
                           'instance=' + aData["instance"] + '">' +
                           aData["instance"] + '</a>';
        $("td:eq(0)", nRow).html(instanceHtml);
    }
    var execution = aData["execution"];
    $("td:eq(" + offset + ")", nRow).html(
        '<a href="/execution/?workflow=' + aData["workflow"] + '&instance=' +
        aData["instance"] + '&job=' + aData["job"] + '&execution=' +
        execution + '">' + execution + '</a>');
    var properties = aData["properties"];
    var propertiesList = [];
    for(var key in properties) {
        if (properties.hasOwnProperty(key)){
            var value = properties[key];
            var customizedValue = value;
            if (endsWith(key, "_url")) {
                customizedValue = '<a href="' + value + '">' + value +
                    '</a>';
            }
            propertiesList.push(key + "=" + customizedValue);
        }
    }
    var propertiesHtml = propertiesList.join(", ");
    $("td:eq(" + (offset + 2) + ")", nRow).html(propertiesHtml);

    var exit_code = aData["exit_code"];
    if (aData["exit_code"] === null) {
        exit_code = '';
    }
    var class_type;
    var exit_html;

    class_type = getClassType(aData["exit_code"]);
    exit_html = "<span class='" + class_type + "'>" + exit_code + "</span>";

    if (typeof aData["cleanup_exit_code"] === 'number') {
        exit_html += '<br/><b>Cleanup exit code: </b>';
        class_type = getClassType(aData["cleanup_exit_code"]);
        exit_html += "<span class='" + class_type + "'>";
        exit_html += aData["cleanup_exit_code"] + "</span>";
    }
    $("td:eq(" + (offset + 3) + ")", nRow).html(exit_html);

    var run_time = aData["run_time"];
    if (run_time === null) {
        run_time = '';
    } else {
        run_time = jintervals(run_time, "{D}d {h}h {m}m {s}s");
    }
    $("td:eq(" + (offset + 6) + ")", nRow).html(run_time);
    var logs = aData["logs"];
    var logs_html = '<ul class="inline">';
    for (var log in logs) {
        logs_html += '<li><a href="/file/?workflow=' + aData["workflow"] +
            '&instance=' + aData["instance"] + '&job=' + aData["job"] +
            '&execution=' + aData["execution"] + '&log_type=' + log +
            '"><i class="icon-file"></i>' + log + '</a></li>';
    }
    logs_html += '</ul>';
    $("td:eq(" + (offset + 7) + ")", nRow).html(logs_html);
};
}
