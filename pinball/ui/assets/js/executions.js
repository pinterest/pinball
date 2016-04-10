function getClassType(exit_code) {
    if (typeof exit_code !== 'number') {
        return "text-info";
    } else if (exit_code === 0) {
        return "text-success";
    } else {
        return "text-danger";
    }
}

function getCreateRowFunc(showInstance) {
    return function(nRow, aData, iDataIndex) {
        // Optional workflow instance link.
        var offset = 0;
        if (showInstance) {
            offset = 1;
            var instanceHtml = '<a href="/jobs/?' +
                               'workflow=' + aData["workflow"] + '&' +
                               'instance=' + aData["instance"] + '">' +
                               aData["instance"] + '</a>';
            $("td:eq(0)", nRow).html(instanceHtml);
        }

        // Job execution instance link.
        var execution = aData["execution"];
        $("td:eq(" + offset + ")", nRow).html(
            '<a href="/execution/?workflow=' + aData["workflow"] + '&instance=' +
            aData["instance"] + '&job=' + aData["job"] + '&execution=' +
            execution + '">' + execution + '</a>');

        // Job properties.
        var jobProp = aData["properties"];
        var jobPropList = [];

        // Convert url prop to clickable link(prefer 'kv_job_url').
        var kvJobUrls = jobProp['kv_job_url'];
        var jobUrls = jobProp['job_url'];
        if (kvJobUrls && (kvJobUrls instanceof Array)) {
            for (var idx = 0; idx < kvJobUrls.length; idx++) {
                // Expected format: "url_anchor|url_link".
                var kvUrl = kvJobUrls[idx].split('|');
                jobPropList.push('<a href="' + kvUrl[1] + '">' + kvUrl[0] + '</a>');
            }
        } else if (jobUrls && (jobUrls instanceof Array)) {
            for (var idx = 0; idx < jobUrls.length; idx++) {
                var urlLink = jobUrls[idx];
                // For Qubole Hadoop1 MR job urls only.
                var urlText = urlLink.split('jobdetails.jsp%3Fjobid%3D')[1];
                jobPropList.push('<a href="' + urlLink + '">' + urlText + '</a>');
            }
        }

        // Handle remaining job properties.
        for (var propKey in jobProp) {
            if (jobProp.hasOwnProperty(propKey)) {
                // Ignore duplicated/verbose info about job id/url.
                if ((propKey === "job_url") ||
                    (propKey === "kv_job_url") ||
                    (propKey === "job_id") ||
                    startsWith(propKey, "job_")) {
                    continue;
                }

                // NOTE: value could be a scalar value or an array of string.
                var propValue = jobProp[propKey];
                if (propValue instanceof Array) {
                    jobPropList.push(propKey + '=' + propValue.join(','));
                } else {
                    jobPropList.push(propKey + '=' + propValue);
                }
            }
        }

        // NOTE: offset + 1 is reserved for job command line.
        var jobPropHtml = jobPropList.sort().join(", ");
        $("td:eq(" + (offset + 2) + ")", nRow).html(jobPropHtml);

        // Exit/Cleanup_Exit code.
        var exit_code = aData["exit_code"];
        if (aData["exit_code"] === null) {
            exit_code = '';
        }
        var class_type = getClassType(aData["exit_code"]);
        var exit_html = "<span class='" + class_type + "'>" + exit_code + "</span>";
        if (typeof aData["cleanup_exit_code"] === 'number') {
            exit_html += '<br/><b>Cleanup exit code: </b>';
            class_type = getClassType(aData["cleanup_exit_code"]);
            exit_html += "<span class='" + class_type + "'>";
            exit_html += aData["cleanup_exit_code"] + "</span>";
        }
        $("td:eq(" + (offset + 3) + ")", nRow).html(exit_html);

        // NOTE: offset + 4/5 are reserved for job start/end time.

        // Job run time.
        var run_time = aData["run_time"];
        if (run_time === null) {
            run_time = '';
        } else {
            run_time = jintervals(run_time, "{D}d {h}h {m}m {s}s");
        }
        $("td:eq(" + (offset + 6) + ")", nRow).html(run_time);

        // Log links.
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
