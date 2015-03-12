/**
 * Check if a string starts with a given prefix.
 * 
 * @param {string} s The string to match against.
 * @param {string} prefix The prefix to look for.
 * @return {boolean} True iff the string starts with the prefix.
 */
function startsWith(s, prefix) {
    return s.indexOf(prefix) === 0;
}

/**
 * Check if a string ends with a given prefix.
 * 
 * @param {string} s The string to match against.
 * @param {string} suffix The suffix to look for.
 * @return {boolean} True iff the string ends with the prefix.
 */
function endsWith(s, suffix) {
    return (s.lastIndexOf(suffix) >= 0)
        && (s.lastIndexOf(suffix) === s.length - suffix.length);
}

/**
 * Search for DOM with exact match on text.
 */
$.expr[":"].containsExact = function(obj, index, meta, stack) {
    return (obj.textContent || obj.innerText || $(obj).text() || "") == meta[3];
};

/**
 * Custom underscore function. Remove empty elements in an object.
 */
_.mixin({
    compactObject: function(o) {
        _.each(o, function(v, k) {
            if (typeof v != 'boolean' && !v)
                delete o[k];
        });
        return o;
    }
});

/**
 * Show loading modal.
 */
var loadingModal = function() {
    $('#modal-loading').modal({
        backdrop: 'static',
        keyboard: false,
        show: true
    });
    $('.modal-header').hide();
    $('.modal-footer').hide();
    $('.modal-dialog').css('width', '200px');
    $('.modal-dialog').css('height', '160px');
    $('.modal-body').css('height', '160px');
    opts = {
        lines: 13, // The number of lines to draw
        length: 20, // The length of each line
        width: 10, // The line thickness
        radius: 30, // The radius of the inner circle
        corners: 1, // Corner roundness (0..1)
        rotate: 0, // The rotation offset
        direction: 1, // 1: clockwise, -1: counterclockwise
        color: '#000', // #rgb or #rrggbb or array of colors
        speed: 1, // Rounds per second
        trail: 60, // Afterglow percentage
        shadow: false, // Whether to render a shadow
        hwaccel: false, // Whether to use hardware acceleration
        className: 'spinner', // The CSS class to assign to the spinner
        zIndex: 2e9, // The z-index (defaults to 2000000000)
        top: '2%', // Top position relative to parent in px
        left: '10%' // Left position relative to parent in px
    };
    var target = $('.modal-body')[0];
    var spinner = new Spinner(opts).spin(target);
};

var hideLoadingModal = function() {
    $('.spinner').remove();
    $('#modal-loading').modal('hide');
};

/**
 * Resize datatable to match header and content.
 */
$
    .extend(
        true,
        $.fn.dataTable.defaults,
        {
            "sDom": "r<'row'<'col-sm-6'l><'col-sm-6'f>>t<'row'<'col-sm-6'i><'col-sm-6'p>>",
            "bDestroy": true,
            "sScrollX": "150%",
            "fnInitComplete": function() {
                if (typeof oTable != 'undefined') {
                    oTable.fnAdjustColumnSizing();
                }
            }
        });

/**
 * Add 'new' label to Select2 selections.
 */
var formatSelection = function(item) {
    if (item.isNew) {
        return '<span class="label label-default">New</span> ' + item.text;
    } else {
        return item.text;
    }
};

/**
 * Allow user to input new value in Select2 field.
 */
var formatResult = function(result, container, query) {
    var text = result.text;
    var term = query.term;
    if (result.isNew) {
        return '<span class="label label-default">New</span> ' + term;
    } else {
        var markup = [];
        var marked = false;
        var match = text.toUpperCase().indexOf(term.toUpperCase()), tl = term.length;
        if (match < 0) {
            markup.push(text);
            return;
        }
        markup.push(text.substring(0, match));
        markup.push("<span class='select2-match'>");
        markup.push(text.substring(match, match + tl));
        markup.push("</span>");
        markup.push(text.substring(match + tl, text.length));
        return markup.join("");
    }
};

/**
 * Load list of workflows. Insert results in workflowElement and turn it into a
 * select box.
 */
var loadWorkflow = function(workflowElements) {
    loadingModal();
    json_url = '../ajax/workflow_names/';
    makeAjaxCall(json_url, '', function(json) {
        var data = [];
        _.each(json, function(workflowName, id) {
            data.push({
                id: 'workflow-' + id,
                text: workflowName
            });
        });
        var newTagMark = '<span class="label label-default">New</span> ';
        _.each(workflowElements, function(workflowElement) {
            $(workflowElement).select2({
                placeholder: 'Choose Or Create A Workflow...',
                allowClear: true,
                data: data,
                width: 'resolve',
                createSearchChoice: function(term, data) {
                    if ($(data).filter(function() {
                        return this.text.localeCompare(term) === 0;
                    }).length === 0) {
                        return {
                            id: 'workflow-new',
                            text: term,
                            isNew: true
                        };
                    }
                },
                formatSelection: formatSelection,
                formatResult: formatResult,
            });
        });
        hideLoadingModal();
    }, handleLoadingAjaxError);
};

/**
 * Load list of workflows. Does not allow creating new workflow.
 */
var loadWorkflowNoNew = function(workflowElements) {
    loadingModal();
    json_url = '../ajax/workflow_names/';
    makeAjaxCall(json_url, '', function(json) {
        var data = [];
        _.each(json, function(workflowName, id) {
            data.push({
                id: 'workflow-' + id,
                text: workflowName
            });
        });
        _.each(workflowElements, function(workflowElement) {
            $(workflowElement).select2({
                placeholder: 'Choose A Workflow...',
                allowClear: true,
                data: data,
                width: 'resolve',
                formatSelection: formatSelection,
                formatResult: formatResult,
            });
        });
        hideLoadingModal();
    }, handleLoadingAjaxError);
};

/**
 * Load list of jobs. Insert results in workflowElement and turn it into a
 * select box.
 */
var loadJob = function(existingWorkflow, jobElement) {
    loadingModal();
    json_url = '../ajax/job_names/?workflow=' + existingWorkflow;
    makeAjaxCall(json_url, '', function(json) {
        clearBox();
        var data = [];
        _.each(json, function(jobName, id) {
            data.push({
                id: 'job-' + id,
                text: jobName
            });
        });
        var newTagMark = '<span class="label label-default">New</span> ';
        $(jobElement).removeAttr('placeholder');
        $(jobElement).select2({
            placeholder: 'Choose Or Create A Job...',
            allowClear: true,
            data: data,
            width: 'resolve',
            createSearchChoice: function(term, data) {
                if ($(data).filter(function() {
                    return this.text.localeCompare(term) === 0;
                }).length === 0) {
                    return {
                        id: 'job-new',
                        text: term,
                        isNew: true
                    };
                }
            },
            formatSelection: formatSelection,
            formatResult: formatResult,
        });
        hideLoadingModal();
    }, handleLoadingAjaxError);
};

/**
 * Load list of jobs. Does not allow creating new job.
 */
var loadJobNoNew = function(existingWorkflow, jobElement) {
    loadingModal();
    json_url = '../ajax/job_names/?workflow=' + existingWorkflow;
    makeAjaxCall(json_url, '', function(json) {
        clearBox();
        var data = [];
        _.each(json, function(jobName, id) {
            data.push({
                id: 'job-' + id,
                text: jobName
            });
        });
        $(jobElement).removeAttr('placeholder');
        $(jobElement).select2({
            placeholder: 'Choose A Job...',
            allowClear: true,
            data: data,
            width: 'resolve',
            formatSelection: formatSelection,
            formatResult: formatResult,
        });
        hideLoadingModal();
    }, handleLoadingAjaxError);
};

/**
 * Get workflow details as json.
 */
var getWorkflowDetails = function(workflowName, callback) {
    json_url = '../ajax/schedule_config/?workflow=' + workflowName;
    $.getJSON(json_url).done(function(json) {
        callback(json);
    });
};

/**
 * Get job details as json.
 */
var getJobDetails = function(workflowName, jobName, callback) {
    json_url = '../ajax/job_config/?workflow=' + workflowName + '&job='
        + jobName;
    makeAjaxCall(json_url, null, callback, handleLoadingAjaxError);
};

/**
 * Populate form fields with a given model object.
 */
var loadForm = function(form, object) {
    _.each(object, function(value, key) {
        if (key in form.fields) {
            form.fields[key].editor.setValue(value);
        }
    });
};

/**
 * Clean form fields according to the given object.
 */
var cleanForm = function(form, object) {
    loadForm(form, object);
};
