;
(function(editors) {
    /**
     * ButtonGroup editor Expect schema.choices: [{val: false, label: 'Job'}]
     */
    editors.ButtonGroup = editors.Base
        .extend({
            events: _.extend({}, editors.Base.prototype.events, {
                'change': function(event) {
                    var self = this;
                    setTimeout(function() {
                        self.determineChange();
                    }, 0);
                },
            }),

            determineChange: function(event) {
                var currentValue = this.getValue();
                var changed = (currentValue !== this.previousValue);

                if (changed) {
                    this.previousValue = currentValue;
                    this.trigger('change', this);
                }
            },

            initialize: function(options) {
                editors.Base.prototype.initialize.call(this, options);
            },

            getValue: function() {
                index = parseInt(this.$el.find('.active').children().attr(
                    'index'));
                return _.isNaN(index) ? undefined
                    : this.schema.choices[index].val;
            },

            /**
             * Clear the button state with value = -1
             */
            setValue: function(value) {
                if (typeof (value) == 'boolean') {
                    if (value == false) {
                        $(this.$el.children().children()[0]).button('toggle');
                    } else {
                        $(this.$el.children().children()[1]).button('toggle');
                    }
                } else if (typeof (value) == 'number') {
                    if (this.$el.children().children().length <= value
                        || value == -1) {
                        this.$el.children().children().removeClass('active');
                    } else {
                        $(this.$el.children().children()[value]).button(
                            'toggle');
                    }
                }
            },

            render: function() {
                editors.Base.prototype.render.apply(this, arguments);
                var buttonGroup = this.$el
                    .append(
                        '<div '
                            + 'class="btn-group btn-group-justified" data-toggle="buttons"'
                            + '></div>').children();
                _.each(this.schema.choices, function(choice, index) {
                    label = buttonGroup.append(
                        '<label class="btn btn-default"></label>').children()
                        .last();
                    label.append('<input type="radio" name="options" index="'
                        + index + '"> ' + choice.label);
                });
                return this;
            },
        });

    editors.KeyValueText = editors.Base.extend({
        initialize: function(options) {
            options = options || {};
            editors.Base.prototype.initialize.call(this, options);
            valuePair = options.value == undefined ? ['', ''] : options.value;

            // Option defaults
            this.options = _.extend({
                FormText: editors.FormText,
                FormTextarea: editors.FormTextarea,
            }, options);

            options.schema = _.extend({
                placeholder: this.schema.keyPlaceholder
            }, options.schema);
            options.value = valuePair[0];
            this.textEditor1 = new this.options.FormText(options);
            this.textEditor1.$el.removeAttr('name');

            options.schema.placeholder = this.schema.valuePlaceholder;
            options.value = valuePair[1];
            this.textEditor2 = new this.options.FormTextarea(options);
            this.textEditor2.$el.removeAttr('name');

            this.$hidden1 = $('<input>', {
                type: 'hidden',
                name: options.key
            });
            this.$hidden2 = $('<input>', {
                type: 'hidden',
                name: options.value
            });

            // Both editors contains all values
            this.value = this.textEditor1.value;
            this.setValue(this.value);
        },

        getValue: function() {
            this.updateHidden();
            return value = [this.$hidden1.val(), this.$hidden2.val()];
        },

        setValue: function(value) {
            this.textEditor1.setValue(value[0]);
            this.textEditor2.setValue(value[1]);
        },

        updateHidden: function() {
            this.$hidden1.val(this.textEditor1.getValue());
            this.$hidden2.val(this.textEditor2.getValue());
        },

        render: function() {
            editors.Base.prototype.render.apply(this, arguments);
            this.$el.append(this.textEditor1.render().el);
            this.$el.append(this.textEditor2.render().el);
            this.updateHidden();
            this.$el.append(this.$hidden1);
            this.$el.append(this.$hidden2);
            return this;
        },

        focus: function() {
            if (this.hasFocus)
                return;
            this.$el.focus();
        },
    });

    /**
     * TextArea editor
     */
    editors.FormTextarea = editors.Text.extend({

        tagName: 'textarea',

        initialize: function(options) {
            editors.Base.prototype.initialize.call(this, options);
            this.$el.addClass('form-control');
            this.$el.attr('placeholder', options.schema.placeholder);
        },
    });

    /**
     * Double text editor for two textbox in the same row. Output is array of
     * two textboxes.
     */
    editors.DoubleText = editors.Base.extend({
        initialize: function(options) {
            options = options || {};
            editors.Base.prototype.initialize.call(this, options);

            // Option defaults
            this.options = _.extend({
                FormText: editors.FormText,
            }, options);

            var id = this.schema.id;
            placeholder = this.schema.placeholder;
            options.schema.placeholder = placeholder[0];
            options.id = id[0];
            this.textEditor1 = new this.options.FormText(options);
            this.textEditor1.$el.removeAttr('name');

            options.schema.placeholder = placeholder[1];
            options.id = id[1];
            this.textEditor2 = new this.options.FormText(options);
            this.textEditor2.$el.removeAttr('name');

            this.$hidden = $('<input>', {
                type: 'hidden',
                name: options.key
            });

            this.value = this.textEditor1.value;
            this.setValue(this.value);
        },

        getValue: function() {
            this.updateHidden();
            return this.$hidden.val().split(',');
        },

        setValue: function(value) {
            if (value == undefined || value == '') {
                return;
            }
            this.textEditor1.setValue(value[0]);
            this.textEditor2.setValue(value[1]);
        },

        updateHidden: function() {
            this.$hidden.val([this.textEditor1.getValue(),
                this.textEditor2.getValue()]);
        },

        render: function() {
            editors.Base.prototype.render.apply(this, arguments);
            var newDiv = this.$el.append(
                '<div class="col-sm-6 doubletext"></div>').children().last();
            newDiv.append($(this.textEditor1.render().el));
            editors.Base.prototype.render.apply(this, arguments);
            newDiv = this.$el.append('<div class="col-sm-6 doubletext"></div>')
                .children().last();
            newDiv.append($(this.textEditor2.render().el));
            this.updateHidden();
            this.$el.append(this.$hidden);
            return this;
        },

        focus: function() {
            if (this.hasFocus)
                return;
            this.$el.focus();
        },
    });

    /**
     * Number editor for time. Including week, day, hour, minute. Output is
     * string: 1w4d1H5M
     */
    editors.TimeTextString = editors.Base.extend({
        initialize: function(options) {
            options = options || {};
            editors.Base.prototype.initialize.call(this, options);

            // Option defaults
            this.options = _.extend({
                Number: editors.Number,
            }, options);

            options.schema = _.extend({
                placeholder: this.schema.keyPlaceholder
            }, options.schema);
            this.textEditor1 = new this.options.Number(options);
            this.textEditor1.$el.removeAttr('name');

            options.schema.placeholder = this.schema.valuePlaceholder;
            this.textEditor2 = new this.options.Number(options);
            this.textEditor2.$el.removeAttr('name');

            options.schema.placeholder = this.schema.valuePlaceholder;
            this.textEditor3 = new this.options.Number(options);
            this.textEditor3.$el.removeAttr('name');

            options.schema.placeholder = this.schema.valuePlaceholder;
            this.textEditor4 = new this.options.Number(options);
            this.textEditor4.$el.removeAttr('name');

            this.$hidden = $('<input>', {
                type: 'hidden',
                name: options.key
            });

            this.value = this.textEditor1.value;
        },

        getValue: function() {
            this.updateHidden();
            var value = this.$hidden.val().split(',');
            var letter = ['w', 'd', 'H', 'M'];
            var valueString = "";
            _.each(_.range(4), function(i) {
                if (value[i] != '') {
                    valueString = valueString + value[i] + letter[i];
                }
            });
            return valueString;
        },

        setValue: function(value) {
            if (value == undefined || value == '') {
                return;
            }
            var week = value.split('w');
            if (week.length > 1) {
                this.textEditor1.setValue(week[0]);
                week = week[1];
            } else {
                this.textEditor1.setValue('');
                week = week[0];
            }
            var day = week.split('d');
            if (day.length > 1) {
                this.textEditor2.setValue(day[0]);
                day = day[1];
            } else {
                this.textEditor2.setValue('');
                day = day[0];
            }
            var hour = day.split('H');
            if (day.length > 1) {
                this.textEditor3.setValue(hour[0]);
                hour = hour[1];
            } else {
                this.textEditor3.setValue('');
                hour = hour[0];
            }
            var minute = hour.split('M');
            if (minute.length > 1) {
                this.textEditor4.setValue(minute[0]);
            } else {
                this.textEditor4.setValue('');
            }
        },

        updateHidden: function() {
            this.$hidden.val([this.textEditor1.getValue(),
                this.textEditor2.getValue(), this.textEditor3.getValue(),
                this.textEditor4.getValue()]);
        },

        render: function() {
            editors.Base.prototype.render.apply(this, arguments);
            var newDiv = this.$el.append('<div class="col-sm-2"></div>')
                .children().last();
            newDiv.append($(this.textEditor1.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Week</div>');
            editors.Base.prototype.render.apply(this, arguments);
            newDiv = this.$el.append('<div class="col-sm-2"></div>').children()
                .last();
            newDiv.append($(this.textEditor2.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Day</div>');
            editors.Base.prototype.render.apply(this, arguments);
            newDiv = this.$el.append('<div class="col-sm-2"></div>').children()
                .last();
            newDiv.append($(this.textEditor3.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Hour</div>');
            editors.Base.prototype.render.apply(this, arguments);
            newDiv = this.$el.append('<div class="col-sm-2"></div>').children()
                .last();
            newDiv.append($(this.textEditor4.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Minute</div>');
            this.updateHidden();
            this.$el.append(this.$hidden);
            this.setValue(this.value);
            return this;
        },

        focus: function() {
            if (this.hasFocus)
                return;
            this.$el.focus();
        },
    });

    /**
     * Number editor for time. Including week, day, hour, minute. Output is
     * number of seconds.
     */
    editors.TimeText = editors.TimeTextString.extend({
        getValue: function() {
            this.updateHidden();
            var value = this.$hidden.val().split(',');
            var valueSec = 0;
            if (!_.isNaN(value[3])) {
                valueSec = valueSec + value[3] * 60;
            }
            if (!_.isNaN(value[2])) {
                valueSec = valueSec + value[2] * 3600;
            }
            if (!_.isNaN(value[1])) {
                valueSec = valueSec + value[1] * 86400;
            }
            if (!_.isNaN(value[0])) {
                valueSec = valueSec + value[0] * 604800;
            }
            return valueSec == 0 ? undefined : valueSec;
        },

        setValue: function(value) {
            if (value == undefined || value == '') {
                return;
            }
            value = parseInt(parseInt(value) / 60);
            var minute = value % 60;
            if (minute != 0) {
                this.textEditor4.setValue(minute);
            } else {
                this.textEditor4.setValue('');
            }
            value = (value - minute) / 60;
            var hour = value % 24;
            if (hour != 0) {
                this.textEditor3.setValue(hour);
            } else {
                this.textEditor3.setValue('');
            }
            value = (value - hour) / 24;
            var day = value % 7;
            if (day != 0) {
                this.textEditor2.setValue(day);
            } else {
                this.textEditor2.setValue('');
            }
            var week = (value - day) / 7;
            if (week != 0) {
                this.textEditor1.setValue(week);
            } else {
                this.textEditor1.setValue('');
            }
        },

        updateHidden: function() {
            this.$hidden.val([this.textEditor1.getValue(),
                this.textEditor2.getValue(), this.textEditor3.getValue(),
                this.textEditor4.getValue()]);
        },

        render: function() {
            editors.Base.prototype.render.apply(this, arguments);
            var newDiv = this.$el.append(
                '<div class="col-sm-2 multitext"></div>').children().last();
            newDiv.append($(this.textEditor1.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Week</div>');
            editors.Base.prototype.render.apply(this, arguments);
            newDiv = this.$el.append('<div class="col-sm-2 multitext"></div>')
                .children().last();
            newDiv.append($(this.textEditor2.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Day</div>');
            editors.Base.prototype.render.apply(this, arguments);
            newDiv = this.$el.append('<div class="col-sm-2 multitext"></div>')
                .children().last();
            newDiv.append($(this.textEditor3.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Hour</div>');
            editors.Base.prototype.render.apply(this, arguments);
            newDiv = this.$el.append('<div class="col-sm-2 multitext"></div>')
                .children().last();
            newDiv.append($(this.textEditor4.render().el));
            newDiv = this.$el.append('<div class="col-sm-1"></div>').children()
                .last();
            newDiv.append('<div class="row help-block">Minute</div>');
            this.updateHidden();
            this.$el.append(this.$hidden);
            this.setValue(this.value);
            return this;
        },

        focus: function() {
            if (this.hasFocus)
                return;
            this.$el.focus();
        },
    });
})(Backbone.Form.editors);
