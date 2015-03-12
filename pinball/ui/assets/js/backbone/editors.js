//Imported from https://github.com/powmedia/backbone-forms/issues/144
;(function(editors) {

    // like 'Select' editor, but will always return a boolean (true or false)
    editors.BooleanSelect = editors.Select.extend({
        initialize: function(options) {
            options.schema.options = [
                                      { val: '1', label: 'Yes' },
                                      { val: '', label: 'No' }
                                      ];
            editors.Select.prototype.initialize.call(this, options);
        },
        getValue: function() {
            return !!editors.Select.prototype.getValue.call(this);
        },
        setValue: function(value) {
            value = value ? '1' : '';
            editors.Select.prototype.setValue.call(this, value);
        }
    });

    // like the 'Select' editor, except will always return a number (int or float)
    editors.NumberSelect = editors.Select.extend({
        getValue: function() {
            return parseFloat(editors.Select.prototype.getValue.call(this));
        },
        setValue: function(value) {
            editors.Select.prototype.setValue.call(this, parseFloat(value));
        }
    });

    // 'Select' editor in bootstrap style
    editors.FormSelect = editors.Select.extend({
        initialize: function(options) {
            editors.Select.prototype.initialize.call(this, options);
            this.$el.addClass('form-control');
        },

        render: function() {
            editors.Select.prototype.render.apply(this, arguments);
            return this;
        }
    });

    // https://github.com/eternicode/bootstrap-datepicker/
    editors.DatePicker = editors.Text.extend({
        initialize: function(options) {
            editors.Text.prototype.initialize.call(this, options);
            this.$el.addClass('datepicker-input');
            this.$el.addClass('form-control');
        },

        getValue: function() {
            var value = this.$el.val();
            if (value) {
                return moment(value, 'MM/DD/YYYY').format('YYYY-MM-DD');
            } else {
                return '';
            }
        },

        setValue: function(value) {
            if (value) {
                var formatted = moment(value).utc().format('MM/DD/YYYY');
                this.$el.val(formatted);
            } else {
                this.$el.val('');
            }
        },

        render: function() {
            editors.Text.prototype.render.apply(this, arguments);
            this.$el.datepicker({
                autoclose: true
            });
            return this;
        }
    });

    // https://github.com/jonthornton/jquery-timepicker
    editors.TimePicker = editors.Text.extend({
        initialize: function(options) {
            editors.Text.prototype.initialize.call(this, options);
            this.$el.addClass('timepicker-input');
            this.$el.addClass('form-control');
        },

        render: function() {
            editors.Text.prototype.render.apply(this, arguments);
            this.$el.timepicker({
//              minTime: this.schema.minTime,
//              maxTime: this.schema.maxTime,
                'timeFormat': 'H.i.s.000',
                'step': 10,
                defaultTime: '12:00 AM',
                showMeridian: false,
            });
            return this;
        },

        getValue: function() {
            var value = this.$el.val();
            if (value) {
                return moment(value, 'hh:mm').format('HH.mm.00.000');
            } else {
                return '';
            }
        },

        setValue: function(value) {
            if (!value) value = '';
            this.value = value;
            var ret = editors.Text.prototype.setValue.apply(this, arguments);
            return ret;
        }
    });

    // Show both a date and time field
    // https://github.com/eternicode/bootstrap-datepicker/
    // https://github.com/jonthornton/jquery-timepicker
    editors.DateTimePicker = editors.Base.extend({
        events: {
            'changeDate': 'updateHidden',
            'changeTime': 'updateHidden',
            'input input': 'updateHidden' // so that clearing time works
        },
        initialize: function(options) {
            options = options || {};
            editors.Base.prototype.initialize.call(this, options);

            // Option defaults
            this.options = _.extend({
                DateEditor: editors.DatePicker,
                TimeEditor: editors.TimePicker
            }, options);

            // Schema defaults
            this.schema = _.extend({
                minsInterval: 15,
                minTime: '4:00am',
                maxTime: '11:00pm'
            }, options.schema || {});

            this.dateEditor = new this.options.DateEditor(options);
            this.dateEditor.$el.removeAttr('name');

            var timeOptions = _(options).clone();
            timeOptions.schema = _(this.schema).clone();
            timeOptions.schema.editorAttrs.placeholder = 'Any time';
            timeOptions.model = null;
            this.timeEditor = new this.options.TimeEditor(timeOptions);
            this.timeEditor.$el.removeAttr('name');

            this.$hidden = $('<input>', { type: 'hidden', name: options.key });

            this.value = this.dateEditor.value;
            this.setValue(this.value);
        },

        getValue: function() {
            return this.$hidden.val();
        },

        parseTimeValue: function(value) {
            return time;
        },

        setValue: function(value) {
            this.dateEditor.setValue(value);
            // pull the time portion of an ISO formatted string
            var time = '';
            if (_.isString(value) && value.indexOf('T') !== -1) {
                var m = moment(value);
                time = m ? m.format('h:mma') : '';
            }
            this.timeEditor.setValue(time);
        },

        updateHidden: function() {
            // update the hidden input with the value we want the server to see
            // if a date and time were chosen, include ISO formatted datetime with TZ offset
            // if no time was chosen, include only the date
            var date = moment(this.dateEditor.getValue());
            var time = this.timeEditor.getValue() ? this.timeEditor.$el.timepicker('getTime') : null;
            if (date && time) {
                date.hours(time.getHours());
                date.minutes(time.getMinutes());
            }
            var value = date ? date.format() : '';
            if (value && !time) {
                value = value.substr(0, value.indexOf('T'));
            }
            this.$hidden.val(value);
        },

        render: function() {
            editors.Base.prototype.render.apply(this, arguments);

            this.$el.append(this.dateEditor.render().el);
            this.$el.append(this.timeEditor.render().el);
            this.updateHidden();
            this.$el.append(this.$hidden);
            return this;
        }
    });

    editors.Range = editors.Text.extend({
        events: _.extend({}, editors.Text.prototype.events, {
            'change': function(event) {
                this.trigger('change', this);
            }
        }),

        initialize: function(options) {
            editors.Text.prototype.initialize.call(this, options);

            this.$el.attr('type', 'range');

            if (this.schema.appendToLabel) {
                this.updateLabel();
                this.on('change', this.updateLabel, this);
            }
        },

        getValue: function() {
            var val = editors.Text.prototype.getValue.call(this);
            return parseInt(val, 10);
        },

        updateLabel: function() {
            _(_(function() {
                var $label = this.$el.parents('.bbf-field').find('label');
                $label.text(this.schema.title + ': ' + this.getValue() + (this.schema.valueSuffix || ''));
            }).bind(this)).defer();
        }
    });

    editors.FormText = editors.Text.extend({
        initialize: function(options) {
            editors.Text.prototype.initialize.call(this, options);
            this.$el.addClass('form-control');
            this.$el.css('width', '100%');
        },

        render: function() {
            editors.Text.prototype.render.apply(this, arguments);
            return this;
        },
    });

    editors.FormTextWithHelp = editors.Text.extend({
        initialize: function(options) {
            editors.Text.prototype.initialize.call(this, options);
            this.$el.addClass('form-control');
        },

        render: function() {
            editors.Text.prototype.render.apply(this, arguments);
            return this;
        },
    });

    /**
     * NUMBER
     * 
     * Normal text input that only allows a number. Letters etc. are not entered.
     */
    editors.Number = editors.Text.extend({

        events: _.extend({}, editors.Text.prototype.events, {
            'keypress': 'onKeyPress',
            'change': 'onKeyPress'
        }),

        initialize: function(options) {
            this.defaultValue = options.schema.min;
            editors.Text.prototype.initialize.call(this, options);

            var schema = this.schema;

            this.$el.addClass('form-control');
            this.$el.attr('type', 'number');
            this.$el.attr('max', schema.max);
            this.$el.attr('min', schema.min);

            if (!schema || !schema.editorAttrs || !schema.editorAttrs.step) {
                // provide a default for `step` attr,
                // but don't overwrite if already specified
                this.$el.attr('step', 'any');
            }
        },

        /**
         * Check value is numeric
         * Force the value to be between min and max (inclusive).
         */
        onKeyPress: function(event) {
            function toRange(max, min, value) {
                if (!_.isNaN(max) && value > max) {
                    return false;
                } else if (!_.isNaN(min) && value < min) {
                    return false;
                } else {
                    return true;
                }
            }

            var self = this,
            delayedDetermineChange = function() {
                setTimeout(function() {
                    self.determineChange();
                }, 0);
            };

            //Allow backspace
            if (event.charCode === 0) {
                delayedDetermineChange();
                return;
            }

            //Get the whole new value so that we can prevent things like double decimals points etc.
            var newVal = this.$el.val();
            if( event.charCode != undefined ) {
                newVal = newVal + String.fromCharCode(event.charCode);
            }

            var numeric = /^[0-9]*\.?[0-9]*?$/.test(newVal);
            if (numeric) {
                numeric = toRange(parseInt(this.$el.attr('max')), parseInt(this.$el.attr('min')), newVal);
            }

            if (numeric) {
                delayedDetermineChange();
            }
            else {
                event.preventDefault();
            }
        },

        getValue: function() {
            var value = this.$el.val();

            return value === "" ? null : parseFloat(value, 10);
        },

        setValue: function(value) {
            value = (function() {
                if (_.isNumber(value)) return value;

                if (_.isString(value) && value !== '') return parseFloat(value, 10);

                return null;
            })();

            if (_.isNaN(value)) value = null;

            editors.Text.prototype.setValue.call(this, value);
        }

    });
})(Backbone.Form.editors);


