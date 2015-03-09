# Copyright 2015, Pinterest, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pinball_ext.job.hadoop_jobs import HadoopJob


class EmrWordCount(HadoopJob):
    def _get_class_name(self):
        return 'wordcount.WordCount'

    def _setup(self):
        self.extra_arguments = [
            # your input path
            's3://your_bucket/data/wordcount.input',
            # your output path
            's3://your_bucket/data/%(end_date)s/wordcount.output'
            % self.params
        ]
