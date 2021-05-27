# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Test for the wordcount example."""

# pytype: skip-file

from __future__ import absolute_import

import collections
import logging
import os
import re
import tempfile
import unittest
import uuid

import pandas as pd
from nose.plugins.attrib import attr

from apache_beam.examples.dataframe import flight_delays
from apache_beam.io.filesystems import FileSystems
from apache_beam.testing.test_pipeline import TestPipeline


class FlightDelaysTest(unittest.TestCase):
  EXPECTED = {
      '2012-12-23': [
          ('AA', 20.082559339525282, 12.825593395252838),
          ('EV', 10.01901901901902, 4.431431431431432),
          ('HA', -1.0829015544041452, 0.010362694300518135),
          ('UA', 19.142555438225976, 11.07180570221753),
          ('MQ', 8.902255639097744, 3.6676691729323307),
          ('OO', 31.148883374689827, 31.90818858560794),
          ('US', 3.092541436464088, -2.350828729281768),
          ('WN', 12.074298711144806, 6.717968157695224),
          ('AS', 5.0456273764258555, 1.0722433460076046),
          ('B6', 20.646569646569645, 16.405405405405407),
          ('DL', 5.2559923298178335, -3.214765100671141),
          ('F9', 22.856060606060606, 24.318181818181817),
          ('FL', 4.492877492877493, -0.8005698005698005),
          ('VX', 61.59154929577465, 61.59154929577465),
          ('YV', 16.155844155844157, 13.376623376623376),
      ],
      '2012-12-24': [
          ('AA', 7.049086757990867, -1.5970319634703196),
          ('EV', 7.297101449275362, 2.2693236714975846),
          ('HA', -2.6789473684210527, -1.9842105263157894),
          ('UA', 10.935406698564593, -1.3337320574162679),
          ('MQ', 15.869642857142857, 9.992857142857142),
          ('OO', 11.048517520215633, 10.138814016172507),
          ('US', 1.369281045751634, -1.4101307189542485),
          ('WN', 7.515952597994531, 0.7028258887876025),
          ('AS', 0.5917602996254682, -2.2659176029962547),
          ('B6', 7.926470588235294, 2.6449579831932772),
          ('DL', 3.7171824973319105, -2.2358591248665953),
          ('F9', 14.01639344262295, 16.352459016393443),
          ('FL', 2.40785498489426, 2.6676737160120845),
          ('VX', 3.841666666666667, -2.4166666666666665),
          ('YV', 0.35384615384615387, 0.6205128205128205),
      ],
      '2012-12-25': [
          ('AA', 23.551581843191197, 35.62585969738652),
          ('EV', 16.96638655462185, 16.008403361344538),
          ('HA', -4.725806451612903, -3.9946236559139785),
          ('UA', 16.663145539906104, 10.772300469483568),
          ('MQ', 32.6, 44.28666666666667),
          ('OO', 16.2275960170697, 17.11948790896159),
          ('US', 2.7024539877300615, 0.22392638036809817),
          ('WN', 14.405783582089553, 10.111940298507463),
          ('AS', 3.792372881355932, 0.2033898305084746),
          ('B6', 8.951476793248945, 3.8860759493670884),
          ('DL', 2.3022170361726952, -3.6709451575262544),
          ('F9', 18.75886524822695, 21.5531914893617),
          ('FL', 1.3982300884955752, 0.9380530973451328),
          ('VX', 23.62878787878788, 23.636363636363637),
          ('YV', 11.2018779342723, 11.530516431924882),
      ],
  }

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.outdir = (
        self.test_pipeline.get_option('temp_location') + '/flight_delays_it-' +
        str(uuid.uuid4()))
    self.output_path = os.path.join(self.outdir, 'output.csv')

  def tearDown(self):
    FileSystems.delete([self.outdir + '/'])

  @attr('IT')
  def test_flight_delays(self):
    flight_delays.run_flight_delay_pipeline(self.test_pipeline,
                                            start_date='2012-12-23',
                                            end_date='2012-12-25',
                                            output=self.output_path)

    def read_csv(path):
      with FileSystems.open(path) as fp:
        return pd.read_csv(fp)

    # Parse result file and compare.
    for date, expectation in self.EXPECTED.items():
      result_df = pd.concat(
          read_csv(metadata.path) for metadata in FileSystems.match(
              [f'{self.output_path}-{date}*'])[0].metadata_list)
      result_df = result_df.sort_values('airline').reset_index(drop=True)

      expected_df = pd.DataFrame(expectation,
                                 columns=['airline', 'departure_delay',
                                          'arrival_delay'])
      expected_df = expected_df.sort_values('airline').reset_index(drop=True)

      try:
        pd.testing.assert_frame_equal(result_df, expected_df)
      except AssertionError as e:
        raise AssertionError(f"date={date!r} result DataFrame:\n\n"
                             f"{result_df}\n\n"
                             "Differs from Expectation:\n\n"
                             f"{expected_df}") from e


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
