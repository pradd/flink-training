/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first
 * calculate the total tips collected by each driver,
 * hour by hour, and then
 * from that stream find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

  private final SourceFunction<TaxiFare> source;
  private final SinkFunction<Tuple3<Long, Long, Float>> sink;

  /**
   * Creates a job using the source and sink provided.
   */
  public HourlyTipsExercise(
          SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

    this.source = source;
    this.sink = sink;
  }

  /**
   * Main method.
   *
   * @throws Exception which occurs during job execution.
   */
  public static void main(String[] args) throws Exception {

    HourlyTipsExercise job =
            new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

    job.execute();
  }

  /**
   * Create and execute the hourly tips pipeline.
   *
   * @return {JobExecutionResult}
   * @throws Exception which occurs during job execution.
   */
  public JobExecutionResult execute() throws Exception {

    // set up streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // start the data generator
    DataStream<TaxiFare> fares = env.addSource(source);

    fares
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                    .withTimestampAssigner((tf, timestamp) -> tf.getEventTimeMillis()))

            .keyBy(fare -> fare.driverId)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .aggregate(new AggregateFunction<TaxiFare, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>>() {
              @Override
              public Tuple3<Long, Long, Float> createAccumulator() {
                return new Tuple3<>(null, null, 0f);
              }

              @Override
              public Tuple3<Long, Long, Float> add(TaxiFare tf, Tuple3<Long, Long, Float> acc) {
                return new Tuple3<>(tf.getEventTimeMillis(), tf.driverId, tf.tip + acc.f2);
              }

              @Override
              public Tuple3<Long, Long, Float> getResult(Tuple3<Long, Long, Float> o) {
                return new Tuple3<>(
                        Instant.ofEpochMilli(o.f0)
                                .plus(1, ChronoUnit.HOURS)
                                .truncatedTo(ChronoUnit.HOURS)
                                .toEpochMilli(),
                        o.f1,
                        o.f2
                );
              }

              @Override
              public Tuple3<Long, Long, Float> merge(Tuple3<Long, Long, Float> acc1, Tuple3<Long, Long, Float> acc2) {
                return new Tuple3<>(
                        acc1.f0 != null ? acc1.f0 : acc2.f0,
                        acc1.f1 != null ? acc1.f1 : acc2.f1,
                        acc1.f2 + acc2.f2
                );
              }
            })

            .keyBy(t3 -> t3.f0)
            .map(new MapFunction<Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>>() {
              @Override
              public Tuple3<Long, Long, Float> map(Tuple3<Long, Long, Float> t) throws Exception {
                System.out.println(t);
                return t;
              }
            })

            .keyBy(t3 -> t3.f0)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .aggregate(new AggregateFunction<Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>>() {
              @Override
              public Tuple3<Long, Long, Float> createAccumulator() {
                return new Tuple3<>(null, null, 0f);
              }

              @Override
              public Tuple3<Long, Long, Float> add(Tuple3<Long, Long, Float> event, Tuple3<Long, Long, Float> acc) {
                if (acc.f0 == null) return event;
                return event.f2 > acc.f2 ? event : acc;
              }

              @Override
              public Tuple3<Long, Long, Float> getResult(Tuple3<Long, Long, Float> o) {
                return o;
              }

              @Override
              public Tuple3<Long, Long, Float> merge(Tuple3<Long, Long, Float> acc1, Tuple3<Long, Long, Float> acc2) {
                return add(acc1, acc2);
              }
            })

            .map(new MapFunction<Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>>() {
              @Override
              public Tuple3<Long, Long, Float> map(Tuple3<Long, Long, Float> t) throws Exception {
                System.out.println(t);
                return t;
              }
            })

            .addSink(sink);


    // the results should be sent to the sink that was passed in
    // (otherwise the tests won't work)
    // you can end the pipeline with something like this:

    // DataStream<Tuple3<Long, Long, Float>> hourlyMax = ...
    // hourlyMax.addSink(sink);

    // execute the pipeline and return the result
    return env.execute("Hourly Tips");
  }
}
