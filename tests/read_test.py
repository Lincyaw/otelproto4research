import random
import time
from pandas import json_normalize
import pytest
import opentelemetry.proto.metrics.v1.metrics_pb2 as metrics_pb2
import opentelemetry.proto.resource.v1.resource_pb2 as resource_pb2
import opentelemetry.proto.common.v1.common_pb2 as common_pb2
import src.otelproto4research.read as fn
from google.protobuf.json_format import MessageToJson
import pandas as pd


def gen_metric_data() -> metrics_pb2.MetricsData:
    md = metrics_pb2.MetricsData()
    for i in range(random.randint(1, 10)):
        md.resource_metrics.append(gen_resource_metric())
    return md


def gen_resource_metric() -> metrics_pb2.ResourceMetrics:
    rm = metrics_pb2.ResourceMetrics()
    rm.schema_url = "https://schema.org/"
    for i in range(random.randint(1, 10)):
        rm.resource.attributes.append(gen_kv())
    for i in range(random.randint(1, 10)):
        rm.scope_metrics.append(gen_scope_metric())
    return rm


def gen_scope_metric() -> metrics_pb2.ScopeMetrics:
    sm = metrics_pb2.ScopeMetrics()
    sm.schema_url = "https://schema.org/test"

    sm.scope.name = "mock"
    sm.scope.version = "0.1.0"
    for i in range(random.randint(1, 10)):
        sm.scope.attributes.append(gen_kv())
    for i in range(random.randint(1, 10)):
        sm.metrics.append(gen_metric())
    return sm


def gen_metric() -> metrics_pb2.Metric:
    m = metrics_pb2.Metric()
    m.name = "metric" + str(random.randint(1, 100))
    m.description = "description" + str(random.randint(1, 100))
    m.unit = "seconds"

    choice = random.randint(0, 10)
    if choice == 0:
        for i in range(random.randint(1, 10)):
            m.gauge.data_points.append(gen_data_point())
    elif choice == 1:
        for i in range(random.randint(1, 10)):
            m.exponential_histogram.data_points.append(gen_exponential_histogram_data_point())
    elif choice == 2:
        for i in range(random.randint(1, 10)):
            m.histogram.data_points.append(gen_histogram_data_point())
    elif choice == 3:
        for i in range(random.randint(1, 10)):
            m.summary.data_points.append(gen_summary_data_point())
    elif choice == 4:
        for i in range(random.randint(1, 10)):
            m.sum.data_points.append(gen_data_point())
    return m


def gen_data_point() -> metrics_pb2.NumberDataPoint:
    dp = metrics_pb2.NumberDataPoint()
    dp.start_time_unix_nano = time.time_ns()
    dp.time_unix_nano = time.time_ns()
    dp.as_int = random.randint(1, 100)
    for i in range(random.randint(1, 10)):
        dp.attributes.append(gen_kv())
    return dp


def gen_summary_data_point() -> metrics_pb2.SummaryDataPoint:
    sd = metrics_pb2.SummaryDataPoint()
    return sd


def gen_histogram_data_point() -> metrics_pb2.HistogramDataPoint:
    sd = metrics_pb2.HistogramDataPoint()
    return sd


def gen_exponential_histogram_data_point() -> metrics_pb2.ExponentialHistogramDataPoint:
    sd = metrics_pb2.ExponentialHistogramDataPoint()
    return sd


def gen_kv() -> common_pb2.KeyValue:
    kv = common_pb2.KeyValue()
    kv.key = "key" + str(random.randint(0, 100))
    seed = random.randint(0, 5)
    if seed == 0:
        kv.value.string_value = str(random.randint(1, 100))
    elif seed == 1:
        kv.value.bool_value = True
    elif seed == 2:
        kv.value.int_value = random.randint(1, 100)
    elif seed == 3:
        kv.value.double_value = random.random()
    elif seed == 4:
        kv.value.array_value.values.append(gen_any_value())
    elif seed == 5:
        kv.value.kvlist_value.values.append(gen_kv())
    elif seed == 6:
        kv.value.bytes_value = b'test'
    return kv


def gen_any_value() -> common_pb2.AnyValue:
    val = common_pb2.AnyValue()
    seed = random.randint(0, 7)
    if seed == 0:
        val.string_value = str(random.randint(1, 100))
    elif seed == 1:
        val.bool_value = True
    elif seed == 2:
        val.int_value = random.randint(1, 100)
    elif seed == 3:
        val.double_value = random.random()
    elif seed == 4:
        val.array_value.values.append(gen_any_value())
    elif seed == 5:
        val.kvlist_value.values.append(gen_kv())
    elif seed == 6:
        val.bytes_value = b'test'
    return val


def test_basic_serialize():
    # 测试read_function的代码
    med = gen_metric_data()
    df = fn.metrics_data2dataframe(med)
    print(df)
