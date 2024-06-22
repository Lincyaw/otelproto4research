import random
import time

import pytest
import opentelemetry.proto.metrics.v1.metrics_pb2 as metrics_pb2
import opentelemetry.proto.resource.v1.resource_pb2 as resource_pb2
import opentelemetry.proto.common.v1.common_pb2 as common_pb2
import src.otelproto4research.read as fn
import pandas as pd


def gen_metric_data() -> metrics_pb2.MetricsData:
    md = metrics_pb2.MetricsData()
    md.resource_metrics.append(gen_resource_metric())
    return md


def gen_resource_metric() -> metrics_pb2.ResourceMetrics:
    rm = metrics_pb2.ResourceMetrics()
    rm.schema_url = "https://schema.org/"
    rm.resource.attributes.append(gen_kv())
    rm.scope_metrics.append(gen_scope_metric())
    return rm


def gen_scope_metric() -> metrics_pb2.ScopeMetrics:
    sm = metrics_pb2.ScopeMetrics()
    sm.schema_url = "https://schema.org/test"

    sm.scope.name = "mock"
    sm.scope.version = "0.1.0"
    sm.scope.attributes.append(gen_kv())

    sm.metrics.append(gen_metric())
    return sm


def gen_metric() -> metrics_pb2.Metric:
    m = metrics_pb2.Metric()
    m.name = "metric" + str(random.randint(1, 100))
    m.description = "description" + str(random.randint(1, 100))
    m.unit = "seconds"
    m.gauge.data_points.append(gen_data_point())
    return m


def gen_data_point() -> metrics_pb2.NumberDataPoint:
    dp = metrics_pb2.NumberDataPoint()
    dp.start_time_unix_nano = time.time_ns()
    dp.time_unix_nano = time.time_ns()
    dp.as_int = random.randint(1, 100)
    dp.attributes.append(gen_kv())
    return dp


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
    print(med)
    df = fn.metrics_data2dataframe(med)
    print(df)


