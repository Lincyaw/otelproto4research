from typing import List, Dict, Any
import opentelemetry.proto as op
import opentelemetry.proto.metrics.v1.metrics_pb2 as metrics_pb2
import opentelemetry.proto.trace.v1.trace_pb2 as trace_pb2
import opentelemetry.proto.logs.v1.logs_pb2 as logs_pb2
import opentelemetry.proto.common.v1.common_pb2 as common_pb2
import pandas as pd


def __parse_attributes(attributes: List[common_pb2.KeyValue]) -> Dict[
    str, Any]:
    return {attr.key: attr.value for attr in attributes}


def __parse_number_data_point(point: metrics_pb2.NumberDataPoint) -> Dict[
    str, Any]:
    data = {
        "start_time_unix_nano": point.start_time_unix_nano,
        "time_unix_nano": point.time_unix_nano,
        "value": point.as_double if point.HasField(
            "as_double") else point.as_int,
    }
    return data


def __parse_histogram_data_point(point: metrics_pb2.HistogramDataPoint) -> Dict[
    str, Any]:
    data = {
        "start_time_unix_nano": point.start_time_unix_nano,
        "time_unix_nano": point.time_unix_nano,
        "count": point.count,
        "sum": point.sum if point.HasField("sum") else None,
        "min": point.min if point.HasField("min") else None,
        "max": point.max if point.HasField("max") else None,
    }
    return data


def __parse_summary_data_point(point: metrics_pb2.SummaryDataPoint) -> Dict[
    str, Any]:
    data = {
        "start_time_unix_nano": point.start_time_unix_nano,
        "time_unix_nano": point.time_unix_nano,
        "count": point.count,
        "sum": point.sum,
        "quantile_values": {qv.quantile: qv.value for qv in
                            point.quantile_values},
    }
    return data


def metrics_resource2dataframe(metric: metrics_pb2.ResourceMetrics) -> pd.DataFrame:
    pass


def metrics_data2dataframe(metric: metrics_pb2.MetricsData) -> pd.DataFrame:
    rows = []
    for resource_metric in metric.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for m in scope_metric.metrics:
                common_data = {
                    "name": m.name,
                    "description": m.description,
                    "unit": m.unit,
                }
                if m.HasField("gauge"):
                    for point in m.gauge.data_points:
                        data = __parse_number_data_point(point)
                        data["type"] = "gauge"
                        row = {**common_data, **data}
                        rows.append(row)
                elif m.HasField("sum"):
                    for point in m.sum.data_points:
                        data = __parse_number_data_point(point)
                        data["type"] = "sum"
                        row = {**common_data, **data}
                        rows.append(row)
                elif m.HasField("histogram"):
                    for point in m.histogram.data_points:
                        data = __parse_histogram_data_point(point)
                        data["type"] = "histogram"
                        row = {**common_data, **data}
                        rows.append(row)
                elif m.HasField("exponential_histogram"):
                    pass
                    # for point in m.exponential_histogram.data_points:
                    #     data = __parse_histogram_data_point(point)
                    #     data["type"] = "exponential_histogram"
                    #     row = {**common_data, **data}
                    #     rows.append(row)
                elif m.HasField("summary"):
                    for point in m.summary.data_points:
                        data = __parse_summary_data_point(point)
                        data["type"] = "summary"
                        row = {**common_data, **data}
                        rows.append(row)
    return pd.DataFrame(rows)


def log_resource2dataframe(metric: logs_pb2.ResourceLogs) -> pd.DataFrame:
    pass


def log_data2dataframe(metric: logs_pb2.LogsData) -> pd.DataFrame:
    pass


def trace_resource2dataframe(metric: trace_pb2.ResourceSpans) -> pd.DataFrame:
    pass


def trace_data2dataframe(metric: trace_pb2.TracesData) -> pd.DataFrame:
    pass
