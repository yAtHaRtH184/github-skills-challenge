from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully",
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout",
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service",
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service",
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_anomaly_detector_detects_all_anomaly_types():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:10:00",
        "service": "payment-service",
        "response_time_ms": 600,
        "cpu_percent": 90,
        "memory_percent": 90,
        "log_level": "WARNING",
        "message": "Multiple issues detected",
    }

    event = detector.detect(record)

    assert event is not None
    assert "High response time" in event["reasons"]
    assert "High CPU utilization" in event["reasons"]
    assert "High memory utilization" in event["reasons"]
    assert "Error log detected" in event["reasons"]


def test_pipeline_loads_and_processes_data(tmp_path):
    data_file = tmp_path / "service_data.json"

    data_file.write_text(
        """[
            {
                "timestamp": "2026-09-20T10:00:00",
                "service": "payment-service",
                "response_time_ms": 700,
                "cpu_percent": 90,
                "memory_percent": 90,
                "log_level": "WARNING",
                "message": "Failure"
            },
            {
                "timestamp": "2026-09-20T10:01:00",
                "service": "payment-service",
                "response_time_ms": 100,
                "cpu_percent": 30,
                "memory_percent": 40,
                "log_level": "INFO",
                "message": "OK"
            }
        ]""",
        encoding="utf-8",
    )

    result = run_pipeline(str(data_file))

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert len(result["events_consumed"]) == 0


def test_producer_rejects_empty_event():
    topic = EventTopic("test")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []


def test_event_topic_clear():
    topic = EventTopic("test")
    topic.publish({"event": 1})

    topic.clear()

    assert topic.get_messages() == []


def test_event_topic_name():
    topic = EventTopic("service-events")

    assert topic.name == "service-events"


def test_aiops_pipeline_main_block(tmp_path, monkeypatch, capsys):
    import runpy

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    data_file = data_dir / "service_data.json"

    data_file.write_text(
        """[
            {
                "timestamp": "2026-09-20T10:00:00",
                "service": "payment-service",
                "response_time_ms": 700,
                "cpu_percent": 90,
                "memory_percent": 90,
                "log_level": "WARNING",
                "message": "Failure"
            }
        ]""",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)

    runpy.run_module("src.aiops_pipeline", run_name="__main__")

    output = capsys.readouterr().out

    assert "AIOps Pipeline Result" in output
    assert "Records processed: 1" in output
    assert "Anomalies detected: 1" in output